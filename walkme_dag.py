from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor

import json
import tempfile
import os
import logging

import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema
import unicodedata

logger = logging.getLogger(__name__)

class HttpPostSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, http_conn_id, endpoint, data=None, check_response=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.data = data or {}
        self.check_response = check_response or (lambda response: True)

    def poke(self, context):
        self.log.info(f"POST {self.endpoint} with data: {self.data}")
        http = HttpHook(method='POST', http_conn_id=self.http_conn_id)
        response = http.run(endpoint=self.endpoint, data=self.data)
        if not self.check_response(response):
            self.log.info("Response check failed, sensor will keep waiting...")
            return False
        self.log.info("Response check passed!")
        return True

walk_id_name_map = {
    '2': 'Vereda da Ponta de São Lourenço (PR8)',
    '3': 'Calheta - Levada das 25 Fontes e Risco',
    '4': 'Levada do Caldeirão Verde (PR9)',
    '11': 'Vereda dos Balcões (PR11)',
    '15': 'Vereda do Areeiro - Pico Ruivo (PR1)',
}

def walk_name_validated_characters(walk_name):
    nfkd_form = unicodedata.normalize('NFKD', walk_name)
    only_ascii = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    sanitized = "".join([c if c.isalnum() or c == ' ' else '_' for c in only_ascii])
    return sanitized.replace(' ', '_')

def upload_file_to_minio(data_to_minio, bucket_name, bucket_key):
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
        json.dump(data_to_minio, temp_file)
        temp_file_path = temp_file.name

    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    s3_client = s3_hook.get_conn()

    s3_client.upload_file(
        Filename=temp_file_path,
        Bucket=bucket_name,
        Key=bucket_key
    )

    os.remove(temp_file_path)

def get_latest_saved_comments_from_minio (s3_hook, bucket_name="rawdata", key=None):
    if key is None:
        raise ValueError("S3 key must be provided to fetch saved comments.")
    try:
        obj = s3_hook.get_key(bucket_name=bucket_name, key=key)
        content = obj.get()['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        logger.info(f"No commentary saved Nenhum comentário salvo anteriormente ou erro ao ler {key}. Erro: {e}")
        return []

def fetch_and_store_raw_data(walk_id: str):
    http_hook = HttpHook(
        method='POST',
        http_conn_id='walkme_conn'
    )

    all_comments = []
    page = 0

    while page < 3:
        payload = {
            'action': 'walk_get_comments',
            'walkId': walk_id,
            'page': str(page)
        }

        logger.info(f"[walkId={walk_id}] A buscar comentários da página {page}")
        response = http_hook.run(
            endpoint='wp-admin/admin-ajax.php',
            data=payload
        )

        try:
            data = response.json()

            if not data:
                break
        except Exception as e:
            raise AirflowException(f"[walkId={walk_id}] Erro ao decodificar JSON na página {page}: {str(e)}")

        if data.get("result") != "success":
            raise AirflowException(f"[walkId={walk_id}] Resposta inválida ou sem sucesso na página {page}")

        comments = data.get("comments", [])
        logger.info(f"[walkId={walk_id}] Página {page} retornou {len(comments)} comentários")
        all_comments.extend(comments)

        hasMore = data.get("hasMore", "no")
        logger.info(f"[walkId={walk_id}] hasMore: {hasMore}")
        if hasMore != "yes":
            break

        page += 1

    if not all_comments:
        logger.info(f"[walkId={walk_id}] Nenhum comentário novo disponível.")
        return

    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    walk_name = walk_id_name_map.get(walk_id, f"walk{walk_id}")
    cleared_walk_name = walk_name_validated_characters(walk_name)
    key_path = f"walkmeguide/{cleared_walk_name}.json"
    
    saved_comments = get_latest_saved_comments_from_minio(s3_hook, key=key_path)
    saved_ids = {c["id"] for c in saved_comments}
    new_ids = {c["id"] for c in all_comments}
    if not new_ids - saved_ids:
        logger.info(f"[walkId={walk_id}] Sem novos comentários, não será feito upload.")
        return

    upload_file_to_minio(
        data_to_minio=all_comments,
        bucket_name="rawdata",
        bucket_key=key_path
    )

    logger.info(f"[walkId={walk_id}] Novos comentários guardados em: {key_path}")

def clean_user_photos(data):
    for c in data:
        if "userPhoto" in c:
            del c["userPhoto"]

def clean_empty_comments(data):
    return [c for c in data if c.get("comment", "").strip()]

def clean_duplicate_comments(data):
    seen = set()
    unique_comments = []
    for c in data:
        comment_id = str(c.get("id", ""))
        comment_text = c.get("comment", "").strip()
        key = (comment_id, comment_text)
        if key not in seen:
            seen.add(key)
            unique_comments.append(c)
    return unique_comments

def transform_comments(data):
    clean_user_photos(data)
    data = clean_empty_comments(data)
    data = clean_duplicate_comments(data)
    return data

def get_raw_data_transform_load_temp_file(walk_id: str):
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    walk_name = walk_id_name_map.get(walk_id, f"walk{walk_id}")
    cleared_walk_name = walk_name_validated_characters(walk_name)
    bucket_key = f"walkmeguide/{cleared_walk_name}.json"
    s3_object = s3_hook.get_key(
        key=bucket_key,
        bucket_name='rawdata'
    )

    file_content = s3_object.get()['Body'].read().decode('utf-8')
    walkme_data = json.loads(file_content)
    logger.info(f"[walkId={walk_id}] Dados brutos obtidos do MinIO: {len(walkme_data)} comentários")

    walkme_data = transform_comments(walkme_data)
    logger.info(f"[walkId={walk_id}] Dados limpos: {len(walkme_data)} comentários restantes")

    output_data = {
        "walk_id": walk_id,
        "walk_name": walk_name,
        "comments": walkme_data
    }

    upload_file_to_minio(
        data_to_minio=output_data,
        bucket_name="tempfiles",
        bucket_key=f'walkmeguide/{cleared_walk_name}_cleaned.json'
    )

def validation_layer(walk_id: str):
    s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
    walk_name = walk_id_name_map.get(walk_id, f"walk{walk_id}")
    cleared_walk_name = walk_name_validated_characters(walk_name)
    bucket_key = f"walkmeguide/{cleared_walk_name}_cleaned.json"
    s3_object = s3_hook.get_key(
        key=bucket_key,
        bucket_name='tempfiles'
    )

    if s3_object is None:
        raise AirflowException(f"Ficheiro {bucket_key} não encontrado no bucket tempfiles")

    file_content = s3_object.get()['Body'].read().decode('utf-8')

    walkme_data = json.loads(file_content)

    if not walkme_data:
        raise AirflowException(f"Ficheiro {bucket_key} está vazio ou não contém dados válidos")

    comments = walkme_data.get("comments", [])

    if not comments:
        raise AirflowException(f"Ficheiro {bucket_key} não contém comentários")
    
    df = pd.DataFrame(walkme_data)

    if df.empty:
        raise AirflowException(f"DataFrame vazio após carregar {bucket_key}")
    
    walkme_schema = DataFrameSchema(
        {
            "walk_id": Column(pa.String, nullable=False),
            "walk_name": Column(pa.String, nullable=False),
            "comments": Column(pa.Object, nullable=False, coerce=True),
        },
    )

    try:
        validated_df = walkme_schema.validate(df)
    except pa.errors.SchemaErrors as e:
        raise AirflowException(f"Erro de validação no walk_id {walk_id}: {e.failure_cases}")

def upload_data_to_postgress(walk_id: str):
    try:
        s3_hook = S3Hook(aws_conn_id='minio_s3_conn')
        pg_hook = PostgresHook(postgres_conn_id='postgresql_conn')
        
        walk_name = walk_id_name_map.get(walk_id, f"walk{walk_id}")
        cleared_walk_name = walk_name_validated_characters(walk_name)
        bucket_key = f"walkmeguide/{cleared_walk_name}_cleaned.json"
        s3_object = s3_hook.get_key(
            key=bucket_key,
            bucket_name='tempfiles'
        )
                
        if s3_object is None:
            raise AirflowException(f"File '{cleared_walk_name}_cleaned.json' not found in 'tempfiles' bucket")

        file_content = s3_object.get()['Body'].read().decode('utf-8')
        walkme_data = json.loads(file_content)

        comments = walkme_data.get("comments", [])
        if not comments:
            raise AirflowException("No comments found to upload.")

        connection = pg_hook.get_conn()
        cursor = connection.cursor()

        existing_comment_ids = set()
        cursor.execute(
            "SELECT data->>'id' FROM data_lake WHERE domain = %s AND data_source_id = %s",
            ('WalkMe', 1)
        )
        existing_records = cursor.fetchall()
        for record in existing_records:
            if record[0]:
                existing_comment_ids.add(record[0])

        new_comments = []
        for comment in comments:
            comment_id = str(comment.get('id', ''))
            if comment_id and comment_id not in existing_comment_ids:
                new_comments.append(comment)

        if not new_comments:
            logger.info(f"Walk [walkId={walk_id}] - No new comments to insert into database.")
            cursor.close()
            connection.close()
            return

        for comment in new_comments:
            cursor.execute(
                "INSERT INTO data_lake (domain, data_source_id, data) VALUES (%s, %s, %s::jsonb)",
                ('WalkMe', 1, json.dumps(comment))
            )

        connection.commit()
        cursor.close()
        connection.close()
        
        logger.info(f"Walk [walkId={walk_id}] - Inserted {len(new_comments)} new comments into database.")
            
    except Exception as e:
        print(f"Error details: {str(e)}")
        raise AirflowException(f"Error happened inserting data: {str(e)}")

def create_dag():
    default_args = {
        'owner': 'filipe',
    }

    with DAG(
        dag_id='walkme_comments',
        default_args=default_args,
        description='Verifica e guarda novos comentários de caminhadas do Walkme',
        start_date=datetime(2025, 6, 16),
        schedule_interval='@daily',
        catchup=False,
    ) as dag:

        walk_ids = ['2', '3', '4', '11', '15'] 

        for walk_id in walk_ids:

            check_website_status = HttpPostSensor(
                task_id=f'check_website_status_{walk_id}',
                http_conn_id='walkme_conn',
                endpoint='wp-admin/admin-ajax.php',
                data={'action': 'walk_get_comments', 'walkId': walk_id, 'page': '0'},
                check_response=lambda response: "success" in response.text,
                retries=3,
                poke_interval=120,  
                timeout=600,
            )

            fetch_data_store_raw_data_task = PythonOperator(
                task_id=f'fetch_and_store_comments_{walk_id}',
                python_callable=fetch_and_store_raw_data,
                op_args=[walk_id],
                retries=2,
                retry_delay=timedelta(minutes=10),
            )

            get_raw_data_transform_load_temp_file_task = PythonOperator(
                task_id=f'get_raw_data_transform_load_temp_file_{walk_id}',
                python_callable=get_raw_data_transform_load_temp_file,
                op_args=[walk_id],
                retries=2,
                retry_delay=timedelta(minutes=10),
            )

            validation_layer_task = PythonOperator(
                task_id=f'validate_cleaned_comments_{walk_id}',
                python_callable=validation_layer,
                op_args=[walk_id],
                retries=2,
                retry_delay=timedelta(minutes=10),
            )

            upload_data_to_postgress_task = PythonOperator(
                task_id=f'upload_data_to_postgress_{walk_id}',
                python_callable=upload_data_to_postgress,
                op_args=[walk_id],
                retries=2,
                retry_delay=timedelta(minutes=10),
            )

            check_website_status >> fetch_data_store_raw_data_task >> get_raw_data_transform_load_temp_file_task >> validation_layer_task >> upload_data_to_postgress_task
    
    return dag

dag = create_dag()