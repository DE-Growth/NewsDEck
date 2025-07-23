from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from kafka import KafkaConsumer
from datetime import datetime as dt

# 기본 설정
default_args = {
    'owner': 'seoyunjang',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# DAG 정의
dag = DAG(
    'news_etl_pipeline',
    default_args=default_args,
    description='뉴스 데이터 ETL 파이프라인',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['news', 'etl', 'kafka']
)

def consume_kafka_messages():
    """Kafka에서 메시지 소비 및 PostgreSQL 저장"""
    
    # Kafka Consumer 설정
    consumer = KafkaConsumer(
        'news-topic',
        bootstrap_servers=['kafka:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='airflow-consumer-group',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        consumer_timeout_ms=30000
    )
    
    # PostgreSQL 연결
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    messages_processed = 0
    
    try:
        print("Kafka 메시지 소비 시작...")
        
        for message in consumer:
            try:
                news_data = message.value
                
                if not news_data.get('title'):
                    continue
                
                # PostgreSQL 삽입
                insert_sql = """
                INSERT INTO news_articles 
                (title, description, link, pub_date, keyword, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """
                
                collected_at = None
                if news_data.get('collected_at'):
                    try:
                        collected_at = dt.fromisoformat(news_data['collected_at'].replace('Z', '+00:00'))
                    except:
                        collected_at = dt.now()
                
                values = (
                    news_data.get('title', '')[:500],
                    news_data.get('description', ''),
                    news_data.get('link', ''),
                    news_data.get('pub_date', ''),
                    news_data.get('keyword', ''),
                    collected_at
                )
                
                postgres_hook.run(insert_sql, parameters=values)
                messages_processed += 1
                
                print(f"✅ 메시지 저장: {news_data.get('title', '')[:50]}...")
                
                if messages_processed >= 100:
                    break
                    
            except Exception as e:
                print(f"❌ 메시지 처리 오류: {e}")
                continue
                
    except Exception as e:
        print(f"❌ Kafka 소비 오류: {e}")
    
    finally:
        consumer.close()
        print(f"🎉 총 {messages_processed}개 메시지 처리 완료")
    
    return messages_processed

# Task 정의
consume_task = PythonOperator(
    task_id='consume_kafka_messages',
    python_callable=consume_kafka_messages,
    dag=dag
)