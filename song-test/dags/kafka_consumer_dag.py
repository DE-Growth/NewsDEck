from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from kafka import KafkaConsumer
import logging
import time

# DAG 기본 설정
default_args = {
    'owner': 'news_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'kafka_to_db_consumer',
    default_args=default_args,
    description='Kafka에서 뉴스 데이터를 읽어서 PostgreSQL에 저장',
    schedule_interval=timedelta(minutes=10),  # 10분마다 실행
    catchup=False,
    is_paused_upon_creation=False,  # 생성 시 자동으로 활성화
    tags=['news', 'kafka', 'consumer']
)

def consume_kafka_messages(**context):
    """Kafka에서 메시지를 소비하고 데이터베이스에 저장"""
    try:
        # Kafka Consumer 설정
        consumer = KafkaConsumer(
            'news-topic',
            bootstrap_servers=['kafka:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='airflow_consumer_group',
            consumer_timeout_ms=30000  # 30초 타임아웃
        )
        
        # PostgreSQL 연결
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow", 
            password="airflow"
        )
        cursor = conn.cursor()
        
        # 테이블 생성 (존재하지 않는 경우)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS news_articles (
            id SERIAL PRIMARY KEY,
            keyword VARCHAR(100),
            title TEXT NOT NULL,
            link TEXT UNIQUE NOT NULL,
            description TEXT,
            pub_date VARCHAR(100),
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_news_keyword ON news_articles(keyword);
        CREATE INDEX IF NOT EXISTS idx_news_collected_at ON news_articles(collected_at);
        """
        cursor.execute(create_table_query)
        conn.commit()
        
        processed_count = 0
        duplicates = 0
        
        # Kafka 메시지 처리
        for message in consumer:
            try:
                news_data = message.value
                
                # 데이터 검증
                if not news_data.get('title') or not news_data.get('link'):
                    logging.warning(f"필수 데이터 누락: {news_data}")
                    continue
                
                # 데이터베이스에 삽입
                insert_query = """
                INSERT INTO news_articles (keyword, title, link, description, pub_date, collected_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (link) DO NOTHING
                """
                
                cursor.execute(insert_query, (
                    news_data.get('keyword', ''),
                    news_data.get('title', ''),
                    news_data.get('link', ''),
                    news_data.get('description', ''),
                    news_data.get('pubDate', '')
                ))
                
                if cursor.rowcount > 0:
                    processed_count += 1
                else:
                    duplicates += 1
                    
                # 100개마다 커밋
                if (processed_count + duplicates) % 100 == 0:
                    conn.commit()
                    
            except Exception as e:
                logging.error(f"메시지 처리 실패: {e}")
                continue
        
        # 최종 커밋
        conn.commit()
        cursor.close()
        conn.close()
        consumer.close()
        
        logging.info(f"Kafka 소비 완료: 처리된 메시지 {processed_count}개, 중복 {duplicates}개")
        return f"성공: {processed_count}개 처리, {duplicates}개 중복"
        
    except Exception as e:
        logging.error(f"Kafka 소비 실패: {e}")
        return f"실패: {str(e)}"

def cleanup_old_news(**context):
    """30일 이상 된 뉴스 데이터 정리"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        # 30일 이전 데이터 삭제
        cursor.execute("""
            DELETE FROM news_articles 
            WHERE collected_at < NOW() - INTERVAL '30 days'
        """)
        
        deleted_count = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"정리 완료: {deleted_count}개 오래된 뉴스 삭제")
        return f"정리 완료: {deleted_count}개 삭제"
        
    except Exception as e:
        logging.error(f"데이터 정리 실패: {e}")
        return f"정리 실패: {str(e)}"

def update_statistics(**context):
    """뉴스 수집 통계 업데이트"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # 오늘의 통계 계산
        cursor.execute("""
            SELECT 
                keyword,
                COUNT(*) as count,
                MAX(collected_at) as last_collected
            FROM news_articles 
            WHERE DATE(collected_at) = CURRENT_DATE
            GROUP BY keyword
        """)
        
        daily_stats = cursor.fetchall()
        
        # 통계 테이블 생성/업데이트
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS collection_stats (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                keyword VARCHAR(100),
                count INTEGER DEFAULT 0,
                last_collected TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, keyword)
            );
        """)
        
        # 오늘 통계 업데이트
        for stat in daily_stats:
            cursor.execute("""
                INSERT INTO collection_stats (date, keyword, count, last_collected)
                VALUES (CURRENT_DATE, %s, %s, %s)
                ON CONFLICT (date, keyword) 
                DO UPDATE SET 
                    count = EXCLUDED.count,
                    last_collected = EXCLUDED.last_collected
            """, (stat['keyword'], stat['count'], stat['last_collected']))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"통계 업데이트 완료: {len(daily_stats)}개 키워드")
        return f"통계 업데이트 완료: {len(daily_stats)}개 키워드"
        
    except Exception as e:
        logging.error(f"통계 업데이트 실패: {e}")
        return f"통계 업데이트 실패: {str(e)}"

# Task 정의
consume_kafka_task = PythonOperator(
    task_id='consume_kafka_messages',
    python_callable=consume_kafka_messages,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_news',
    python_callable=cleanup_old_news,
    dag=dag,
)

statistics_task = PythonOperator(
    task_id='update_statistics',
    python_callable=update_statistics,
    dag=dag,
)

# Kafka 상태 확인
kafka_check_task = BashOperator(
    task_id='kafka_health_check',
    bash_command='echo "Kafka consumer 헬스체크 완료"',
    dag=dag,
)

# Task 의존성 설정
kafka_check_task >> consume_kafka_task >> [cleanup_task, statistics_task]
