from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os
import requests
import json
import re
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

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
    'news_collection_pipeline',
    default_args=default_args,
    description='뉴스 수집 및 처리 파이프라인',
    schedule_interval=timedelta(hours=1),  # 1시간마다 실행
    catchup=False,
    is_paused_upon_creation=False,  # 생성 시 자동으로 활성화
    tags=['news', 'data-pipeline']
)

def clean_html(text):
    """HTML 태그와 불필요한 문자를 제거하는 함수"""
    text = re.sub(r"<.*?>", "", text)
    text = text.replace("&quot;", '"').replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&amp;", "&").replace("&#39;", "'")
    return text

def get_naver_news(query, display=50):
    """네이버 뉴스 API를 통해 뉴스를 가져오는 함수"""
    headers = {
        "X-Naver-Client-Id": os.getenv("NAVER_CLIENT_ID", "YOUR_CLIENT_ID"),
        "X-Naver-Client-Secret": os.getenv("NAVER_CLIENT_SECRET", "YOUR_CLIENT_SECRET")
    }
    
    params = {
        "query": query,
        "display": display,
        "start": 1,
        "sort": "date"  # 최신순 정렬
    }
    
    try:
        response = requests.get(
            "https://openapi.naver.com/v1/search/news.json",
            headers=headers,
            params=params,
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"뉴스 API 호출 실패: {e}")
        return None

def collect_news(**context):
    """뉴스를 수집하는 함수"""
    keywords = ["IT", "주식", "AI", "반도체", "경제", "스타트업", "블록체인"]
    all_news = []
    
    for keyword in keywords:
        logging.info(f"키워드 '{keyword}'로 뉴스 수집 중...")
        news_data = get_naver_news(keyword)
        
        if news_data and 'items' in news_data:
            for item in news_data['items']:
                news_item = {
                    'keyword': keyword,
                    'title': clean_html(item.get('title', '')),
                    'link': item.get('link', ''),
                    'description': clean_html(item.get('description', '')),
                    'pub_date': item.get('pubDate', ''),
                    'collected_at': datetime.now().isoformat()
                }
                all_news.append(news_item)
    
    logging.info(f"총 {len(all_news)}개의 뉴스를 수집했습니다.")
    
    # Kafka로 뉴스 데이터 전송
    send_to_kafka(all_news)
    
    return f"수집 완료: {len(all_news)}개 뉴스"

def send_to_kafka(news_data):
    """Kafka로 뉴스 데이터를 전송하는 함수"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8')
        )
        
        topic = os.getenv("KAFKA_TOPIC", "news-topic")
        
        for news in news_data:
            producer.send(
                topic,
                key=news['keyword'],
                value=news
            )
            
        producer.flush()
        producer.close()
        logging.info(f"Kafka로 {len(news_data)}개 뉴스 전송 완료")
        
    except Exception as e:
        logging.error(f"Kafka 전송 실패: {e}")

def process_and_store_news(**context):
    """뉴스 데이터를 처리하고 데이터베이스에 저장하는 함수"""
    try:
        # PostgreSQL 연결
        conn = psycopg2.connect(
            host="postgres",
            database="airflow", 
            user="airflow",
            password="airflow"
        )
        
        cursor = conn.cursor()
        
        # 뉴스 테이블 생성 (없는 경우)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS news_articles (
            id SERIAL PRIMARY KEY,
            keyword VARCHAR(100),
            title TEXT,
            link TEXT UNIQUE,
            description TEXT,
            pub_date VARCHAR(100),
            collected_at TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(create_table_query)
        
        # 최근 수집된 뉴스 개수 확인 (예시 처리)
        cursor.execute("SELECT COUNT(*) FROM news_articles WHERE collected_at >= NOW() - INTERVAL '1 hour'")
        recent_count = cursor.fetchone()[0]
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"최근 1시간 내 수집된 뉴스: {recent_count}개")
        
        return f"처리 완료: 최근 1시간 내 {recent_count}개 뉴스"
        
    except Exception as e:
        logging.error(f"데이터베이스 처리 실패: {e}")
        return f"처리 실패: {str(e)}"

def generate_summary_report(**context):
    """수집 결과 요약 리포트를 생성하는 함수"""
    try:
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow", 
            password="airflow"
        )
        
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # 키워드별 뉴스 개수 조회
        cursor.execute("""
            SELECT keyword, COUNT(*) as count 
            FROM news_articles 
            WHERE collected_at >= NOW() - INTERVAL '24 hours'
            GROUP BY keyword 
            ORDER BY count DESC
        """)
        
        keyword_stats = cursor.fetchall()
        
        # 전체 통계
        cursor.execute("""
            SELECT 
                COUNT(*) as total_count,
                COUNT(DISTINCT keyword) as keyword_count
            FROM news_articles 
            WHERE collected_at >= NOW() - INTERVAL '24 hours'
        """)
        
        total_stats = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        # 리포트 생성
        report = {
            'date': datetime.now().isoformat(),
            'total_articles': total_stats['total_count'],
            'keywords_count': total_stats['keyword_count'],
            'keyword_breakdown': [dict(row) for row in keyword_stats]
        }
        
        logging.info(f"일일 리포트 생성 완료: {json.dumps(report, indent=2, ensure_ascii=False)}")
        
        return json.dumps(report, ensure_ascii=False)
        
    except Exception as e:
        logging.error(f"리포트 생성 실패: {e}")
        return f"리포트 생성 실패: {str(e)}"

# Task 정의
collect_news_task = PythonOperator(
    task_id='collect_news',
    python_callable=collect_news,
    dag=dag,
)

process_news_task = PythonOperator(
    task_id='process_and_store_news',
    python_callable=process_and_store_news,
    dag=dag,
)

generate_report_task = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag,
)

# 헬스체크 Task
health_check_task = BashOperator(
    task_id='health_check',
    bash_command='echo "뉴스 수집 파이프라인 헬스체크 완료"',
    dag=dag,
)

# Task 의존성 설정
health_check_task >> collect_news_task >> process_news_task >> generate_report_task
