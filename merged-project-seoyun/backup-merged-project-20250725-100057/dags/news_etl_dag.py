from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
import time
import psycopg2
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime as dt
import pytz

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

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
    'integrated_news_pipeline',
    default_args=default_args,
    description='통합 뉴스 수집 및 저장 파이프라인',
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    tags=['news', 'integrated', 'kafka', 'producer', 'consumer']
)

def collect_and_store_news():
    """뉴스 수집 → Kafka 전송 → PostgreSQL 저장을 한 번에 처리"""
    
    print("🚀 통합 뉴스 파이프라인 시작!")
    
    # ========================
    # 1단계: 뉴스 수집 (Producer 역할)
    # ========================
    print("📰 1단계: 뉴스 수집 시작...")
    
    # 네이버 API 설정 (환경변수 대신 설정값 사용)
    client_id = "nHALZLzPSxgWWrVlQfp6"  # 실제 API 키
    client_secret = "T73FiHjlpT"  # 실제 시크릿
    
    # Kafka Producer 설정
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    
    keywords = ['IT', 'AI', '블록체인']  # 수집할 키워드들
    collected_news = []
    
    for keyword in keywords:
        print(f"🔍 '{keyword}' 뉴스 검색 중...")
        
        # 네이버 뉴스 API 호출
        url = "https://openapi.naver.com/v1/search/news.json"
        headers = {
            'X-Naver-Client-Id': client_id,
            'X-Naver-Client-Secret': client_secret
        }
        params = {
            'query': keyword,
            'display': 5,
            'sort': 'date'
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            result = response.json()
            
            if result and result.get('items'):
                for item in result['items']:
                    # 한국 시간으로 수집 시간 설정
                    kst_now = datetime.now(KST)
                    
                    news_data = {
                        'title': item.get('title', '').replace('<b>', '').replace('</b>', ''),
                        'description': item.get('description', '').replace('<b>', '').replace('</b>', ''),
                        'link': f"{item.get('link', '')}?collected_at={int(kst_now.timestamp())}",  # 한국 시간 타임스탬프
                        'pub_date': item.get('pubDate', ''),
                        'keyword': keyword,
                        'collected_at': kst_now.isoformat()  # 한국 시간으로 설정
                    }
                    
                    # Kafka로 전송
                    try:
                        future = producer.send('news-topic', value=news_data)
                        record_metadata = future.get(timeout=10)
                        collected_news.append(news_data)
                        print(f"  ✅ Kafka 전송: {news_data['title'][:50]}...")
                    except Exception as e:
                        print(f"  ❌ Kafka 전송 실패: {e}")
                
                time.sleep(1)  # API 호출 제한 고려
        
        except Exception as e:
            print(f"❌ '{keyword}' API 호출 오류: {e}")
    
    producer.close()
    print(f"📤 총 {len(collected_news)}개 뉴스를 Kafka로 전송 완료!")
    
    # ========================
    # 2단계: 잠시 대기 (Kafka 메시지 안정화)
    # ========================
    print("⏳ 2단계: Kafka 메시지 안정화 대기...")
    time.sleep(5)
    
    # ========================
    # 3단계: Kafka에서 데이터 읽어서 PostgreSQL 저장 (Consumer 역할)
    # ========================
    print("💾 3단계: PostgreSQL 저장 시작...")
    
    # Kafka Consumer 설정
    consumer = KafkaConsumer(
        'news-topic',
        bootstrap_servers=['kafka:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id=f'integrated-consumer-{int(datetime.now(KST).timestamp())}',  # 한국 시간 기반 유니크 그룹 ID
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=15000  # 15초 타임아웃
    )
    
    messages_processed = 0
    try:
        for message in consumer:
            try:
                news_data = message.value
                if not news_data.get('title'):
                    continue
                
                # PostgreSQL 직접 연결
                conn = psycopg2.connect(
                    host='postgres',
                    port=5432,
                    database='airflow', 
                    user='airflow',
                    password='airflow'
                )
                cur = conn.cursor()
                
                # PostgreSQL 삽입 (한국 시간으로 저장)
                insert_sql = """
                INSERT INTO news_articles
                (title, description, link, pub_date, keyword, collected_at)
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                
                # 한국 시간으로 collected_at 설정
                collected_at = None
                if news_data.get('collected_at'):
                    try:
                        # ISO 형식의 한국 시간을 파싱
                        collected_at = dt.fromisoformat(news_data['collected_at'].replace('Z', '+00:00'))
                        # 만약 UTC 시간이라면 한국 시간으로 변환
                        if collected_at.tzinfo is None:
                            collected_at = KST.localize(collected_at)
                        else:
                            collected_at = collected_at.astimezone(KST)
                    except:
                        collected_at = dt.now(KST)  # 파싱 실패 시 현재 한국 시간 사용
                else:
                    collected_at = dt.now(KST)  # collected_at이 없으면 현재 한국 시간 사용
                
                values = (
                    news_data.get('title', '')[:500],
                    news_data.get('description', ''),
                    news_data.get('link', ''),
                    news_data.get('pub_date', ''),
                    news_data.get('keyword', ''),
                    collected_at
                )
                
                cur.execute(insert_sql, values)
                conn.commit()
                cur.close()
                conn.close()
                
                messages_processed += 1
                print(f"  ✅ DB 저장: {news_data.get('title', '')[:50]}...")
                
                # 충분히 처리했으면 종료
                if messages_processed >= 50:
                    break
                    
            except Exception as e:
                print(f"❌ 메시지 처리 오류: {e}")
                continue
    
    except Exception as e:
        print(f"❌ Kafka 소비 오류: {e}")
    
    finally:
        consumer.close()
    
    print(f"🎉 총 {messages_processed}개 뉴스를 PostgreSQL에 저장 완료!")
    
    # ========================
    # 4단계: 결과 요약 (한국 시간으로)
    # ========================
    result_summary = {
        'collected_count': len(collected_news),
        'stored_count': messages_processed,
        'keywords': keywords,
        'execution_time': datetime.now(KST).isoformat()  # 한국 시간으로 실행 시간 기록
    }
    
    print("📊 실행 결과:")
    print(f"  - 수집된 뉴스: {result_summary['collected_count']}개")
    print(f"  - 저장된 뉴스: {result_summary['stored_count']}개")
    print(f"  - 처리된 키워드: {', '.join(keywords)}")
    print(f"  - 실행 시간: {result_summary['execution_time']}")
    
    return result_summary

# Task 정의
integrated_task = PythonOperator(
    task_id='collect_and_store_news',
    python_callable=collect_and_store_news,
    dag=dag
)