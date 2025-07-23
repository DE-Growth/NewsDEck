import os
import json
import time
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

# 환경 변수 로드
load_dotenv()

class NewsProducer:
    def __init__(self):
        self.client_id = os.getenv('NAVER_CLIENT_ID')
        self.client_secret = os.getenv('NAVER_CLIENT_SECRET')
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9093')
        self.topic = os.getenv('KAFKA_TOPIC', 'news-topic')
        
        # Kafka Producer 초기화
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_servers],
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
        
    def search_news(self, query, display=10):
        """네이버 뉴스 검색"""
        url = "https://openapi.naver.com/v1/search/news.json"
        headers = {
            'X-Naver-Client-Id': self.client_id,
            'X-Naver-Client-Secret': self.client_secret
        }
        params = {
            'query': query,
            'display': display,
            'sort': 'date'
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"API 호출 오류: {e}")
            return None
    
    def send_to_kafka(self, data):
        """Kafka로 데이터 전송"""
        try:
            future = self.producer.send(self.topic, value=data)
            record_metadata = future.get(timeout=10)
            print(f"✅ 메시지 전송 성공: {record_metadata.topic}[{record_metadata.partition}] offset {record_metadata.offset}")
            return True
        except Exception as e:
            print(f"❌ Kafka 전송 실패: {e}")
            return False
    
    def run_once(self):
        """한 번 실행"""
        keywords = os.getenv('COLLECT_KEYWORDS', 'IT').split(',')
        
        for keyword in keywords:
            keyword = keyword.strip()
            print(f"🔍 '{keyword}' 뉴스 검색 중...")
            
            result = self.search_news(keyword, display=5)
            if result and result.get('items'):
                for item in result['items']:
                    news_data = {
                        'title': item.get('title', '').replace('<b>', '').replace('</b>', ''),
                        'description': item.get('description', '').replace('<b>', '').replace('</b>', ''),
                        'link': item.get('link', ''),
                        'pub_date': item.get('pubDate', ''),
                        'keyword': keyword,
                        'collected_at': datetime.now().isoformat()
                    }
                    
                    # Kafka로 전송
                    if self.send_to_kafka(news_data):
                        print(f"  📰 '{news_data['title'][:50]}...' 전송 완료")
                
                time.sleep(1)  # API 호출 제한 고려
            
            print(f"✅ '{keyword}' 검색 완료\n")

if __name__ == "__main__":
    producer = NewsProducer()
    
    print("=== 뉴스 수집기 시작 ===")
    print("Ctrl+C로 종료")
    
    try:
        while True:
            producer.run_once()
            interval = int(os.getenv('COLLECT_INTERVAL', 300))
            print(f"⏰ {interval}초 대기 중...")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n🛑 수집기 종료")
    finally:
        producer.producer.close()