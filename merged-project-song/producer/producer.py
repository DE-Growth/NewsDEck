import os
import json
import time
import requests
import re
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from datetime import datetime
import logging

# 환경 변수 로드
load_dotenv()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IntegratedNewsProducer:
    """
    통합 뉴스 수집기
    - seoyunjang의 클래스 기반 구조 채택
    - song-test의 HTML 정리 기능과 에러 처리 강화
    """
    
    def __init__(self):
        # 네이버 API 설정
        self.client_id = os.getenv('NAVER_CLIENT_ID')
        self.client_secret = os.getenv('NAVER_CLIENT_SECRET')
        
        # Kafka 설정 (로컬/도커 환경 자동 감지)
        self.kafka_servers = self._get_kafka_servers()
        self.topic = os.getenv('KAFKA_TOPIC', 'news-topic')
        
        # 수집 설정
        self.keywords = self._get_keywords()
        self.collect_interval = int(os.getenv('COLLECT_INTERVAL', 600))  # 10분 (600초)
        self.articles_per_keyword = int(os.getenv('ARTICLES_PER_KEYWORD', 10))
        
        # API URL
        self.api_url = "https://openapi.naver.com/v1/search/news.json"
        
        # Kafka Producer 초기화
        self.producer = self._create_kafka_producer()
        
        logger.info(f"통합 뉴스 수집기 초기화 완료")
        logger.info(f"Kafka: {self.kafka_servers}, Topic: {self.topic}")
        logger.info(f"키워드: {self.keywords}")
        
    def _get_kafka_servers(self):
        """Kafka 서버 주소 결정 (환경 자동 감지)"""
        kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        
        if kafka_servers:
            return kafka_servers
            
        # 자동 감지: Docker 환경 vs 로컬 환경
        if os.path.exists('/.dockerenv'):
            # Docker 환경
            return 'kafka:9092'
        else:
            # 로컬 환경
            return 'localhost:9093'
    
    def _get_keywords(self):
        """검색 키워드 목록 가져오기"""
        keywords_str = os.getenv('COLLECT_KEYWORDS', 'IT,주식,AI,반도체,경제')
        return [keyword.strip() for keyword in keywords_str.split(',')]
    
    def _create_kafka_producer(self):
        """Kafka Producer 생성 (연결 재시도 포함)"""
        max_retries = 5
        retry_delay = 10
        
        for attempt in range(max_retries):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_servers],
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                    acks='all',
                    retries=3,
                    batch_size=16384,
                    linger_ms=100
                )
                
                logger.info(f"Kafka Producer 연결 성공: {self.kafka_servers}")
                return producer
                
            except NoBrokersAvailable:
                logger.warning(f"Kafka 연결 실패 (시도 {attempt + 1}/{max_retries}). {retry_delay}초 후 재시도...")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.error("Kafka 연결 최종 실패")
                    raise
    
    def clean_html(self, text):
        """HTML 태그와 불필요한 문자 제거 (song-test 기반)"""
        if not text:
            return ""
            
        # HTML 태그 제거
        text = re.sub(r"<.*?>", "", text)
        
        # HTML 엔티티 변환
        text = text.replace("&quot;", '"')
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&amp;", "&")
        text = text.replace("&#39;", "'")
        text = text.replace("&nbsp;", " ")
        
        # 연속된 공백 정리
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    
    def search_news(self, query, display=None):
        """네이버 뉴스 검색 (강화된 에러 처리)"""
        if not self.client_id or not self.client_secret:
            logger.error("네이버 API 키가 설정되지 않았습니다.")
            return None
        
        display = display or self.articles_per_keyword
        
        headers = {
            'X-Naver-Client-Id': self.client_id,
            'X-Naver-Client-Secret': self.client_secret,
            'User-Agent': 'IntegratedNewsProducer/1.0'
        }
        
        params = {
            'query': query,
            'display': display,
            'sort': 'date',
            'start': 1
        }
        
        try:
            logger.info(f"'{query}' 뉴스 검색 중... (최대 {display}건)")
            
            response = requests.get(
                self.api_url, 
                headers=headers, 
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            result = response.json()
            items = result.get('items', [])
            
            logger.info(f"'{query}' 검색 완료: {len(items)}건 수집")
            return items
            
        except requests.exceptions.RequestException as e:
            logger.error(f"'{query}' API 호출 실패: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"'{query}' JSON 파싱 실패: {e}")
            return None
    
    def send_to_kafka(self, data):
        """Kafka로 데이터 전송 (강화된 에러 처리)"""
        try:
            future = self.producer.send(self.topic, value=data)
            record_metadata = future.get(timeout=10)
            
            logger.debug(f"Kafka 전송 성공: {record_metadata.topic}[{record_metadata.partition}] offset {record_metadata.offset}")
            return True
            
        except Exception as e:
            logger.error(f"Kafka 전송 실패: {e}")
            return False
    
    def process_article(self, item, keyword):
        """뉴스 기사 데이터 처리 및 정제"""
        try:
            # 데이터 정제
            title = self.clean_html(item.get('title', ''))
            description = self.clean_html(item.get('description', ''))
            
            # 필수 데이터 검증
            if not title or not item.get('link'):
                logger.warning(f"필수 데이터 누락: title={bool(title)}, link={bool(item.get('link'))}")
                return None
            
            # 통합 뉴스 데이터 구조
            news_data = {
                'keyword': keyword,
                'title': title[:500],  # 길이 제한
                'link': item.get('link', ''),
                'originallink': item.get('originallink', ''),
                'description': description[:1000],  # 길이 제한
                'pubDate': item.get('pubDate', ''),
                'pub_date': item.get('pubDate', ''),  # 양쪽 형식 지원
                'collected_at': datetime.now().isoformat(),
                'source': 'naver_news_api',
                'producer_version': '1.0'
            }
            
            return news_data
            
        except Exception as e:
            logger.error(f"기사 처리 실패: {e}")
            return None
    
    def collect_news_by_keyword(self, keyword):
        """특정 키워드로 뉴스 수집"""
        try:
            # API 호출
            articles = self.search_news(keyword)
            if not articles:
                return 0
            
            success_count = 0
            
            for article in articles:
                # 기사 데이터 처리
                news_data = self.process_article(article, keyword)
                if not news_data:
                    continue
                
                # Kafka로 전송
                if self.send_to_kafka(news_data):
                    success_count += 1
                    logger.info(f"[{keyword}] '{news_data['title'][:50]}...' 전송 완료")
                else:
                    logger.error(f"[{keyword}] '{news_data['title'][:50]}...' 전송 실패")
            
            # Kafka flush (즉시 전송 보장)
            self.producer.flush()
            
            logger.info(f"[{keyword}] 수집 완료: {success_count}/{len(articles)}건 성공")
            return success_count
            
        except Exception as e:
            logger.error(f"[{keyword}] 수집 실패: {e}")
            return 0
    
    def run_once(self):
        """한 번의 수집 사이클 실행"""
        logger.info("=== 뉴스 수집 사이클 시작 ===")
        start_time = time.time()
        
        total_collected = 0
        
        for keyword in self.keywords:
            try:
                collected = self.collect_news_by_keyword(keyword)
                total_collected += collected
                
                # API 호출 제한 고려 (키워드 간 1초 대기)
                if keyword != self.keywords[-1]:  # 마지막 키워드가 아니면
                    time.sleep(1)
                    
            except Exception as e:
                logger.error(f"키워드 '{keyword}' 처리 중 오류: {e}")
                continue
        
        elapsed_time = time.time() - start_time
        logger.info(f"=== 수집 사이클 완료: 총 {total_collected}건 수집 (소요시간: {elapsed_time:.1f}초) ===")
        
        return total_collected
    
    def run_continuous(self):
        """지속적인 뉴스 수집 실행"""
        logger.info("=== 통합 뉴스 수집기 시작 ===")
        logger.info(f"수집 주기: {self.collect_interval}초")
        logger.info("Ctrl+C로 종료")
        
        cycle_count = 0
        
        try:
            while True:
                cycle_count += 1
                logger.info(f"\n--- 수집 사이클 #{cycle_count} ---")
                
                collected = self.run_once()
                
                if collected > 0:
                    logger.info(f"✅ 사이클 #{cycle_count} 성공: {collected}건 수집")
                else:
                    logger.warning(f"⚠️ 사이클 #{cycle_count} 실패: 수집된 뉴스 없음")
                
                logger.info(f"⏰ 다음 수집까지 {self.collect_interval}초 대기...")
                time.sleep(self.collect_interval)
                
        except KeyboardInterrupt:
            logger.info("\n🛑 사용자 요청으로 수집기 종료")
        except Exception as e:
            logger.error(f"💥 예상치 못한 오류로 수집기 종료: {e}")
        finally:
            self.close()
    
    def close(self):
        """리소스 정리"""
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka Producer 종료 완료")
        except Exception as e:
            logger.error(f"리소스 정리 중 오류: {e}")

def main():
    """메인 실행 함수"""
    try:
        producer = IntegratedNewsProducer()
        
        # 환경 변수에 따라 실행 모드 결정
        run_mode = os.getenv('RUN_MODE', 'continuous')
        
        if run_mode == 'once':
            # 한 번만 실행
            producer.run_once()
        else:
            # 지속적 실행
            producer.run_continuous()
            
    except Exception as e:
        logger.error(f"프로그램 시작 실패: {e}")
        exit(1)

if __name__ == "__main__":
    main()
