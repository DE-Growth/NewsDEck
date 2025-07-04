import os
import requests
import json
import time
import re
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

# 로컬에서 .env 파일의 환경 변수를 불러옵니다.
load_dotenv()

# --- 설정 ---
# 네이버 API 클라이언트 정보 (환경 변수에서 가져오기)
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")
NAVER_API_URL = "https://openapi.naver.com/v1/search/news.json"

# Kafka 서버 주소 설정
# 개발 환경에 따라 아래 두 줄 중 하나를 선택하여 사용하세요.
# -------------------------------------------------------------------

# [로컬 개발용] 로컬 PC에서 직접 producer.py를 실행할 때 사용합니다.
# Docker 외부에서 Kafka 컨테이너(localhost:9093)로 접속합니다.
KAFKA_BROKER = "localhost:9093"

# [도커 배포용] Docker Compose로 producer 컨테이너를 실행할 때 사용합니다.
# Docker 내부 네트워크를 통해 다른 컨테이너(kafka:9092)로 접속합니다.
# KAFKA_BROKER = 'kafka:9092'

# -------------------------------------------------------------------

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news-topic")

# 검색할 키워드 리스트
SEARCH_KEYWORDS = ["IT", "주식", "AI", "반도체", "경제"]

# 뉴스 수집 주기 (초 단위, 테스트 시에는 짧게 설정 권장)
FETCH_INTERVAL = 3600  # 1시간


def clean_html(text):
    """HTML 태그와 불필요한 문자를 제거하는 함수"""
    text = re.sub(r"<.*?>", "", text)
    text = text.replace("&quot;", '"').replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&amp;", "&").replace("&#39;", "'")
    return text


def get_naver_news(query, display=100, start=1):
    """네이버 뉴스 API를 호출하여 뉴스 데이터를 가져오는 함수"""
    if not NAVER_CLIENT_ID or not NAVER_CLIENT_SECRET:
        print(
            "오류: NAVER_CLIENT_ID 또는 NAVER_CLIENT_SECRET 환경 변수가 설정되지 않았습니다."
        )
        return []

    headers = {
        "X-Naver-Client-Id": NAVER_CLIENT_ID,
        "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
    }
    params = {"query": query, "display": display, "start": start, "sort": "date"}
    try:
        response = requests.get(NAVER_API_URL, headers=headers, params=params)
        response.raise_for_status()
        news_data = response.json()
        print(
            f"'{query}' 키워드로 뉴스 {len(news_data.get('items', []))}건을 성공적으로 가져왔습니다."
        )
        return news_data.get("items", [])
    except requests.exceptions.RequestException as e:
        print(f"API 호출 중 오류 발생: {e}")
        return []


def create_kafka_producer():
    """Kafka Producer 인스턴스를 생성하고 연결을 시도하는 함수"""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                    "utf-8"
                ),
                acks="all",
            )
            print(f"Kafka Producer가 {KAFKA_BROKER}에 성공적으로 연결되었습니다.")
            return producer
        except NoBrokersAvailable:
            print(
                f"Kafka 브로커({KAFKA_BROKER})를 찾을 수 없습니다. 10초 후 재시도합니다."
            )
            time.sleep(10)


def main():
    """메인 실행 함수: 주기적으로 뉴스를 수집하여 Kafka에 전송"""
    producer = create_kafka_producer()

    while True:
        print(f"\n--- {time.ctime()} 뉴스 수집 시작 ---")
        for keyword in SEARCH_KEYWORDS:
            news_items = get_naver_news(keyword)
            if not news_items:
                continue
            for item in news_items:
                cleaned_item = {
                    "keyword": keyword,
                    "title": clean_html(item.get("title", "")),
                    "originallink": item.get("originallink", ""),
                    "link": item.get("link", ""),
                    "description": clean_html(item.get("description", "")),
                    "pubDate": item.get("pubDate", ""),
                }
                producer.send(KAFKA_TOPIC, value=cleaned_item)
                print(
                    f"  -> Kafka 전송 완료: [토픽: {KAFKA_TOPIC}] {cleaned_item['title'][:50]}..."
                )
            producer.flush()
            time.sleep(1)

        print(f"--- 수집 완료. 다음 수집까지 {FETCH_INTERVAL}초 대기합니다. ---")
        time.sleep(FETCH_INTERVAL)


if __name__ == "__main__":
    main()
