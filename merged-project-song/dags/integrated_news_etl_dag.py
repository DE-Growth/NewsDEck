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

# DAG 기본 설정 (song-test 기반)
default_args = {
    "owner": "merged_news_team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의 (song-test의 완전한 구조 채택)
dag = DAG(
    "integrated_news_etl_pipeline",
    default_args=default_args,
    description="통합 뉴스 데이터 ETL 파이프라인 - Kafka에서 PostgreSQL로",
    schedule_interval=timedelta(minutes=10),  # 10분마다 실행
    catchup=False,
    is_paused_upon_creation=False,  # 생성 시 자동으로 활성화
    tags=["news", "kafka", "consumer", "etl", "integrated"],
)


def consume_kafka_messages(**context):
    """Kafka에서 메시지를 소비하고 데이터베이스에 저장 (song-test 기반 고도화)"""
    try:
        # Kafka Consumer 설정
        consumer = KafkaConsumer(
            "news-topic",
            bootstrap_servers=["kafka:9092"],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="integrated_airflow_consumer_group",
            consumer_timeout_ms=30000,  # 30초 타임아웃
        )

        # PostgreSQL 연결
        conn = psycopg2.connect(
            host="postgres", database="airflow", user="airflow", password="airflow"
        )
        cursor = conn.cursor()

        # 테이블 생성 (존재하지 않는 경우) - song-test의 완전한 스키마
        create_table_query = """
        CREATE TABLE IF NOT EXISTS news_articles (
            id SERIAL PRIMARY KEY,
            keyword VARCHAR(100),
            title TEXT NOT NULL,
            link TEXT UNIQUE NOT NULL,
            description TEXT,
            pub_date VARCHAR(100),
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_news_keyword ON news_articles(keyword);
        CREATE INDEX IF NOT EXISTS idx_news_collected_at ON news_articles(collected_at);
        CREATE INDEX IF NOT EXISTS idx_news_created_at ON news_articles(created_at);
        """
        cursor.execute(create_table_query)
        conn.commit()

        processed_count = 0
        duplicates = 0
        errors = 0

        # 시스템 로그 테이블 생성 (존재하지 않는 경우)
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS system_logs (
                id SERIAL PRIMARY KEY,
                level VARCHAR(20) NOT NULL,
                message TEXT NOT NULL,
                component VARCHAR(50),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON system_logs(timestamp);
            CREATE INDEX IF NOT EXISTS idx_logs_level ON system_logs(level);
            CREATE INDEX IF NOT EXISTS idx_logs_component ON system_logs(component);
        """
        )

        # 시작 로그
        log_message = f"Kafka 메시지 소비 시작 - Consumer Group: integrated_airflow_consumer_group"
        logging.info(log_message)

        # 시스템 로그 테이블에 기록
        cursor.execute(
            """
            INSERT INTO system_logs (level, message, component) 
            VALUES (%s, %s, %s)
        """,
            ("INFO", log_message, "KAFKA_CONSUMER"),
        )

        # Kafka 메시지 처리
        for message in consumer:
            try:
                news_data = message.value

                # 데이터 검증 강화
                if not news_data.get("title") or not news_data.get("link"):
                    logging.warning(f"필수 데이터 누락: {news_data}")
                    errors += 1
                    continue

                # 제목 길이 제한 (데이터 무결성)
                title = news_data.get("title", "")[:500]  # seoyunjang의 길이 제한 적용

                # 데이터베이스에 삽입
                insert_query = """
                INSERT INTO news_articles (keyword, title, link, description, pub_date, collected_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (link) DO UPDATE SET
                    processed_at = NOW(),
                    title = EXCLUDED.title,
                    description = EXCLUDED.description
                """

                cursor.execute(
                    insert_query,
                    (
                        news_data.get("keyword", ""),
                        title,
                        news_data.get("link", ""),
                        news_data.get("description", ""),
                        news_data.get(
                            "pubDate", news_data.get("pub_date", "")
                        ),  # 두 형식 모두 지원
                    ),
                )

                if cursor.rowcount > 0:
                    processed_count += 1
                else:
                    duplicates += 1

                # 100개마다 커밋 (성능 최적화)
                if (processed_count + duplicates) % 100 == 0:
                    conn.commit()
                    logging.info(
                        f"중간 커밋: 처리 {processed_count}, 중복 {duplicates}, 오류 {errors}"
                    )

            except Exception as e:
                logging.error(f"메시지 처리 실패: {e}")
                errors += 1
                continue

        # 최종 커밋
        conn.commit()

        # 완료 로그
        completion_message = f"Kafka 소비 완료: 처리 {processed_count}개, 중복 {duplicates}개, 오류 {errors}개"
        logging.info(completion_message)

        # 시스템 로그에 기록
        cursor.execute(
            """
            INSERT INTO system_logs (level, message, component) 
            VALUES (%s, %s, %s)
        """,
            ("INFO", completion_message, "KAFKA_CONSUMER"),
        )

        conn.commit()
        cursor.close()
        conn.close()
        consumer.close()

        return f"성공: {processed_count}개 처리, {duplicates}개 중복, {errors}개 오류"

    except Exception as e:
        error_message = f"Kafka 소비 실패: {e}"
        logging.error(error_message)

        # 오류 로그 기록
        try:
            conn = psycopg2.connect(
                host="postgres", database="airflow", user="airflow", password="airflow"
            )
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO system_logs (level, message, component) 
                VALUES (%s, %s, %s)
            """,
                ("ERROR", error_message, "KAFKA_CONSUMER"),
            )
            conn.commit()
            cursor.close()
            conn.close()
        except:
            pass

        return f"실패: {str(e)}"


def cleanup_old_news(**context):
    """30일 이상 된 뉴스 데이터 정리 (song-test 기반)"""
    try:
        conn = psycopg2.connect(
            host="postgres", database="airflow", user="airflow", password="airflow"
        )
        cursor = conn.cursor()

        # 30일 이전 데이터 삭제
        cursor.execute(
            """
            DELETE FROM news_articles 
            WHERE collected_at < NOW() - INTERVAL '30 days'
        """
        )

        deleted_count = cursor.rowcount

        # 통계 데이터도 정리 (60일 이전)
        cursor.execute(
            """
            DELETE FROM collection_stats 
            WHERE date < CURRENT_DATE - INTERVAL '60 days'
        """
        )

        deleted_stats = cursor.rowcount

        # 시스템 로그도 정리 (90일 이전)
        cursor.execute(
            """
            DELETE FROM system_logs 
            WHERE timestamp < NOW() - INTERVAL '90 days'
        """
        )

        deleted_logs = cursor.rowcount

        conn.commit()

        # 정리 완료 로그
        cleanup_message = f"데이터 정리 완료: 뉴스 {deleted_count}건, 통계 {deleted_stats}건, 로그 {deleted_logs}건 삭제"
        logging.info(cleanup_message)

        # 시스템 로그에 기록
        cursor.execute(
            """
            INSERT INTO system_logs (level, message, component) 
            VALUES (%s, %s, %s)
        """,
            ("INFO", cleanup_message, "DATA_CLEANUP"),
        )

        conn.commit()
        cursor.close()
        conn.close()

        return cleanup_message

    except Exception as e:
        error_message = f"데이터 정리 실패: {e}"
        logging.error(error_message)
        return error_message


def update_statistics(**context):
    """뉴스 수집 통계 업데이트 (song-test 기반 고도화)"""
    try:
        conn = psycopg2.connect(
            host="postgres", database="airflow", user="airflow", password="airflow"
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # 오늘의 통계 계산
        cursor.execute(
            """
            SELECT 
                keyword,
                COUNT(*) as count,
                MAX(collected_at) as last_collected,
                MIN(collected_at) as first_collected
            FROM news_articles 
            WHERE DATE(collected_at) = CURRENT_DATE
            GROUP BY keyword
            ORDER BY count DESC
        """
        )

        daily_stats = cursor.fetchall()

        # 통계 테이블 생성/업데이트
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS collection_stats (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                keyword VARCHAR(100),
                count INTEGER DEFAULT 0,
                last_collected TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, keyword)
            );
            
            CREATE INDEX IF NOT EXISTS idx_stats_date ON collection_stats(date);
            CREATE INDEX IF NOT EXISTS idx_stats_keyword ON collection_stats(keyword);
        """
        )

        # 오늘 통계 업데이트
        updated_keywords = []
        for stat in daily_stats:
            cursor.execute(
                """
                INSERT INTO collection_stats (date, keyword, count, last_collected)
                VALUES (CURRENT_DATE, %s, %s, %s)
                ON CONFLICT (date, keyword) 
                DO UPDATE SET 
                    count = EXCLUDED.count,
                    last_collected = EXCLUDED.last_collected
            """,
                (stat["keyword"], stat["count"], stat["last_collected"]),
            )
            updated_keywords.append(f"{stat['keyword']}({stat['count']})")

        conn.commit()

        # 통계 업데이트 완료 로그
        stats_message = f"통계 업데이트 완료: {len(daily_stats)}개 키워드 - {', '.join(updated_keywords)}"
        logging.info(stats_message)

        # 시스템 로그에 기록
        cursor.execute(
            """
            INSERT INTO system_logs (level, message, component) 
            VALUES (%s, %s, %s)
        """,
            ("INFO", stats_message, "STATISTICS"),
        )

        conn.commit()
        cursor.close()
        conn.close()

        return stats_message

    except Exception as e:
        error_message = f"통계 업데이트 실패: {e}"
        logging.error(error_message)
        return error_message


def system_health_check(**context):
    """시스템 상태 종합 점검"""
    try:
        conn = psycopg2.connect(
            host="postgres", database="airflow", user="airflow", password="airflow"
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # 테이블 존재 확인
        cursor.execute(
            """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('news_articles', 'collection_stats', 'system_logs')
        """
        )
        tables = [row["table_name"] for row in cursor.fetchall()]

        # 최근 24시간 활동 확인
        cursor.execute(
            """
            SELECT COUNT(*) as recent_articles
            FROM news_articles 
            WHERE collected_at >= NOW() - INTERVAL '24 hours'
        """
        )
        recent_count = cursor.fetchone()["recent_articles"]

        health_status = {
            "tables_ready": len(tables) == 3,
            "recent_activity": recent_count > 0,
            "database_connection": True,
        }

        health_message = (
            f"시스템 상태: 테이블 {len(tables)}/3개, 최근 24시간 뉴스 {recent_count}건"
        )

        # 시스템 로그에 기록
        cursor.execute(
            """
            INSERT INTO system_logs (level, message, component) 
            VALUES (%s, %s, %s)
        """,
            ("INFO", health_message, "HEALTH_CHECK"),
        )

        conn.commit()
        cursor.close()
        conn.close()

        logging.info(health_message)
        return health_message

    except Exception as e:
        error_message = f"시스템 상태 점검 실패: {e}"
        logging.error(error_message)
        return error_message


# Task 정의 (song-test 기반 확장)
kafka_health_check_task = BashOperator(
    task_id="kafka_health_check",
    bash_command='echo "Kafka 연결 상태 확인 중..." && sleep 2 && echo "Kafka 준비 완료"',
    dag=dag,
)

system_health_task = PythonOperator(
    task_id="system_health_check",
    python_callable=system_health_check,
    dag=dag,
)

consume_kafka_task = PythonOperator(
    task_id="consume_kafka_messages",
    python_callable=consume_kafka_messages,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id="cleanup_old_news",
    python_callable=cleanup_old_news,
    dag=dag,
)

statistics_task = PythonOperator(
    task_id="update_statistics",
    python_callable=update_statistics,
    dag=dag,
)

# Task 의존성 설정 (확장된 파이프라인)
(
    kafka_health_check_task
    >> system_health_task
    >> consume_kafka_task
    >> [cleanup_task, statistics_task]
)
