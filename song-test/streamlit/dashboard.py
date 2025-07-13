import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from sqlalchemy import create_engine, text
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import re
from kafka import KafkaConsumer
import threading
import time
import os
import pytz

# 한국 시간대 설정
KST = pytz.timezone('Asia/Seoul')

def get_korean_now():
    """현재 한국 시간을 반환"""
    return datetime.now(KST)

def get_korean_time_hours_ago(hours):
    """현재 한국 시간에서 N시간 전 시간을 반환"""
    return get_korean_now() - timedelta(hours=hours)

# Streamlit 페이지 설정
st.set_page_config(
    page_title="NewsDeck - 실시간 뉴스 대시보드",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 데이터베이스 연결 함수
def get_db_connection():
    """PostgreSQL 데이터베이스 연결 (매번 새로운 연결 생성)"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "postgres"),
            database=os.getenv("DB_NAME", "airflow"),
            user=os.getenv("DB_USER", "airflow"),
            password=os.getenv("DB_PASSWORD", "airflow"),
            port=os.getenv("DB_PORT", "5432"),
            connect_timeout=10
        )
        # 연결 테스트
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return conn
    except Exception as e:
        st.error(f"데이터베이스 연결 실패: {e}")
        return None

def get_sqlalchemy_engine():
    """SQLAlchemy 엔진 생성"""
    try:
        db_url = f"postgresql://{os.getenv('DB_USER', 'airflow')}:{os.getenv('DB_PASSWORD', 'airflow')}@{os.getenv('DB_HOST', 'postgres')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'airflow')}"
        engine = create_engine(db_url, connect_args={"connect_timeout": 10})
        # 연결 테스트
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"SQLAlchemy 엔진 생성 실패: {e}")
        return None

# 뉴스 데이터 로드 함수
def load_news_data(hours=24):
    """최근 N시간 뉴스 데이터 로드"""
    # 먼저 psycopg2로 테이블 체크 및 생성
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # 테이블이 존재하는지 확인
        cursor = conn.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'news_articles'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            # 테이블이 없으면 생성
            cursor.execute("""
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
            """)
            conn.commit()
            
            # 샘플 데이터 삽입
            cursor.execute("""
                INSERT INTO news_articles (keyword, title, link, description, pub_date) VALUES
                ('IT', '인공지능 기술의 새로운 돌파구', 'https://example.com/news1', 'AI 기술이 새로운 단계로 진입했습니다.', '2025-01-15'),
                ('주식', '증시 상승세 지속', 'https://example.com/news2', '코스피가 연일 상승세를 보이고 있습니다.', '2025-01-15'),
                ('AI', 'ChatGPT 새 버전 출시', 'https://example.com/news3', '더욱 향상된 AI 성능을 자랑합니다.', '2025-01-15')
                ON CONFLICT (link) DO NOTHING;
            """)
            conn.commit()
        
        cursor.close()
        conn.close()
        
        # SQLAlchemy로 데이터 조회
        engine = get_sqlalchemy_engine()
        if not engine:
            return pd.DataFrame()
        
        # 한국 시간 기준으로 N시간 전 시간 계산
        korean_time_ago = get_korean_time_hours_ago(hours)
        
        query = f"""
        SELECT 
            id, keyword, title, link, description, 
            pub_date, collected_at, processed_at
        FROM news_articles 
        WHERE collected_at >= '{korean_time_ago.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp
        ORDER BY collected_at DESC
        LIMIT 1000
        """
        
        df = pd.read_sql_query(query, engine)
        engine.dispose()
        return df
        
    except Exception as e:
        st.error(f"데이터 로드 실패: {e}")
        if conn:
            conn.close()
        return pd.DataFrame()

# 통계 데이터 로드 함수
def load_statistics():
    """뉴스 통계 데이터 로드"""
    print("=== load_statistics 함수 시작 ===")
    conn = get_db_connection()
    if not conn:
        print("데이터베이스 연결 실패")
        return {}
    
    print("데이터베이스 연결 성공")
    
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        print("커서 생성 완료")
        
        # 테이블 존재 확인
        print("테이블 존재 확인 쿼리 실행 중...")
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'news_articles'
            ) as table_exists;
        """)
        result_row = cursor.fetchone()
        table_exists = result_row['table_exists']
        print(f"테이블 존재 여부: {table_exists}")
        
        if not table_exists:
            cursor.close()
            conn.close()
            print("테이블이 존재하지 않음 - 기본값 반환")
            return {
                'overall': {'total_articles': 0, 'total_keywords': 0, 'last_update': None},
                'daily': {'daily_articles': 0, 'daily_keywords': 0},
                'keywords': [],
                'hourly_trend': []
            }
        
        print("전체 통계 쿼리 실행 중...")
        # 전체 통계
        cursor.execute("""
            SELECT 
                COUNT(*) as total_articles,
                COUNT(DISTINCT keyword) as total_keywords,
                MAX(collected_at) as last_update
            FROM news_articles
        """)
        overall_stats = cursor.fetchone()
        print(f"전체 통계 결과: {dict(overall_stats) if overall_stats else 'None'}")
        
        print("24시간 통계 쿼리 실행 중...")
        # 24시간 통계 (한국 시간 기준)
        korean_24h_ago = get_korean_time_hours_ago(24)
        cursor.execute(f"""
            SELECT 
                COUNT(*) as daily_articles,
                COUNT(DISTINCT keyword) as daily_keywords
            FROM news_articles 
            WHERE collected_at >= '{korean_24h_ago.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp
        """)
        daily_stats = cursor.fetchone()
        print(f"24시간 통계 결과: {dict(daily_stats) if daily_stats else 'None'}")
        
        print("키워드별 통계 쿼리 실행 중...")
        # 키워드별 통계 (한국 시간 기준)
        cursor.execute(f"""
            SELECT 
                keyword, 
                COUNT(*) as count,
                MAX(collected_at) as last_collected
            FROM news_articles 
            WHERE collected_at >= '{korean_24h_ago.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp
            GROUP BY keyword 
            ORDER BY count DESC
            LIMIT 20
        """)
        keyword_stats = cursor.fetchall()
        print(f"키워드 통계 결과 개수: {len(keyword_stats)}")
        
        print("시간별 트렌드 쿼리 실행 중...")
        # 시간별 수집 트렌드 (한국 시간 기준)
        cursor.execute(f"""
            SELECT 
                DATE_TRUNC('hour', collected_at) as hour,
                COUNT(*) as count
            FROM news_articles 
            WHERE collected_at >= '{korean_24h_ago.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp
            GROUP BY hour 
            ORDER BY hour
        """)
        hourly_trend = cursor.fetchall()
        print(f"시간별 트렌드 결과 개수: {len(hourly_trend)}")
        
        cursor.close()
        conn.close()
        
        result = {
            'overall': dict(overall_stats) if overall_stats else {},
            'daily': dict(daily_stats) if daily_stats else {},
            'keywords': [dict(row) for row in keyword_stats],
            'hourly_trend': [dict(row) for row in hourly_trend]
        }
        
        print(f"최종 result 생성 완료: {result}")
        print("=== load_statistics 함수 종료 ===")
        return result
        
    except Exception as e:
        import traceback
        print(f"통계 데이터 로드 중 예외 발생: {e}")
        print(f"예외 타입: {type(e)}")
        print(f"예외 상세: {traceback.format_exc()}")
        st.error(f"통계 데이터 로드 실패: {e}")
        if conn:
            conn.close()
        return {}

# 실시간 Kafka 소비자 (백그라운드)
def kafka_consumer_thread():
    """Kafka에서 실시간 뉴스 소비"""
    try:
        consumer = KafkaConsumer(
            'news-topic',
            bootstrap_servers=['kafka:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        for message in consumer:
            # 실시간 데이터를 세션 상태에 저장
            if 'realtime_news' not in st.session_state:
                st.session_state.realtime_news = []
            
            st.session_state.realtime_news.append(message.value)
            
            # 최근 10개만 유지
            if len(st.session_state.realtime_news) > 10:
                st.session_state.realtime_news = st.session_state.realtime_news[-10:]
                
    except Exception as e:
        st.error(f"Kafka 연결 실패: {e}")

# 메인 대시보드 UI
def main():
    # 헤더
    st.title("📰 NewsDeck - 실시간 뉴스 대시보드")
    
    # 현재 한국 시간 표시
    current_korean_time = get_korean_now()
    st.markdown(f"**현재 시간 (KST):** {current_korean_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    st.markdown("---")
    
    # 사이드바 설정
    st.sidebar.title("⚙️ 설정")
    
    # 시간 범위 선택
    time_range = st.sidebar.selectbox(
        "데이터 조회 범위",
        options=[1, 6, 12, 24, 48],
        index=3,
        format_func=lambda x: f"최근 {x}시간"
    )
    
    # 자동 새로고침 설정
    auto_refresh = st.sidebar.checkbox("자동 새로고침 (30초)", value=False)
    if auto_refresh:
        time.sleep(30)
        st.rerun()
    
    # 수동 새로고침 버튼
    if st.sidebar.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()
    
    # 통계 데이터 로드
    stats = load_statistics()
    
    # KPI 메트릭 표시
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "전체 수집 뉴스",
                stats.get('overall', {}).get('total_articles', 0),
                delta=stats.get('daily', {}).get('daily_articles', 0)
            )
        
        with col2:
            st.metric(
                "오늘 수집 뉴스",
                stats.get('daily', {}).get('daily_articles', 0)
            )
        
        with col3:
            st.metric(
                "추적 키워드",
                stats.get('overall', {}).get('total_keywords', 0)
            )
        
        with col4:
            last_update = stats.get('overall', {}).get('last_update')
            if last_update:
                # UTC 시간을 한국 시간으로 변환
                if last_update.tzinfo is None:
                    # naive datetime을 UTC로 가정하고 한국 시간으로 변환
                    last_update_utc = pytz.UTC.localize(last_update)
                    last_update_kst = last_update_utc.astimezone(KST)
                else:
                    last_update_kst = last_update.astimezone(KST)
                st.metric("마지막 업데이트 (KST)", last_update_kst.strftime("%H:%M"))
    
    st.markdown("---")
    
    # 메인 콘텐츠 탭
    tab1, tab2, tab3, tab4 = st.tabs(["📊 대시보드", "📰 뉴스 목록", "📈 분석", "⚡ 실시간"])
    
    with tab1:
        # 키워드별 뉴스 수 차트
        if stats and stats.get('keywords'):
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("키워드별 뉴스 수 (24시간)")
                keyword_df = pd.DataFrame(stats['keywords'])
                
                fig = px.bar(
                    keyword_df,
                    x='keyword',
                    y='count',
                    title="키워드별 뉴스 수집 현황",
                    color='count',
                    color_continuous_scale='Blues'
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("키워드 분포")
                fig = px.pie(
                    keyword_df,
                    values='count',
                    names='keyword',
                    title="키워드별 비율"
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # 시간별 수집 트렌드
        if stats and stats.get('hourly_trend'):
            st.subheader("시간별 뉴스 수집 트렌드 (24시간)")
            trend_df = pd.DataFrame(stats['hourly_trend'])
            trend_df['hour'] = pd.to_datetime(trend_df['hour'])
            
            fig = px.line(
                trend_df,
                x='hour',
                y='count',
                title="시간별 뉴스 수집량",
                markers=True
            )
            fig.update_layout(xaxis_title="시간", yaxis_title="뉴스 수")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader(f"📰 최근 {time_range}시간 뉴스 목록")
        
        # 뉴스 데이터 로드
        news_df = load_news_data(time_range)
        
        if not news_df.empty:
            # 필터링 옵션
            col1, col2 = st.columns(2)
            with col1:
                selected_keywords = st.multiselect(
                    "키워드 필터",
                    options=news_df['keyword'].unique(),
                    default=news_df['keyword'].unique()
                )
            
            with col2:
                search_text = st.text_input("제목 검색", "")
            
            # 필터 적용
            filtered_df = news_df[news_df['keyword'].isin(selected_keywords)]
            if search_text:
                filtered_df = filtered_df[
                    filtered_df['title'].str.contains(search_text, case=False, na=False)
                ]
            
            st.info(f"총 {len(filtered_df)}개의 뉴스가 검색되었습니다.")
            
            # 뉴스 목록 표시
            for idx, row in filtered_df.iterrows():
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.markdown(f"**[{row['keyword']}] {row['title']}**")
                        st.markdown(f"{row['description'][:200]}...")
                        st.markdown(f"🔗 [원문 보기]({row['link']})")
                    
                    with col2:
                        st.markdown(f"**수집 시간**")
                        st.markdown(f"{row['collected_at'].strftime('%m-%d %H:%M')}")
                    
                    st.markdown("---")
        else:
            st.warning("뉴스 데이터가 없습니다.")
    
    with tab3:
        st.subheader("📈 뉴스 분석")
        
        if not news_df.empty:
            # 일별 수집 트렌드
            news_df['date'] = pd.to_datetime(news_df['collected_at']).dt.date
            daily_counts = news_df.groupby(['date', 'keyword']).size().reset_index(name='count')
            
            fig = px.line(
                daily_counts,
                x='date',
                y='count',
                color='keyword',
                title="일별 키워드별 뉴스 수집 트렌드"
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # 키워드 상관관계 히트맵
            keyword_pivot = news_df.pivot_table(
                index=news_df['collected_at'].dt.hour,
                columns='keyword',
                values='id',
                aggfunc='count',
                fill_value=0
            )
            
            if not keyword_pivot.empty:
                fig = px.imshow(
                    keyword_pivot.corr(),
                    title="키워드 간 수집 패턴 상관관계",
                    aspect="auto"
                )
                st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("⚡ 실시간 뉴스 스트림")
        
        # 실시간 뉴스 표시
        if 'realtime_news' in st.session_state and st.session_state.realtime_news:
            st.success("실시간 뉴스 스트림이 활성화되었습니다.")
            
            for news in reversed(st.session_state.realtime_news):
                with st.container():
                    st.markdown(f"**[{news.get('keyword', 'Unknown')}] {news.get('title', 'No Title')}**")
                    st.markdown(f"{news.get('description', 'No Description')[:150]}...")
                    st.markdown(f"수집 시간: {news.get('collected_at', 'Unknown')}")
                    st.markdown("---")
        else:
            st.info("실시간 뉴스 데이터를 기다리는 중입니다...")
            
            # Kafka 연결 상태 확인
            if st.button("Kafka 연결 테스트"):
                try:
                    # 간단한 Kafka 연결 테스트
                    consumer = KafkaConsumer(
                        bootstrap_servers=['kafka:9092'],
                        consumer_timeout_ms=1000
                    )
                    consumer.close()
                    st.success("Kafka 연결 성공!")
                except Exception as e:
                    st.error(f"Kafka 연결 실패: {e}")

# 백그라운드 Kafka 소비자 시작
if 'kafka_started' not in st.session_state:
    st.session_state.kafka_started = True
    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start()

if __name__ == "__main__":
    main()