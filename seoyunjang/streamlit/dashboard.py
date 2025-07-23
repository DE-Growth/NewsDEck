import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time

# 페이지 설정
st.set_page_config(
    page_title="뉴스 분석 대시보드",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 데이터베이스 연결 함수
@st.cache_data(ttl=60)  # 1분 캐시
def load_data():
    """PostgreSQL에서 뉴스 데이터 로드"""
    try:
        conn = psycopg2.connect(
            host="localhost",  # WSL2에서 Docker 접근
            port="5432",
            database="newsdb",
            user="newsuser",        # ✅ 올바른 사용자명
            password="newspass"     # ✅ 올바른 비밀번호
        )
        
        query = """
        SELECT 
            id, title, description, link, pub_date, 
            keyword, collected_at, created_at
        FROM news_articles 
        ORDER BY created_at DESC 
        LIMIT 1000;
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        # 날짜 컬럼 변환
        if not df.empty:
            df['created_at'] = pd.to_datetime(df['created_at'])
            df['collected_at'] = pd.to_datetime(df['collected_at'])
        
        return df
        
    except Exception as e:
        st.error(f"데이터 로드 오류: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_statistics():
    """통계 데이터 로드"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5432", 
            database="newsdb",
            user="newsuser",        # ✅ 올바른 사용자명
            password="newspass"     # ✅ 올바른 비밀번호
        )
        
        # 키워드별 통계
        keyword_query = """
        SELECT 
            keyword,
            COUNT(*) as count,
            MAX(created_at) as latest
        FROM news_articles 
        GROUP BY keyword 
        ORDER BY count DESC;
        """
        
        keyword_df = pd.read_sql(keyword_query, conn)
        
        # 시간대별 통계 (최근 24시간)
        hourly_query = """
        SELECT 
            DATE_TRUNC('hour', created_at) as hour,
            COUNT(*) as count
        FROM news_articles 
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        GROUP BY DATE_TRUNC('hour', created_at)
        ORDER BY hour;
        """
        
        hourly_df = pd.read_sql(hourly_query, conn)
        conn.close()
        
        return keyword_df, hourly_df
        
    except Exception as e:
        st.error(f"통계 로드 오류: {e}")
        return pd.DataFrame(), pd.DataFrame()

# 메인 대시보드
def main():
    # 제목
    st.title("📰 실시간 뉴스 분석 대시보드")
    st.markdown("---")
    
    # 사이드바
    st.sidebar.title("🎛️ 필터 옵션")
    
    # 데이터 로드
    with st.spinner("📡 데이터 로딩 중..."):
        df = load_data()
        keyword_stats, hourly_stats = get_statistics()
    
    if df.empty:
        st.warning("📭 표시할 뉴스 데이터가 없습니다.")
        st.info("💡 Producer를 실행하여 뉴스를 수집해보세요!")
        return
    
    # 전체 통계
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📊 총 뉴스 수", len(df))
    
    with col2:
        unique_keywords = df['keyword'].nunique()
        st.metric("🏷️ 키워드 수", unique_keywords)
    
    with col3:
        latest_time = df['created_at'].max()
        if pd.notna(latest_time):
            time_diff = datetime.now() - latest_time.to_pydatetime()
            minutes_ago = int(time_diff.total_seconds() / 60)
            st.metric("⏰ 최신 뉴스", f"{minutes_ago}분 전")
        else:
            st.metric("⏰ 최신 뉴스", "N/A")
    
    with col4:
        today_count = len(df[df['created_at'].dt.date == datetime.now().date()])
        st.metric("📅 오늘 뉴스", today_count)
    
    st.markdown("---")
    
    # 사이드바 필터
    keywords = ['전체'] + list(df['keyword'].unique())
    selected_keyword = st.sidebar.selectbox("🔍 키워드 선택", keywords)
    
    # 날짜 필터
    date_range = st.sidebar.date_input(
        "📅 날짜 범위",
        value=(datetime.now().date() - timedelta(days=1), datetime.now().date()),
        max_value=datetime.now().date()
    )
    
    # 데이터 필터링
    filtered_df = df.copy()
    
    if selected_keyword != '전체':
        filtered_df = filtered_df[filtered_df['keyword'] == selected_keyword]
    
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['created_at'].dt.date >= start_date) & 
            (filtered_df['created_at'].dt.date <= end_date)
        ]
    
    # 메인 콘텐츠
    tab1, tab2, tab3 = st.tabs(["📰 뉴스 목록", "📊 통계 분석", "⏰ 실시간 모니터링"])
    
    with tab1:
        st.subheader("📰 뉴스 목록")
        
        if filtered_df.empty:
            st.warning("선택한 조건에 맞는 뉴스가 없습니다.")
        else:
            # 뉴스 목록 표시
            for idx, row in filtered_df.head(20).iterrows():
                with st.container():
                    col1, col2 = st.columns([3, 1])
                    
                    with col1:
                        st.markdown(f"**{row['title']}**")
                        if row['description']:
                            st.markdown(f"*{row['description'][:200]}...*")
                        if row['link']:
                            st.markdown(f"🔗 [원문 보기]({row['link']})")
                    
                    with col2:
                        st.markdown(f"**키워드:** {row['keyword']}")
                        st.markdown(f"**수집 시간:** {row['created_at'].strftime('%m-%d %H:%M')}")
                    
                    st.markdown("---")
    
    with tab2:
        st.subheader("📊 통계 분석")
        
        if not keyword_stats.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🏷️ 키워드별 뉴스 수")
                fig_pie = px.pie(
                    keyword_stats, 
                    values='count', 
                    names='keyword',
                    title="키워드별 분포"
                )
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                st.subheader("📈 키워드별 뉴스 수 (막대그래프)")
                fig_bar = px.bar(
                    keyword_stats, 
                    x='keyword', 
                    y='count',
                    title="키워드별 뉴스 수"
                )
                st.plotly_chart(fig_bar, use_container_width=True)
        
        if not hourly_stats.empty:
            st.subheader("⏰ 시간대별 뉴스 수집 현황 (최근 24시간)")
            fig_line = px.line(
                hourly_stats, 
                x='hour', 
                y='count',
                title="시간대별 뉴스 수집량"
            )
            st.plotly_chart(fig_line, use_container_width=True)
    
    with tab3:
        st.subheader("⏰ 실시간 모니터링")
        
        # 자동 새로고침 버튼
        if st.button("🔄 새로고침"):
            st.rerun()
        
        # 최근 10개 뉴스
        st.subheader("📥 최신 뉴스 (실시간)")
        recent_news = df.head(10)
        
        for idx, row in recent_news.iterrows():
            st.markdown(f"**[{row['keyword']}]** {row['title']}")
            st.caption(f"수집 시간: {row['created_at'].strftime('%Y-%m-%d %H:%M:%S')}")
            st.markdown("---")
        
        # 자동 새로고침 설정
        st.info("💡 페이지는 수동으로 새로고침됩니다. 실시간 업데이트를 위해 새로고침 버튼을 클릭하세요.")

if __name__ == "__main__":
    main()