import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import time
import os
import pytz
from typing import Dict, List, Optional

# Streamlit 페이지 설정
st.set_page_config(
    page_title="NewsDeck - 통합 뉴스 대시보드",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 커스텀 CSS
st.markdown(
    """
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        border-left: 4px solid #1f77b4;
    }
    .status-good { color: #28a745; }
    .status-warning { color: #ffc107; }
    .status-error { color: #dc3545; }
</style>
""",
    unsafe_allow_html=True,
)


class IntegratedNewsDashboard:
    """
    통합 뉴스 대시보드
    - song-test의 고도화된 기능 기반
    - seoyunjang의 간결한 구조 참고
    """

    def __init__(self):
        self.db_config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "database": os.getenv("DB_NAME", "airflow"),
            "user": os.getenv("DB_USER", "airflow"),
            "password": os.getenv("DB_PASSWORD", "airflow"),
            "port": os.getenv("DB_PORT", "5432"),
        }

        # 데이터베이스 URL 생성
        self.db_url = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"

        # 한국 시간대 설정
        self.kst = pytz.timezone("Asia/Seoul")

    @st.cache_resource(ttl=300)
    def get_db_engine(_self):
        """SQLAlchemy 엔진 생성 및 캐싱"""
        try:
            engine = create_engine(_self.db_url)
            return engine
        except Exception as e:
            st.error(f"데이터베이스 연결 오류: {e}")
            return None

    @st.cache_data(ttl=60)
    def load_news_data(_self, hours: int = 24, limit: int = 1000) -> pd.DataFrame:
        """뉴스 데이터 로드"""
        try:
            engine = _self.get_db_engine()
            if engine is None:
                return pd.DataFrame()

            query = f"""
            SELECT 
                id, keyword, title, link, description, 
                pub_date, collected_at, processed_at, created_at
            FROM news_articles 
            WHERE collected_at >= NOW() - INTERVAL '{hours} hours'
            ORDER BY collected_at DESC
            LIMIT {limit}
            """

            df = pd.read_sql_query(query, engine)

            # 날짜 컬럼 변환 (한국 시간대 적용)
            if not df.empty:
                for col in ["collected_at", "processed_at", "created_at"]:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col])
                        # UTC에서 한국 시간대로 변환
                        if df[col].dt.tz is None:
                            df[col] = (
                                df[col].dt.tz_localize("UTC").dt.tz_convert(_self.kst)
                            )
                        else:
                            df[col] = df[col].dt.tz_convert(_self.kst)

            return df

        except Exception as e:
            st.error(f"뉴스 데이터 로드 실패: {e}")
            return pd.DataFrame()

    @st.cache_data(ttl=60)
    def load_statistics(_self) -> Dict:
        """통계 데이터 로드"""
        try:
            engine = _self.get_db_engine()
            if engine is None:
                return {}

            with engine.connect() as conn:
                # 전체 통계
                overall_query = """
                    SELECT 
                        COUNT(*) as total_articles,
                        COUNT(DISTINCT keyword) as total_keywords,
                        MAX(collected_at) as last_update
                    FROM news_articles
                """
                overall_result = conn.execute(text(overall_query)).fetchone()
                overall_stats = dict(overall_result._mapping) if overall_result else {}

                # 24시간 통계
                daily_query = """
                    SELECT 
                        COUNT(*) as daily_articles,
                        COUNT(DISTINCT keyword) as daily_keywords
                    FROM news_articles 
                    WHERE collected_at >= NOW() - INTERVAL '24 hours'
                """
                daily_result = conn.execute(text(daily_query)).fetchone()
                daily_stats = dict(daily_result._mapping) if daily_result else {}

                # 키워드별 통계
                keyword_query = """
                    SELECT 
                        keyword, 
                        COUNT(*) as count,
                        MAX(collected_at) as last_collected
                    FROM news_articles 
                    WHERE collected_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY keyword 
                    ORDER BY count DESC
                    LIMIT 20
                """
                keyword_results = conn.execute(text(keyword_query)).fetchall()
                keyword_stats = [dict(row._mapping) for row in keyword_results]

                # 시간대별 통계 (한국 시간대 기준)
                hourly_query = """
                    SELECT 
                        DATE_TRUNC('hour', collected_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul') as hour,
                        COUNT(*) as count
                    FROM news_articles 
                    WHERE collected_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY DATE_TRUNC('hour', collected_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Seoul')
                    ORDER BY hour
                """
                hourly_results = conn.execute(text(hourly_query)).fetchall()
                hourly_stats = [dict(row._mapping) for row in hourly_results]

            engine.dispose()

            return {
                "overall": overall_stats,
                "daily": daily_stats,
                "keywords": keyword_stats,
                "hourly": hourly_stats,
            }

        except Exception as e:
            st.error(f"통계 데이터 로드 실패: {e}")
            return {}

    @st.cache_data(ttl=300)
    def load_system_logs(_self, limit: int = 50) -> pd.DataFrame:
        """시스템 로그 로드"""
        try:
            engine = _self.get_db_engine()
            if engine is None:
                return pd.DataFrame()

            query = f"""
            SELECT level, message, component, timestamp
            FROM system_logs 
            ORDER BY timestamp DESC
            LIMIT {limit}
            """

            df = pd.read_sql_query(query, engine)

            if not df.empty:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                # UTC에서 한국 시간대로 변환
                if df["timestamp"].dt.tz is None:
                    df["timestamp"] = (
                        df["timestamp"].dt.tz_localize("UTC").dt.tz_convert(_self.kst)
                    )
                else:
                    df["timestamp"] = df["timestamp"].dt.tz_convert(_self.kst)

            return df

        except Exception as e:
            # 시스템 로그 테이블이 없을 수 있으므로 에러를 조용히 처리
            return pd.DataFrame()

    def render_header(self):
        """헤더 렌더링"""
        st.markdown(
            '<h1 class="main-header">📰 NewsDeck 통합 뉴스 대시보드</h1>',
            unsafe_allow_html=True,
        )
        st.markdown("---")

    def render_sidebar(self) -> Dict:
        """사이드바 렌더링"""
        st.sidebar.header("🛠️ 대시보드 설정")

        # 시간 범위 선택
        time_range = st.sidebar.selectbox(
            "📅 시간 범위",
            options=[1, 6, 12, 24, 48, 72],
            index=3,  # 24시간 기본값
            format_func=lambda x: f"최근 {x}시간",
        )

        # 자동 새로고침
        auto_refresh = st.sidebar.checkbox("🔄 자동 새로고침 (30초)", value=False)

        # 키워드 필터
        keywords_filter = st.sidebar.text_input(
            "🔍 키워드 필터 (쉼표로 구분)", placeholder="예: AI, 주식, IT"
        )

        # 새로고침 버튼
        if st.sidebar.button("🔄 수동 새로고침", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        # 자동 새로고침 개선 (깜박임 방지)
        if auto_refresh:
            # 세션 상태로 마지막 새로고침 시간 추적
            if "last_refresh" not in st.session_state:
                st.session_state.last_refresh = time.time()

            current_time = time.time()
            if current_time - st.session_state.last_refresh >= 30:
                st.session_state.last_refresh = current_time
                st.rerun()

        return {
            "time_range": time_range,
            "auto_refresh": auto_refresh,
            "keywords_filter": (
                [k.strip() for k in keywords_filter.split(",") if k.strip()]
                if keywords_filter
                else []
            ),
        }

    def render_key_metrics(self, stats: Dict):
        """주요 지표 렌더링"""
        st.subheader("📊 주요 지표")

        overall = stats.get("overall", {})
        daily = stats.get("daily", {})

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            total_articles = overall.get("total_articles", 0)
            st.metric(
                label="총 뉴스 기사",
                value=f"{total_articles:,}",
                help="전체 수집된 뉴스 기사 수",
            )

        with col2:
            daily_articles = daily.get("daily_articles", 0)
            st.metric(
                label="24시간 수집",
                value=f"{daily_articles:,}",
                help="최근 24시간 동안 수집된 뉴스",
            )

        with col3:
            total_keywords = overall.get("total_keywords", 0)
            st.metric(
                label="전체 키워드",
                value=f"{total_keywords:,}",
                help="수집 중인 전체 키워드 수",
            )

        with col4:
            last_update = overall.get("last_update")
            if last_update:
                # UTC에서 한국 시간으로 변환
                last_update_kst = (
                    pd.to_datetime(last_update).tz_localize("UTC").tz_convert(self.kst)
                )
                last_update_str = last_update_kst.strftime("%H:%M")
                status = (
                    "🟢 정상"
                    if last_update_kst > datetime.now(self.kst) - timedelta(hours=1)
                    else "🟡 지연"
                )
            else:
                last_update_str = "없음"
                status = "🔴 중단"

            st.metric(
                label="마지막 업데이트",
                value=last_update_str,
                delta=status,
                help="가장 최근 뉴스 수집 시간 (한국 시간)",
            )

    def render_keyword_analysis(self, stats: Dict, filters: Dict):
        """키워드 분석 렌더링"""
        st.subheader("🔍 키워드별 분석")

        keywords_data = stats.get("keywords", [])
        if not keywords_data:
            st.info("표시할 키워드 데이터가 없습니다.")
            return

        # 필터 적용
        if filters["keywords_filter"]:
            keywords_data = [
                item
                for item in keywords_data
                if item["keyword"] in filters["keywords_filter"]
            ]

        if not keywords_data:
            st.warning("필터 조건에 맞는 데이터가 없습니다.")
            return

        # 데이터프레임 생성
        df_keywords = pd.DataFrame(keywords_data)

        col1, col2 = st.columns(2)

        with col1:
            # 키워드별 수집량 바 차트
            fig_bar = px.bar(
                df_keywords.head(10),
                x="keyword",
                y="count",
                title="키워드별 뉴스 수집량 (Top 10)",
                color="count",
                color_continuous_scale="blues",
            )
            fig_bar.update_layout(
                xaxis_title="키워드", yaxis_title="뉴스 수", showlegend=False
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col2:
            # 키워드별 수집량 파이 차트
            fig_pie = px.pie(
                df_keywords.head(8),
                values="count",
                names="keyword",
                title="키워드별 비율 (Top 8)",
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        # 상세 키워드 테이블
        st.subheader("📋 키워드별 상세 정보")

        display_df = df_keywords.copy()
        # 한국 시간대로 변환하여 표시
        display_df["last_collected"] = (
            pd.to_datetime(display_df["last_collected"])
            .dt.tz_localize("UTC")
            .dt.tz_convert(self.kst)
            .dt.strftime("%Y-%m-%d %H:%M")
        )
        display_df = display_df.rename(
            columns={
                "keyword": "키워드",
                "count": "수집량",
                "last_collected": "마지막 수집 (KST)",
            }
        )

        # 고정 높이로 테이블 안정화 (깜박임 방지)
        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=300,  # 고정 높이 설정
        )

    def render_time_series(self, stats: Dict):
        """시계열 분석 렌더링"""
        st.subheader("📈 시간대별 수집 트렌드")

        hourly_data = stats.get("hourly", [])
        if not hourly_data:
            st.info("표시할 시간대별 데이터가 없습니다.")
            return

        # 데이터프레임 생성 (한국 시간대 적용)
        df_hourly = pd.DataFrame(hourly_data)
        if not df_hourly.empty:
            df_hourly["hour"] = pd.to_datetime(df_hourly["hour"])
            # 이미 PostgreSQL에서 한국 시간으로 변환되었으므로 KST로 표시만 하면 됨
            df_hourly["hour_kst"] = df_hourly["hour"]

        # 시계열 차트 (한국 시간 표시)
        fig_time = px.line(
            df_hourly,
            x="hour_kst",
            y="count",
            title="시간대별 뉴스 수집량 (KST)",
            markers=True,
        )
        fig_time.update_layout(
            xaxis_title="시간 (KST)", yaxis_title="뉴스 수", hovermode="x unified"
        )
        fig_time.update_traces(line=dict(color="#1f77b4", width=3), marker=dict(size=8))

        st.plotly_chart(fig_time, use_container_width=True)

        # 시간대별 통계
        if len(df_hourly) > 1:
            avg_per_hour = df_hourly["count"].mean()
            max_hour = df_hourly.loc[df_hourly["count"].idxmax()]
            min_hour = df_hourly.loc[df_hourly["count"].idxmin()]

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("시간당 평균", f"{avg_per_hour:.1f}건")
            with col2:
                st.metric(
                    "최고 수집 시간",
                    f"{max_hour['hour_kst'].strftime('%H시')} (KST)",
                    f"{max_hour['count']}건",
                )
            with col3:
                st.metric(
                    "최저 수집 시간",
                    f"{min_hour['hour_kst'].strftime('%H시')} (KST)",
                    f"{min_hour['count']}건",
                )

    def render_recent_news(self, df: pd.DataFrame, filters: Dict):
        """최근 뉴스 목록 렌더링"""
        st.subheader("📰 최근 수집된 뉴스")

        if df.empty:
            st.info("표시할 뉴스가 없습니다.")
            return

        # 필터 적용
        filtered_df = df.copy()
        if filters["keywords_filter"]:
            filtered_df = filtered_df[
                filtered_df["keyword"].isin(filters["keywords_filter"])
            ]

        if filtered_df.empty:
            st.warning("필터 조건에 맞는 뉴스가 없습니다.")
            return

        # 뉴스 개수 선택
        num_news = st.selectbox("표시할 뉴스 수", [10, 20, 50, 100], index=1)

        # 뉴스 목록 표시
        for idx, row in filtered_df.head(num_news).iterrows():
            with st.expander(
                (
                    f"[{row['keyword']}] {row['title'][:80]}..."
                    if len(row["title"]) > 80
                    else f"[{row['keyword']}] {row['title']}"
                ),
                expanded=False,
            ):
                col1, col2 = st.columns([3, 1])

                with col1:
                    st.write(f"**제목:** {row['title']}")
                    if row["description"]:
                        st.write(
                            f"**내용:** {row['description'][:200]}..."
                            if len(row["description"]) > 200
                            else f"**내용:** {row['description']}"
                        )
                    st.write(f"**링크:** [뉴스 보기]({row['link']})")

                with col2:
                    st.write(f"**키워드:** {row['keyword']}")
                    # 한국 시간으로 표시
                    if pd.notnull(row["collected_at"]):
                        collected_time = row["collected_at"]
                        if hasattr(collected_time, "tz_localize"):
                            # 이미 시간대가 적용된 경우
                            time_str = collected_time.strftime("%m-%d %H:%M (KST)")
                        else:
                            time_str = collected_time.strftime("%m-%d %H:%M (KST)")
                        st.write(f"**수집시간:** {time_str}")
                    if row["pub_date"]:
                        st.write(f"**발행일:** {row['pub_date']}")

    def render_system_status(self):
        """시스템 상태 렌더링"""
        st.subheader("🖥️ 시스템 상태")

        # 시스템 로그 로드
        logs_df = self.load_system_logs()

        if not logs_df.empty:
            # 최근 로그 상태 분석
            recent_logs = logs_df.head(10)
            error_count = len(recent_logs[recent_logs["level"] == "ERROR"])
            warning_count = len(recent_logs[recent_logs["level"] == "WARNING"])

            col1, col2, col3 = st.columns(3)

            with col1:
                status_color = "🟢" if error_count == 0 else "🔴"
                st.metric(
                    "시스템 상태",
                    f"{status_color} {'정상' if error_count == 0 else '오류'}",
                )

            with col2:
                st.metric("최근 경고", f"{warning_count}건")

            with col3:
                st.metric("최근 오류", f"{error_count}건")

            # 최근 로그 목록
            with st.expander("📋 최근 시스템 로그", expanded=False):
                for _, log in recent_logs.iterrows():
                    level_color = {"INFO": "🟦", "WARNING": "🟨", "ERROR": "🟥"}.get(
                        log["level"], "⬜"
                    )

                    st.write(
                        f"{level_color} **[{log['component']}]** {log['message']} - {log['timestamp'].strftime('%m-%d %H:%M:%S (KST)')}"
                    )
        else:
            st.info("시스템 로그 데이터를 사용할 수 없습니다.")

    def run(self):
        """메인 대시보드 실행"""
        self.render_header()

        # 사이드바 설정
        filters = self.render_sidebar()

        # 데이터 로드
        with st.spinner("데이터를 로드하는 중..."):
            stats = self.load_statistics()
            news_df = self.load_news_data(hours=filters["time_range"])

        # 주요 지표
        self.render_key_metrics(stats)

        st.markdown("---")

        # 키워드 분석
        self.render_keyword_analysis(stats, filters)

        st.markdown("---")

        # 시계열 분석
        self.render_time_series(stats)

        st.markdown("---")

        # 최근 뉴스
        self.render_recent_news(news_df, filters)

        st.markdown("---")

        # 시스템 상태
        self.render_system_status()

        # 푸터 (한국 시간 표시)
        st.markdown("---")
        kst_time = datetime.now(self.kst)
        st.markdown(
            """
            <div style='text-align: center; color: #666; margin-top: 2rem;'>
                <p>📰 NewsDeck 통합 뉴스 대시보드 | 실시간 뉴스 수집 및 분석 시스템</p>
                <p>마지막 업데이트: {} (KST)</p>
            </div>
            """.format(
                kst_time.strftime("%Y-%m-%d %H:%M:%S")
            ),
            unsafe_allow_html=True,
        )


def main():
    """메인 함수"""
    try:
        dashboard = IntegratedNewsDashboard()
        dashboard.run()
    except Exception as e:
        st.error(f"대시보드 실행 중 오류가 발생했습니다: {e}")
        st.info("페이지를 새로고침하거나 관리자에게 문의하세요.")


if __name__ == "__main__":
    main()
