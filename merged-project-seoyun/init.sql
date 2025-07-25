
-- ================================
-- Database Initialization Script
-- 통합 버전: song-test 기반 + seoyunjang 요소 추가
-- ================================
-- Base: song-test/init.sql (완전한 시스템 구조)
-- Replaced: seoyunjang/dags/setup_database.sql (기본 테이블만)
-- Last updated: 2025-07-23
-- 
-- 주요 변경사항:
-- - song-test 버전 채택: 더 완전한 테이블 구조 (news_articles, collection_stats, system_logs)
-- - UNIQUE 제약조건, 샘플 데이터 포함
-- - seoyunjang의 간단한 구조에서 → song-test의 확장된 구조로 업그레이드
-- ================================

-- 뉴스 기사 테이블 생성
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

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_news_keyword ON news_articles(keyword);
CREATE INDEX IF NOT EXISTS idx_news_collected_at ON news_articles(collected_at);
CREATE INDEX IF NOT EXISTS idx_news_pub_date ON news_articles(pub_date);

-- 뉴스 수집 통계 테이블
CREATE TABLE IF NOT EXISTS collection_stats (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    keyword VARCHAR(100),
    count INTEGER DEFAULT 0,
    last_collected TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, keyword)
);

-- 시스템 로그 테이블
CREATE TABLE IF NOT EXISTS system_logs (
    id SERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    component VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 샘플 데이터 삽입 (테스트용)
INSERT INTO news_articles (keyword, title, link, description, pub_date) VALUES
('IT', '인공지능 기술의 새로운 돌파구', 'https://example.com/news1', 'AI 기술이 새로운 단계로 진입했습니다.', '2025-01-15'),
('주식', '증시 상승세 지속', 'https://example.com/news2', '코스피가 연일 상승세를 보이고 있습니다.', '2025-01-15'),
('AI', 'ChatGPT 새 버전 출시', 'https://example.com/news3', '더욱 향상된 AI 성능을 자랑합니다.', '2025-01-15')
ON CONFLICT (link) DO NOTHING;
