-- ================================
-- Database Initialization Script
-- 통합 버전: song-test 기반 완전한 시스템 구조
-- ================================
-- 최종 업데이트: 2025-07-24
-- 
-- 주요 특징:
-- - song-test의 완전한 테이블 구조 채택
-- - 뉴스 기사, 수집 통계, 시스템 로그 테이블
-- - UNIQUE 제약조건, 인덱스, 샘플 데이터 포함
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

-- 인덱스 생성 (성능 최적화)
CREATE INDEX IF NOT EXISTS idx_news_keyword ON news_articles(keyword);
CREATE INDEX IF NOT EXISTS idx_news_collected_at ON news_articles(collected_at);
CREATE INDEX IF NOT EXISTS idx_news_pub_date ON news_articles(pub_date);
CREATE INDEX IF NOT EXISTS idx_news_created_at ON news_articles(created_at);

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

-- 통계 테이블 인덱스
CREATE INDEX IF NOT EXISTS idx_stats_date ON collection_stats(date);
CREATE INDEX IF NOT EXISTS idx_stats_keyword ON collection_stats(keyword);

-- 시스템 로그 테이블
CREATE TABLE IF NOT EXISTS system_logs (
    id SERIAL PRIMARY KEY,
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    component VARCHAR(50),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 시스템 로그 인덱스
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON system_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_level ON system_logs(level);
CREATE INDEX IF NOT EXISTS idx_logs_component ON system_logs(component);

-- 샘플 데이터 삽입 (테스트 및 초기 데이터)
INSERT INTO news_articles (keyword, title, link, description, pub_date) VALUES
('IT', '인공지능 기술의 새로운 돌파구', 'https://example.com/news1', 'AI 기술이 새로운 단계로 진입했습니다.', '2025-01-15'),
('주식', '증시 상승세 지속', 'https://example.com/news2', '코스피가 연일 상승세를 보이고 있습니다.', '2025-01-15'),
('AI', 'ChatGPT 새 버전 출시', 'https://example.com/news3', '더욱 향상된 AI 성능을 자랑합니다.', '2025-01-15'),
('반도체', '삼성전자 신규 반도체 공장 건설', 'https://example.com/news4', '차세대 반도체 생산을 위한 투자 확대', '2025-01-15'),
('경제', '한국 경제 성장률 전망', 'https://example.com/news5', '올해 경제 성장률이 예상보다 높을 것으로 전망됩니다.', '2025-01-15')
ON CONFLICT (link) DO NOTHING;

-- 초기 통계 데이터
INSERT INTO collection_stats (date, keyword, count, last_collected) VALUES
(CURRENT_DATE, 'IT', 1, NOW()),
(CURRENT_DATE, '주식', 1, NOW()),
(CURRENT_DATE, 'AI', 1, NOW()),
(CURRENT_DATE, '반도체', 1, NOW()),
(CURRENT_DATE, '경제', 1, NOW())
ON CONFLICT (date, keyword) DO NOTHING;

-- 초기 시스템 로그
INSERT INTO system_logs (level, message, component) VALUES
('INFO', '데이터베이스 초기화 완료', 'DATABASE'),
('INFO', '샘플 데이터 삽입 완료', 'DATABASE'),
('INFO', '시스템 준비 완료', 'SYSTEM');

-- 설정 완료 메시지
SELECT 
    'NewsDeck 통합 데이터베이스가 성공적으로 초기화되었습니다!' as status,
    COUNT(*) as sample_articles_count
FROM news_articles;
