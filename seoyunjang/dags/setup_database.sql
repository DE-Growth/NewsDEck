-- 뉴스 데이터 테이블 생성
CREATE TABLE IF NOT EXISTS news_articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    description TEXT,
    link VARCHAR(1000),
    pub_date VARCHAR(100),
    keyword VARCHAR(50),
    collected_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 성능 최적화를 위한 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_keyword ON news_articles(keyword);
CREATE INDEX IF NOT EXISTS idx_collected_at ON news_articles(collected_at);
CREATE INDEX IF NOT EXISTS idx_created_at ON news_articles(created_at);

-- 테이블 생성 확인
SELECT 'news_articles 테이블이 성공적으로 생성되었습니다!' as status;

-- 테이블 구조 확인 (선택사항)
-- \dt
-- \d news_articles