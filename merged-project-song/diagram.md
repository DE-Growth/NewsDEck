# System Diagrams (NewsDeck)

본 문서는 프로젝트의 주요 흐름을 Mermaid 다이어그램으로 정리합니다. 각 섹션에는 간략한 설명과 함께 상위 아키텍처, 수집-처리-시각화 시퀀스, Airflow DAG 의존성, 데이터 모델(개념적)을 포함합니다.

## 1) 상위 아키텍처 (End-to-End 흐름)

- 입력: Naver News API, 환경 변수(.env)
- 처리: Producer → Kafka → Airflow ETL → PostgreSQL
- 출력: Streamlit Dashboard(실시간 지표/시각화), 시스템 로그

```mermaid
graph TD
  subgraph Env[Environment]
    E[.env\nNAVER_CLIENT_ID\nNAVER_CLIENT_SECRET\nCOLLECT_KEYWORDS\nCOLLECT_INTERVAL\nARTICLES_PER_KEYWORD]
  end

  subgraph Producer[Producer Service]
    P[IntegratedNewsProducer\nproducer/producer.py]
  end

  subgraph Ext[Naver]
    N[Naver News API]
  end

  subgraph K[Kafka]
    KQ[(news-topic)]
  end

  subgraph AF[Airflow]
    A1[kafka_health_check]
    A2[system_health_check]
    A3[consume_kafka_messages]
    A4[cleanup_task]
    A5[statistics_task]
  end

  subgraph DB["PostgreSQL"]
    T1["news_articles\nUNIQUE(link)\nidx_news_keyword\nidx_news_collected_at"]
    T2["collection_stats\nidx_stats_date"]
    T3["system_logs\nidx_logs_timestamp"]
  end

  subgraph UI[Streamlit]
    S[IntegratedNewsDashboard\nstreamlit/dashboard.py]
  end

  E --> P
  P -->|keywords, interval| N
  N -->|articles JSON| P
  P -->|validated, cleaned JSON| KQ

  KQ --> A1
  A1 --> A2
  A2 --> A3
  A3 -->|UPSERT| T1
  A3 -->|log success/error| T3
  A3 --> A4
  A3 --> A5
  A4 -->|maintenance| T1
  A5 -->|aggregate update| T2

  S -->|read metrics| T1
  S -->|read stats| T2
  S -->|read logs| T3
  S -.->|health checks| DB
```

## 2) 수집-처리-시각화 시퀀스

- 스케줄/사용자 트리거에 따라 Producer가 키워드별 기사 수집
- Kafka로 발행된 메시지를 Airflow가 소비하여 DB에 적재/집계/정리
- Streamlit이 DB에서 최신 지표를 조회해 실시간 대시보드 제공

```mermaid
sequenceDiagram
  participant U as Scheduler/User
  participant P as IntegratedNewsProducer
  participant N as Naver News API
  participant K as Kafka (news-topic)
  participant D as Airflow DAG
  participant DB as PostgreSQL
  participant S as Streamlit Dashboard

  U->>P: Trigger by COLLECT_INTERVAL / keywords
  P->>N: Fetch articles per keyword
  N-->>P: Articles JSON
  P->>P: Clean HTML, validate, deduplicate
  P->>K: Publish messages

  D->>K: consume_kafka_messages
  K-->>D: Messages batch
  D->>DB: UPSERT news_articles (UNIQUE link)
  D->>DB: Write system_logs (success/errors)
  D->>DB: cleanup_task (maintenance)
  D->>DB: statistics_task (collection_stats)

  S->>DB: Query recent articles/stats/logs
  DB-->>S: Result sets
  S->>U: Render realtime metrics (KST)
```

## 3) Airflow DAG 의존성

- 사전 점검(카프카/시스템) 후 메시지 소비 → 정리/집계 분기
- 오류/중복은 DB 제약과 로깅으로 관리

```mermaid
graph TD
  A1[kafka_health_check]
  A2[system_health_check]
  A3[consume_kafka_messages]
  A4[cleanup_task]
  A5[statistics_task]

  A1 --> A2 --> A3
  A3 --> A4
  A3 --> A5
```
