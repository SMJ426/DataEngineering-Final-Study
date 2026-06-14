# 제출물

| No | 제출 항목 | 파일명 |
|-----|----------|---------|
| 1 | processed_event CREATE TABLE DDL | processed_event CREATE TABLE DDL.png |
| 2 | campaign_summary CREATE TABLE DDL | campaign_summary CREATE TABLE DDL.png |
| 3 | raw → processed MERGE 로직 | raw_to_processed_iceberg.py |
| 4 | processed → campaign_summary Incremental MERGE 로직 | processed_to_campaign_summary.py |
| 5 | Conversion Delay 반영 검증 결과 쿼리 | conversion_delay_check.png |
| 6 | Snapshot 확인 결과 | snapshot_result.png |

---

## 학습 내용
해당 차시까지 만든건 아래 파이프라인이다.
```
Kafka (ad-events)
    ↓  [Spark Structured Streaming]
Raw Zone (Parquet 파일)        ← Bronze
    ↓  [Spark Batch - raw_to_processed]
processed_events (Iceberg)     ← Silver
    ↓  [Spark Batch - processed_to_campaign_summary]
campaign_summary (Iceberg)     ← Gold
```

> 광고 이벤트 데이터가 Kafka에서 시작해서 3개 계층을 거쳐 최종 KPI 지표가 되는 흐름

---

### 문제
conversion(전환)이 며칠 뒤에 도착하는 문제가 발생한다.
```예시
월요일 10:00 → 광고 노출 (impression)
월요일 10:01 → 클릭 (click)
월요일 밤   → 일일 집계 실행 → conversion = 0 으로 기록
금요일 22:30 → 전환 이벤트 도착 ← 이미 집계된 row를 어떻게 갱신?
```
>전통 방식(full recompute)은 이걸 처리하려면 파티션 전체를 날리고 다시 써야 해서 비효율적이다. 
Iceberg의 MERGE INTO로 해결한다.

---

### MERGE INTO — 있으면 업데이트, 없으면 삽입
비유: 명함첩
```
새 명함이 왔을 때:
  → 이미 같은 이름이 있으면? 정보 업데이트
  → 없으면? 새로 추가
  ```

SQL로 표현
```sql
MERGE INTO 명함첩 t
USING 새명함 s ON t.이름 = s.이름   -- 이름이 같으면 같은 사람
WHEN MATCHED THEN                   -- 이미 있는 사람
  UPDATE SET t.전화번호 = s.전화번호, t.회사 = s.회사
WHEN NOT MATCHED THEN               -- 새로운 사람
  INSERT (이름, 전화번호, 회사) VALUES (s.이름, s.전화번호, s.회사)
```

이 실습에서의 역할:
```
processed_events에 이미 이런 row가 있음:
  event_id=evt_001, click=1, conversion=0, conversion_delay_sec=NULL

며칠 뒤 전환 데이터가 들어옴:
  event_id=evt_001, click=1, conversion=1, conversion_delay_sec=3600

MERGE INTO 실행:
  → event_id가 같으니 MATCHED
  → conversion=1, conversion_delay_sec=3600으로 업데이트
```
> 전체 파티션을 날리고 다시 쓰지 않고, 필요한 row만 논리적으로 갱신

---
### COW vs MOR — Iceberg가 파일을 수정하는 두 가지 방법
MERGE INTO가 실행될 때 Iceberg는 내부적으로 어떻게 파일을 처리할까

#### 1. Copy-on-Write (COW) — 복사 후 덮어쓰기
```
[BEFORE]
file-001.parquet: 행1, 행2, 행3(수정대상), 행4, 행5

[MERGE 실행]
file-001.parquet 전체를 읽음
행3만 수정
전체를 file-002.parquet으로 새로 씀

[AFTER]
file-001.parquet: 비활성 (metadata에서 제외)
file-002.parquet: 행1, 행2, 행3(수정됨), 행4, 행5
```

- 쓰기: 느림 (파일 전체 재작성)
- 읽기: 빠름 (그냥 최신 파일 읽으면 됨)

#### 2. Merge-on-Read (MOR) — 읽을 때 합치기
```
[BEFORE]
file-001.parquet: 행1, 행2, 행3(수정대상), 행4, 행5

[MERGE 실행]
file-001.parquet 그대로 유지
delete-file-001.parquet: "행3 삭제" 마커만 기록
file-002.parquet: 행3(수정됨)만 저장

[읽을 때]
file-001 - delete-file-001 + file-002 = 최종 결과
```
- 쓰기: 빠름 (변경분만 기록)
- 읽기: 느림 (합치기 연산 필요)


이 실습에서 COW 선택 이유:
- campaign_summary는 BI 대시보드에서 매초 읽힘
- MERGE는 하루에 한 번
- 읽기 >> 쓰기 → COW가 유리

---
### Medallion Architecture — 왜 3계층인가
```
Raw (Bronze)  →  Processed (Silver)  →  Summary (Gold)
원본 보관         정제 + 중복 제거          KPI 집계
```

#### Raw를 왜 남기나?
처리 로직에 버그가 있었다면? Raw가 있으면 다시 처리할 수 있습니다.
Raw를 지우면 원본 데이터가 영원히 사라집니다.

#### Processed가 왜 필요한가?
- Raw는 Kafka에서 온 그대로라 중복이 있을 수 있음
- conversion_delay_sec 같은 계산된 컬럼이 없음
- MERGE를 하려면 Iceberg 테이블이어야 함

#### Summary가 왜 필요한가?
- 마케터는 이벤트 1건씩이 아니라 "캠페인별 일일 CTR"이 필요
- Processed에서 매번 GROUP BY를 돌리면 비용이 큼
- Summary에 미리 집계해두면 빠르게 조회 가능

---
### Sliding Window — 전체 재계산 없이 최신 데이터만
campaign_summary를 매일 업데이트할 때:

나쁜 방법 (Full Recompute):
```
processed_events 전체 (1년치) GROUP BY
→ 매번 수백 GB 스캔
→ 느리고 비쌈
```

좋은 방법 (Incremental MERGE with Sliding Window):
```
processed_events WHERE event_date >= 오늘 - 7일
→ 최근 7일치만 스캔
→ 빠르고 저렴
→ 7일 안에 들어오는 지연 전환도 반영 가능
```

> 꼭 7일 아니어도 됨, 도메인별로 다름. (보험이나 비싼건 30일인 경우도 있음)

---
### 전체 흐름
```
1. CSV 데이터 생성 (광고 이벤트 10만건)

2. kafka_producer.py
   CSV → Kafka(ad-events 토픽)로 이벤트 발행

3. kafka_to_raw_files.py (Streaming)
   Kafka → raw Parquet 파일 저장
   (raw_date/raw_hour 파티션, append-only)

4. raw_to_processed_iceberg.py (full-refresh)
   raw Parquet → processed_events(Iceberg)
   - event_id 중복 제거
   - conversion_delay_sec 계산
   - event_date 파티션

5. batch2 데이터 발행 (전환 데이터 포함)
   Kafka → raw zone에 추가

6. raw_to_processed_iceberg.py (merge)
   새로 들어온 전환 데이터를 MERGE INTO로 반영
   - 기존 row: conversion=0 → conversion=1로 업데이트
   - 새 row: INSERT

7. processed_to_campaign_summary.py
   processed_events → campaign_summary(Iceberg)
   - 최근 7일 데이터 슬라이딩 윈도우
   - CTR/CVR/CPA 재계산
   - MERGE INTO로 날짜×캠페인 단위 upsert
```

>결국 이 파이프라인이 해결하는 것: 며칠 뒤 도착하는 전환 데이터를 전체 재처리 없이 효율적으로 반영하면서, 동시에 집계 지표를 정확하게 유지하는 것