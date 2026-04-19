---
title: Git 기반 마크다운 RAG 인덱싱 파이프라인 설계
date: 2026-04-19
status: draft
---

# Git 기반 마크다운 RAG 인덱싱 파이프라인 설계

## TL;DR

Bitbucket 단일 레포의 마크다운 문서를 주기적 폴링으로 수집해 Milvus(하이브리드 검색 지원)에 적재하는 사내 인덱싱 파이프라인. Airflow 단일 DAG + 운영 CLI 2종. 본문은 사용자 전유 영역으로 파이프라인이 절대 수정하지 않고, 이미지 캡션은 VLM이 생성해 프론트매터 `images` 예약 필드에 구조화 저장한다. 청크는 본문과 이미지 두 종류로 한 컬렉션에 공존하며, chunk_uid 기반 upsert + stale 정리로 최종 일관성을 달성한다. 검색 레이어(랭킹, 리랭커, 에이전트 도구)는 별도 프로젝트로 분리.

## 목차

1. 목표와 비범위
2. 역할 분리 (파이프라인 vs 검색단)
3. 프론트매터 모델
4. 사용자 커밋 규칙
5. Milvus 컬렉션 스키마
6. 파이프라인 아키텍처
7. 변경 처리 흐름 상세
8. 봇 Git 전략
9. 이미지 처리
10. 정합성 & 백필
11. 보안 / 시크릿 / 접근 제어
12. 관찰성 & 실패 처리
13. 성공 기준 & SLO
14. 미해결 이슈 / 후속 결정

---

## 1. 목표와 비범위

### 목표
사내 Bitbucket 단일 레포에 보관된 마크다운 문서를 자동으로 수집·변환·임베딩하여 Milvus 벡터 DB에 최신 상태로 유지하는 인덱싱 파이프라인. 이 인덱스는 사내 지식 검색 챗봇의 데이터 소스가 된다.

### 범위 (In-scope)
- 단일 공식 소스: Bitbucket 레포의 `main` 브랜치에 머지된 `.md` 파일.
- 문서 종류 혼재 허용 (기술 문서, 정책, 회의록, 런북, FAQ 등).
- 한국어 위주, 영어 용어/코드 혼재 허용.
- 처리 범위: `.md` 본문 텍스트 + 본문에 삽입된 이미지의 VLM 캡션.
- 프론트매터 자동 관리 (예약 필드 생성/갱신, 사용자 필드 보존).
- 변경 동기화 (추가/수정/삭제/이동), 최종 일관성 보장.
- 운영 환경: 사내 온프레미스 (Airflow, Milvus, BGE-M3, LLM, VLM 모두 내부 호스팅).

### 비범위 (Out-of-scope)
- 검색 서비스 구현 — 하이브리드 랭킹 방식, 리랭커 모델, 에이전트 도구 설계 전부 검색단 책임.
- 사용자/권한 관리 — 접근 제어는 레포 단위로 해결. 파이프라인은 권한 개념을 다루지 않음.
- 챗봇 프론트엔드, 대화 세션, 답변 생성.
- `.md` 이외 형식(Confluence, Notion, Office 등).
- `.md`에 링크된 외부 문서 및 모든 첨부(PDF 등). 오직 이미지만 처리.
- 외부 URL 이미지, 심볼릭 링크, alias.
- `main` 외 브랜치, 태그, 릴리즈별 인덱싱.
- 실시간(sub-second) 스트리밍. 분 단위 지연 허용.
- Bitbucket 외의 Git 호스팅.

### 성공 기준 (요약, 상세는 섹션 13)
- Push → Milvus 반영 지연을 목표 SLO 내 유지.
- `git` 상태와 Milvus 상태가 자가 치유 루프를 통해 수렴.
- 개별 문서 실패가 전체 파이프라인을 멈추지 않음.

---

## 2. 역할 분리 (파이프라인 vs 검색단)

### 파이프라인의 책임 (이 프로젝트)
Bitbucket 마크다운 상태를 Milvus 인덱스 상태로 변환·동기화한다.

- 수집: `main`의 `.md` 파일을 주기적 폴링으로 가져온다.
- 가공: 프론트매터 관리, 청킹, 이미지 VLM 캡셔닝.
- 임베딩: BGE-M3로 dense + sparse 벡터 생성.
- 적재: Milvus 컬렉션에 upsert/delete. Upsert + stale 정리로 최종 일관성 확보.
- 정합성 유지: 실패 자동 재처리, CLI 기반 수동 drift 복구.
- 관찰성: 인덱싱 상태/실패/지연 메트릭 기록.

### 검색단의 책임 (별도 프로젝트)
Milvus에 적재된 데이터를 소비하는 모든 활동.

- 쿼리 분석과 검색 파라미터 결정.
- 하이브리드 검색 호출과 랭킹 결합 방식 결정 (RRF / Weighted 등).
- 리랭커 모델 선택/서빙/적용.
- 에이전트에게 제공할 도구 세트 설계.
- 사용자 권한 기반 필터링.
- 답변 생성(LLM), 세션 관리, UX 전반.

### 계약 인터페이스 (파이프라인 → 검색단)
- Milvus 컬렉션 이름: 설정값. 단일 컬렉션.
- 컬렉션 스키마 문서: 각 필드 이름/타입/의미/가능한 값 (섹션 5).
- 청크 데이터 모델: 청크 단위, `chunk_kind` 종류, breadcrumb 형식.
- 업데이트 SLO: 폴링 주기와 최악 지연 (섹션 13).
- 정합성 보장 수준: "최종 일관성. 자가 치유 후 `git HEAD == Milvus 상태`."
- 장애 시 동작: 개별 문서 실패는 `indexing_status: failed` 표시, 나머지 정상 서비스.

### 의존 금지
- 검색단은 파이프라인 내부 구현, 메타 스토어, Bitbucket API, Git 히스토리에 의존하지 않음.
- 파이프라인은 검색단의 사용 패턴, 리랭커 선택, 랭킹 방식에 의존하지 않음.

---

## 3. 프론트매터 모델

### 기본 원칙
프론트매터는 공동 공간이며 두 영역으로 나뉜다.

- **예약 필드 (pipeline-managed)**: 파이프라인이 생성·갱신. 사용자가 수정해도 다음 인덱싱에서 `-X ours` rebase로 봇이 덮어씀. CI는 차단하지 않음(경고만).
- **사용자 필드 (user-defined)**: 사용자가 자유롭게 추가. 파이프라인은 읽기만 하고 보존. 이름 제약 없음(팀 접두어 권장, 강제 아님).

### 예약 필드 목록

**식별/버전**
| 필드 | 타입 | 설명 |
|---|---|---|
| `doc_id` | string (UUID v4) | 문서 영속 식별자. 최초 인덱싱 시 발급, 이동/이름 변경에도 불변. |
| `path` | string | 현재 레포 내 상대 경로. |
| `content_hash` | string (sha256 hex) | 프론트매터 제외 본문의 해시. 변경 감지 기준. |
| `last_indexed_commit` | string (git SHA40) | 이 문서가 마지막으로 인덱싱된 커밋. |
| `last_indexed_at` | string (ISO 8601) | 마지막 인덱싱 시각(UTC). |
| `pipeline_schema_version` | int | 프론트매터 스키마 버전. |
| `chunk_strategy_version` | int | 청킹 전략 버전. |
| `embedding_model` | string | 예: `bge-m3@20260301`. |
| `vision_schema_version` | int | 이미지 처리 스키마 버전. |

**내용 분류 (LLM 자동 생성)**
| 필드 | 타입 | 설명 |
|---|---|---|
| `title` | string | 본문 첫 H1. 없거나 비정상이면 LLM 보정. |
| `summary` | string | 한 줄 요약 (50-100자). |
| `doc_type` | enum | `spec | runbook | meeting | policy | faq | how-to | reference | other`. |
| `topics` | string[] | 주요 키워드 3-7개. |
| `language` | enum | `ko | en | mixed`. |

**Git 유래 메타**
| 필드 | 타입 | 설명 |
|---|---|---|
| `authors` | string[] | git log 기반 주요 기여자 (상위 5명). |
| `created_at` | string (ISO 8601) | 최초 커밋 시각. |
| `updated_at` | string (ISO 8601) | 최근 본문 변경 커밋 시각(프론트매터 변경 제외). |

**운영 상태**
| 필드 | 타입 | 설명 |
|---|---|---|
| `chunk_count` | int | 현재 청크 수. |
| `has_images` | bool | 이미지 포함 여부. |
| `indexing_status` | enum | `ok | failed | excluded | pending`. |
| `web_url` | string | Bitbucket 원문 파일 URL. |

**이미지 설명**
| 필드 | 타입 | 설명 |
|---|---|---|
| `images` | array | 본문에서 참조되는 이미지별 캡션 구조. 섹션 9 참조. |

### 사용자 필드
- 예약 목록에 없는 임의의 YAML 키. 파이프라인이 보존.
- Milvus에는 `user_meta` JSON scalar 필드에 통째로 저장.

### 파이프라인이 해석하는 특수 사용자 플래그

| 플래그 | 타입 | 동작 |
|---|---|---|
| `rag_exclude` | bool | `true`면 인덱싱 제외. 기존 인덱싱 문서는 Milvus에서 해당 doc_id 전체 청크 삭제. 프론트매터는 `indexing_status: excluded`로 유지. |

### 충돌 정책
- 예약 필드: 봇이 `-X ours` rebase로 항상 이김. 사용자 학습 영역.
- 사용자 필드: 봇 수정 없음. 보존만.
- 신규 문서: 프론트매터가 비어 있거나 일부 사용자 필드만 있는 상태로 첫 커밋되면, 파이프라인이 예약 필드를 추가한 봇 커밋으로 보완.
- `rag_exclude: true`로 첫 커밋된 신규 문서: doc_id 발급/예약 필드 주입을 건너뜀. 파이프라인은 매 실행마다 프론트매터를 파싱해 확인하므로 별도 캐시 불필요.

### 프론트매터 예시 (인덱싱된 문서)
```yaml
---
doc_id: 550e8400-e29b-41d4-a716-446655440000
path: runbooks/kubernetes-장애대응.md
content_hash: sha256:3b2e9c...
last_indexed_commit: a1b2c3d4e5f6...
last_indexed_at: 2026-04-19T12:00:00Z
pipeline_schema_version: 1
chunk_strategy_version: 1
embedding_model: bge-m3@20260301
vision_schema_version: 1
title: Kubernetes 장애 대응 런북
summary: K8s 클러스터 장애 발생 시 진단과 복구 절차
doc_type: runbook
topics: [kubernetes, oncall, incident, recovery]
language: ko
authors: [randy, jane]
created_at: 2026-03-01T09:00:00Z
updated_at: 2026-04-17T18:30:00Z
chunk_count: 12
has_images: true
indexing_status: ok
web_url: https://bitbucket.org/ops/runbooks/src/main/runbooks/kubernetes-%EC%9E%A5%EC%95%A0%EB%8C%80%EC%9D%91.md
images:
  - path: runbooks/images/k8s-arch.png
    caption: Kubernetes 컨트롤 플레인과 워커 노드의 구성 요소 및 통신 경로.
    section_path: "Kubernetes 장애 대응 런북 > 아키텍처 개요"
owner_team: sre
review_status: approved
---
```

---

## 4. 사용자 커밋 규칙

### 파일/경로 규칙

**확장자**
- 인덱싱 대상: `.md`, `.markdown`. 그 외 확장자는 무시.

**파일명**
- 허용: 한글, 공백, 영문자, 숫자, 하이픈(`-`), 언더스코어(`_`), 괄호, 점.
- 금지:
  - 경로 구분자(`/`, `\`), NUL 문자, 제어 문자.
  - 선행 점(`.hidden.md`).
  - 같은 디렉토리 내 대소문자만 다른 파일명 중복.

**디렉토리**
- 레포 내 자유.
- 이미지는 참조 `.md`와 같은 디렉토리 또는 그 하위에만 위치 (섹션 9).
- `.pipeline/` 경로 수정은 CI 차단 (파이프라인 전용).

### 본문 작성 규칙
- 첫 줄 H1 단 1개. `title` 추출 기준. H1이 여럿이거나 없으면 LLM이 보정하되 경고 로그.
- 표준 CommonMark/GFM. 표, 코드블록, 체크리스트, 이미지 링크 모두 허용.
- 이미지 삽입은 같은 디렉토리 또는 하위의 상대 경로로.
- 외부 URL 이미지는 허용되지만 파이프라인이 처리하지 않음.
- 첨부 파일 링크(PDF 등)는 허용되지만 파이프라인이 처리하지 않음.

### 프론트매터 작성 규칙
- 자유롭게 추가 가능 (예약/사용자 필드 모델).
- 예약 필드 수정은 다음 인덱싱에서 봇이 덮어씀 (CI 차단 없음, 경고만).
- 사용자 필드는 보존.
- 특수 플래그: `rag_exclude: true`로 인덱싱 제외 가능.

### 브랜치/PR 워크플로
- 인덱싱 대상은 `main` 한정. feature 브랜치나 PR 상태 문서는 검색 노출 없음.
- 본문 변경은 PR로만 main 진입. 사용자는 main에 직접 push하지 않음. 봇 계정만 예외.
- PR 병합 방식(squash/merge)은 팀 관행대로.

### 문서 생명주기
| 동작 | 사용자 작업 | 파이프라인 반응 |
|---|---|---|
| 신규 생성 | 새 `.md` 파일 PR 머지. 프론트매터 비어 있어도 됨. | UUID 발급 → 예약 필드 채움 → 청킹/임베딩 → Milvus insert → 봇 커밋. |
| 본문 수정 | 본문만 수정 PR 머지. | `content_hash` 변경 감지 → Full replace → 프론트매터 갱신. |
| 삭제 | `git rm` 후 PR 머지. | 이전 프론트매터의 `doc_id`로 Milvus 청크 전체 삭제. |
| 이동/이름 변경 | `git mv` 후 PR 머지. content_hash 동일. | Milvus `path`/`source_url` 메타만 갱신. 재임베딩 없음. |
| 대량 편집 | 다수 파일 PR. | DAG가 diff 목록 순차 처리. 개별 실패는 해당 문서만 `failed`. |
| 인덱스에서 제외 | `rag_exclude: true` 추가 PR 머지. | doc_id 청크 삭제, 프론트매터 `excluded`. |
| 다시 인덱싱 | `rag_exclude` 제거/false. | 신규 인덱싱과 동일 처리. |

### CI 차단 (Fail-loud)
- `.pipeline/` 경로 변경 (관리자 예외 제외).
- 파일명 규칙 위반 (금지 문자/이름).
- 이미지 경로 규칙 위반 (섹션 9).

### CI 경고 (차단 없음)
- 예약 필드가 사용자 커밋에 포함됨: "다음 인덱싱에서 덮어씌워질 예정입니다."
- H1이 여럿이거나 없음: "제목 추출이 LLM 보정에 의존합니다."
- 매우 짧은 문서: "검색 품질이 낮을 수 있습니다."

---

## 5. Milvus 컬렉션 스키마

### 컬렉션 이름
운영자가 설정 파일에서 지정. 기본값 없음.

```yaml
milvus:
  host: milvus.internal
  port: 19530
  collection_name: docs
```

### 필드 정의

**식별**
| 필드 | 타입 | 설명 |
|---|---|---|
| `chunk_uid` | VARCHAR(128), PK | `{doc_id}:body:{index}` 또는 `{doc_id}:image:{index}`. |
| `doc_id` | VARCHAR(36) | UUID. scalar 인덱스. |
| `chunk_index` | INT64 | 청크 종류별 순번(0부터). |
| `chunk_kind` | VARCHAR(8) | `body` 또는 `image`. |

**벡터 (BGE-M3)**
| 필드 | 타입 | 설명 |
|---|---|---|
| `dense_vector` | FLOAT_VECTOR(1024) | BGE-M3 dense. 메트릭: COSINE. |
| `sparse_vector` | SPARSE_FLOAT_VECTOR | BGE-M3 lexical-weight. 메트릭: IP. |

**청크 콘텐츠**
| 필드 | 타입 | 설명 |
|---|---|---|
| `chunk_text` | VARCHAR(20000) | 임베딩 입력 원문 (breadcrumb 포함). |
| `breadcrumb` | VARCHAR(1024) | LangChain metadata 기반 heading 경로. 이미지 청크는 `section_path` 값. |

**문서 메타 (scalar 필터링용)**
| 필드 | 타입 | 설명 |
|---|---|---|
| `path` | VARCHAR(1024) | 레포 내 상대 경로. |
| `title` | VARCHAR(512) | 문서 제목. |
| `doc_type` | VARCHAR(32) | `spec`/`runbook`/... |
| `topics` | ARRAY<VARCHAR(64)> | `ARRAY_CONTAINS` 필터. |
| `language` | VARCHAR(16) | `ko`/`en`/`mixed`. |
| `authors` | ARRAY<VARCHAR(64)> | 주요 기여자. |
| `created_at` | INT64 (unix epoch ms) | 최초 커밋 시각. |
| `updated_at` | INT64 (unix epoch ms) | 최근 본문 변경 시각. |
| `last_indexed_at` | INT64 (unix epoch ms) | 마지막 인덱싱 시각. |
| `has_images` | BOOL | 이미지 포함 여부. |

**검색 편의**
| 필드 | 타입 | 설명 |
|---|---|---|
| `source_url` | VARCHAR(2048) | Bitbucket 웹 URL(라인 범위 fragment 포함). |
| `line_start` | INT64 | 본문 기준 청크 시작 라인(이미지 청크는 0). |
| `line_end` | INT64 | 본문 기준 청크 종료 라인. |

**사용자 필드**
| 필드 | 타입 | 설명 |
|---|---|---|
| `user_meta` | JSON | 프론트매터 사용자 필드 전체 통합 저장. |

### Milvus에 저장하지 않는 프론트매터 필드
`indexing_status`, `content_hash`, `last_indexed_commit`, `chunk_count`, `pipeline_schema_version`, `chunk_strategy_version`, `embedding_model`, `vision_schema_version`는 검색 시점에 불필요하므로 Milvus에 두지 않는다. 프론트매터와 `.pipeline/state.json`에만 보관.

### 인덱스 구성
| 필드 | 인덱스 | 파라미터 |
|---|---|---|
| `dense_vector` | HNSW (COSINE) | `M=24`, `efConstruction=200` (튜닝 대상) |
| `sparse_vector` | SPARSE_INVERTED_INDEX (IP) | 기본 |
| `doc_id` | INVERTED | 삭제/재인덱싱 시 대량 조회 |
| `chunk_uid` | (PK 자체 인덱스) | |
| `chunk_kind` | INVERTED | body/image 분리 필터 |
| `path`, `doc_type`, `language` | INVERTED | 검색단 필터 성능 |
| `topics`, `authors` | (Array 자체) | `ARRAY_CONTAINS` |

### Chunk UID 규약과 일관성 모델
- chunk_uid를 PK로 두어 Milvus `upsert`가 원자적 교체를 수행.
- 청크 업데이트 시 new 청크 리스트를 upsert → 이후 `doc_id == X`인 청크 중 new_chunk_uids에 없는 것을 delete (stale 정리).
- `version` 필드/컬럼 없음. upsert 자체가 검색 누락 없음.
- Full replace 정책: 본문 또는 이미지 어느 쪽이 변하든 전체 재청킹/재임베딩 (MVP). 부분 업데이트는 성능 이슈 확인 후 도입.

---

## 6. 파이프라인 아키텍처

### 실행 환경
- Airflow (사내 온프레미스).
- BGE-M3, LLM, VLM, Milvus 모두 사내 호스팅 엔드포인트.
- Bitbucket 접근은 SSH 기반만. API 호출 없음.
- 외부 노출된 파이프라인 엔드포인트 없음 (웹훅 없음, 폴링만).

### DAG 구성
단일 DAG: `indexer_dag`.

| 항목 | 값 |
|---|---|
| Schedule | `*/3 * * * *` (설정값) |
| max_active_runs | 1 (직렬화) |
| 역할 | 최신 커밋까지의 변경을 Milvus에 반영하고 프론트매터 갱신 |

**태스크 흐름**
1. `load_state` — 워크디렉토리의 `.pipeline/state.json` 읽어 `last_indexed_commit` 로드. 파일 없으면 "최초 실행".
2. `git_fetch` — `git fetch origin main`. 실패 시 DAG fail.
3. `compute_target_set` — target_set 계산:
   - diff에서 변경된 `.md` 파일.
   - diff에서 변경된 이미지의 조상 디렉토리 grep으로 찾은 참조 `.md`.
   - 레포에서 `indexing_status: failed`인 `.md`.
   - 비면 이후 skip.
4. `process_docs` (dynamic task mapping) — 파일별 케이스 흐름 병렬 처리 (섹션 7).
5. `collect_results` — 성공/실패 집계.
6. `update_state_file` — `.pipeline/state.json` 갱신.
7. `bot_commit_push` — 프론트매터 + state.json 한 커밋으로 묶어 `-X ours` rebase + push. race 시 지수 백오프 재시도(최대 10회), 최종 실패 시 DAG fail + alert.
8. `notify_failures` — 실패 문서가 있으면 Slack/메일 알림.

### 수동 운영 CLI (레포 함께 배포)
| 스크립트 | 역할 | 실행 시점 |
|---|---|---|
| `pipeline-drift-check.py` | git HEAD와 Milvus 상태 비교해 drift 리포트. `--repair`로 복구. | 드리프트 의심 시, 월 1회 정기. |
| `pipeline-backfill.py` | 스키마/모델 버전 변경 시 전체 재인덱싱. Shadow 컬렉션 + alias 교체(블루/그린). `--select <glob>`로 부분 백필. | 모델/스키마 업그레이드 시. |

두 스크립트는 `indexer_dag`와 공용 처리 모듈(청킹, 임베딩, Milvus 어댑터)을 import하여 로직 중복 없음.

### 공통 설계 패턴

**Dynamic task mapping**
문서별 처리는 Airflow `expand()` 기반. 병렬도(`max_map_length`)로 GPU 부하 조절.

**개별 실패 격리**
문서 처리 task는 실패해도 sibling task를 막지 않음 (`trigger_rule=all_done`로 후속 집계 태스크 진행). 실패 문서는 `indexing_status: failed` 기록, 다음 실행에서 자동 재시도.

**재시도 정책**
- I/O 실패 (네트워크, Milvus 타임아웃): task-level 지수 백오프 재시도(3회).
- 모델 호출 실패: 동일.
- 영구 실패 (지원하지 않는 포맷 등): 즉시 `failed` 기록, 재시도 없음.

### 워크디렉토리 & 인증
- Airflow 워커에 레포 클론 유지 (예: `/var/airflow/repo`). 매 실행은 `git fetch`만.
- 손실 시 자동 재클론.
- SSH private key는 Vault/K8s Secret에서 주입해 `~/.ssh/id_ed25519` 마운트.

### 설정 파일 (`pipeline-config.yaml` 예시)
```yaml
repo:
  ssh_url: git@bitbucket.org:ops/runbooks.git
  branch: main
  local_workdir: /var/airflow/repo

bitbucket:
  web_url_base: https://bitbucket.org/ops/runbooks

milvus:
  host: milvus.internal
  port: 19530
  collection_name: docs

embedding:
  model_name: bge-m3@20260301
  endpoint: http://bge-m3.internal:8000
  dense_dim: 1024

llm:
  endpoint: http://llm.internal:8000
  model_name: exaone-3.5-instruct

vision:
  caption_endpoint: http://vlm.internal:8000
  caption_model: qwen2-vl-7b
  prompt_path: prompts/vlm-caption.md
  prompt_version: 1
  caption_min_chars: 80
  caption_max_chars: 500
  cache_by_image_hash: true

chunking:
  headers_to_split_on:
    - ["#", "H1"]
    - ["##", "H2"]
    - ["###", "H3"]
  chunk_size: 1024
  chunk_overlap: 128
  strategy_version: 1

schedule:
  indexer: "*/3 * * * *"

bot:
  git_author_name: docs-bot
  git_author_email: docs-bot@internal
  commit_tag: "[pipeline-auto]"
  push_max_retries: 10
  push_backoff_initial_sec: 1
  push_backoff_max_sec: 300
  push_backoff_jitter: 0.2

secrets:
  ssh_key_source: vault
  ssh_key_path: /etc/airflow/secrets/id_ed25519
  known_hosts_path: /etc/airflow/secrets/known_hosts

observability:
  metrics_backend: prometheus
  metrics_endpoint: http://pushgw:9091
  log_format: json
  log_level: INFO

alerting:
  slack_webhook_env: SLACK_WEBHOOK_URL
  email_to: rag-team@internal
  thresholds:
    indexer_consecutive_failures_warn: 3
    indexer_consecutive_failures_crit: 10
    failed_docs_count_warn: 10
    failed_docs_count_crit: 50
    commit_lag_warn_sec: 600
    commit_lag_crit_sec: 3600
```

### 파이프라인 상태 저장소 (`.pipeline/`)
외부 DB 대신 레포 내 디렉토리를 상태 저장소로 사용.

```
<repo-root>/
├── .pipeline/
│   ├── state.json
│   └── README.md   ("파이프라인 전용. 수동 편집 금지" 안내)
```

`.pipeline/state.json`
```json
{
  "schema_version": 1,
  "last_indexed_commit": "a1b2c3d4e5f6...",
  "last_indexed_at": "2026-04-19T12:00:00Z",
  "pipeline_schema_version": 1,
  "chunk_strategy_version": 1,
  "embedding_model": "bge-m3@20260301",
  "vision_schema_version": 1
}
```

초기화/복구: 파일이 없거나 손상되면 파이프라인은 "최초 실행" 모드로 진입해 레포의 모든 `.md`를 신규로 처리. 강제 재인덱싱이 필요하면 관리자가 이 파일을 삭제하는 PR로 트리거.

`.pipeline/` 하위는 봇 전용. 사용자 PR에서 변경 시 CI가 차단.

실패 로그는 레포에 두지 않음. Airflow task 로그 + 프론트매터 `indexing_status: failed` + Slack 알림으로 충분.

---

## 7. 변경 처리 흐름 상세

### 일관성 모델 (사전 개념)
- PK 기준 upsert: Milvus PK는 `chunk_uid`. 같은 chunk_uid는 원자적 교체.
- chunk_uid 규약: 본문 `{doc_id}:body:{index}`, 이미지 `{doc_id}:image:{index}`.
- Stale 청크 정리: upsert 후 `doc_id == X` 청크의 chunk_uid 집합 중 new 집합에 없는 것 delete.
- 본문 불가침: 파이프라인은 본문을 수정하지 않음. `-X ours` rebase 충돌 영역도 프론트매터에 한정.

### 처리 대상 수집
```
target_set = diff 변경 .md ∪ 이미지 영향 .md ∪ indexing_status == failed .md
```

### 공통 서브흐름 A: 프론트매터 `images` 동기화 (본문 읽기 전용)
```
1. 본문 파싱 → 각 ![alt](path) 참조에서 (path, section_path) 수집
2. 프론트매터 images 와 diff:
   - 본문에만 존재 → VLM 캡션 생성 → images 리스트에 추가
   - 프론트매터에만 존재 → images에서 제거
   - 양쪽 존재 → section_path 갱신 (caption 유지)
3. 이미지 파일이 이번 diff에서 변경되었으면 해당 항목의 caption 재생성
4. images 리스트 path 기준 정렬
```

### Case 1: Added (신규 파일)
```
1. 파일 로드 → 프론트매터 파싱
2. rag_exclude == true → skip
3. doc_id == null → 새 UUID v4 발급
   doc_id != null → 비정상 이식, Modified 경로로 이동
4. 본문 content_hash 계산
5. 프론트매터 images 동기화 (공통 서브흐름 A)
6. LLM 호출: title 보정 / summary / doc_type / topics / language
7. Git 메타 추출: authors / created_at / updated_at
8. web_url 생성
9. 본문 청킹: LangChain MarkdownHeaderTextSplitter → RecursiveCharacterTextSplitter.
   반환 Document 리스트를 그대로 사용 (breadcrumb은 metadata에서 단순 join).
10. 이미지 청크 구성: 프론트매터 images 각 항목을 독립 청크로.
    chunk_text = "{section_path}\n\n{caption}"
11. 임베딩: BGE-M3 dense + sparse 배치 (본문 + 이미지 일괄)
12. Milvus upsert: 본문 chunk_uid = "{doc_id}:body:{i}", 이미지 chunk_uid = "{doc_id}:image:{i}".
    신규이므로 stale 정리 불필요.
13. 예약 필드 갱신 (content_hash, images, chunk_count, indexing_status=ok 등)
14. 봇 커밋 묶음에 파일 추가
```

### Case 2: Modified (본문 또는 이미지 변경)
```
1. 파일 로드 → 프론트매터 파싱
2. rag_exclude == true → "Exclude 전환" 서브흐름
3. 기존 프론트매터에서 doc_id 추출 (없으면 Added 경로)
4. 프론트매터 images 동기화 (공통 서브흐름 A)
5. 본문 content_hash 재계산 → 프론트매터 저장값과 비교
6. 변화 판정:
   - 본문 content_hash 동일 AND images 변경 없음:
     Milvus 작업 없음, 프론트매터만 원복 (-X ours rebase)
   - 그 외 (본문 변경 OR images 변경 OR 둘 다):
     Full replace (MVP 정책)

Full replace:
  a. LLM 재호출 (summary, doc_type, topics, language)
  b. Git 메타 갱신 (authors merge, updated_at)
  c. 본문 청킹 (LangChain)
  d. 이미지 청크 구성
  e. 임베딩 (본문 + 이미지)
  f. Milvus upsert
     new_chunk_uids = {"{doc_id}:body:0..N-1", "{doc_id}:image:0..M-1"}
  g. stale 정리:
     existing_uids = query(expr=f"doc_id == '{doc_id}'", output=["chunk_uid"])
     stale = existing_uids - new_chunk_uids
     if stale: delete(expr=f"chunk_uid in [{stale}]")
  h. 예약 필드 갱신
  i. 봇 커밋 묶음에 추가
```

### Case 3: Deleted (파일 삭제)
```
1. git show <last_indexed_commit>:path/to/doc.md → 프론트매터에서 doc_id 추출
2. doc_id 없음 또는 과거 rag_exclude였음 → skip
3. doc_id 있음 → Milvus delete (expr: doc_id == X)
4. 프론트매터 갱신 없음
```

### Case 4: Renamed — content 동일
```
1. 새 경로 파일 로드 → doc_id 추출 (유지)
2. 본문 content_hash 재계산 → 프론트매터 값과 일치 확인
3. 프론트매터 images 동기화 (이미지 참조 경로 변경 반영)
4. Milvus upsert로 path/source_url 메타 갱신:
   기존 chunk_uid 그대로, path/source_url 필드만 새 값
   벡터/chunk_text 재생성 없음
5. 프론트매터 path, web_url 갱신
6. 봇 커밋 묶음에 추가
```

### Case 5: Renamed + 본문 또는 이미지 변경
Case 2 Full replace 흐름 실행. path/web_url/source_url은 upsert 시 최신값으로 반영.

### 서브흐름: Exclude 전환
```
1. doc_id 추출
2. Milvus delete (expr: doc_id == X)
3. 프론트매터 갱신:
   - indexing_status = "excluded"
   - last_indexed_at, last_indexed_commit 갱신
   - 다른 필드는 유지(참고용)
4. 봇 커밋 묶음에 추가
```

`rag_exclude: true → false/삭제`은 Case 2 Full replace가 자연 처리.

### 서브흐름: Failed 재시도
`indexer_dag` 시작 시 `indexing_status: failed` 문서를 레포에서 grep으로 수집해 target_set에 합침. 처리 경로는 Case 2와 동일. content_hash가 그대로여도 재처리 강제. 재시도 성공 시 `ok`, 실패 시 `failed` 유지.

### 봇 커밋 생성
```
1. 변경된 프론트매터 .md 파일들 + .pipeline/state.json을 하나의 commit으로.
   본문은 절대 포함되지 않음 (검증 가드로 확인).
2. 저자 docs-bot, 메시지 "docs: index frontmatter (commit=<head>, N files) [pipeline-auto]"
3. git pull --rebase -X ours origin main
4. git push origin main (race 시 지수 백오프 재시도)
5. 다음 indexer_dag는 봇 커밋 이후 diff만 처리 (last_indexed_commit 전진)
```

### 처리 단위와 병렬성
target_set 각 문서는 Airflow `expand()`로 병렬 처리. 상한은 `max_map_length`. Milvus upsert/delete는 배치. 개별 문서 실패는 sibling에 영향 없음 (`trigger_rule=all_done`).

---

## 8. 봇 Git 전략

### 작업 흐름
```
git add <변경된 .md 파일들> .pipeline/state.json
git -c user.name=docs-bot \
    -c user.email=docs-bot@internal \
    commit -m "docs: index frontmatter (commit=<head_sha7>, N files) [pipeline-auto]"
git fetch origin main
git pull --rebase -X ours origin main
git push origin main   # race 시 지수 백오프 retry
```

### `-X ours` 전략의 의미
`pull --rebase -X ours`는 충돌 시 현재 작업(봇) 쪽을 채택. 봇이 건드리는 영역은 프론트매터와 `.pipeline/state.json`뿐이므로 본문 충돌은 원천 발생 불가 — 사용자 본문 손실 위험 0. 사용자가 예약 필드를 건드린 PR이 봇 실행 중 머지되는 드문 상황에서 "사용자 예약 필드 수정은 버려진다" 원칙이 그대로 구현됨.

### 커밋 조립 규칙
한 번의 `indexer_dag` 실행에서 여러 문서 변경이 발생해도 봇 커밋은 1개.

| 항목 | 값 |
|---|---|
| 저자 이름 | `docs-bot` (설정값) |
| 저자 이메일 | `docs-bot@internal` (설정값) |
| 메시지 첫 줄 | `docs: index frontmatter (commit=<head_sha7>, N files) [pipeline-auto]` |
| 포함 파일 | 프론트매터가 변경된 `.md` + `.pipeline/state.json` |

**검증 가드**: 스테이징 diff에서 YAML frontmatter 영역 외 라인이 변경돼 있으면 commit 직전에 abort + CRITICAL alert. 본문 불가침 원칙의 런타임 보호.

### 재시도 / 실패 정책
```
for attempt in 1..push_max_retries:   # 기본 10
    sleep(backoff_seconds[attempt])    # 1s → 2s → 4s ... 최대 300s, 지터 ±20%
    git fetch origin main
    git pull --rebase -X ours origin main
    if git push origin main succeeds: break
else:
    DAG task fail
    Slack alert: "bot push giving up after N retries"
```
최종 실패 시 DAG fail. 다음 `indexer_dag` 폴링에서 자연 재시도(`last_indexed_commit`이 전진하지 않음). N회 연속 실패 시 수동 점검.

### 봇 계정 / SSH 키
| 항목 | 내용 |
|---|---|
| 계정 | Bitbucket 서비스 계정 `docs-bot` |
| 인증 | SSH 키 (계정 레벨 또는 레포 access key) |
| 권한 | 대상 레포 main push + read. 다른 레포 접근 없음 |
| 키 저장 | Vault/K8s Secret → Airflow 워커 마운트 |
| 로테이션 | 연 1회 수동 |

### 루프 방지
1. **last_indexed_commit 전진**: 봇 push 성공 시 HEAD를 기록. 다음 폴링은 이 지점 이후 diff만 봄.
2. **봇 커밋 식별 가드 (보조)**: 커밋 저자가 `docs-bot`이고 메시지에 `[pipeline-auto]` 포함이면 재처리 skip.

### `.git-blame-ignore-revs`
본문을 건드리지 않으므로 본문 blame 노이즈 없음 → 별도 관리 불필요. 필요해지면 그때 도입.

### 실패 복구
| 상황 | 복구 |
|---|---|
| push 재시도 전부 실패 | 다음 `indexer_dag` 폴링이 동일 변경 재처리. 수동 개입 불필요. |
| 반복적 push 실패 | SSH 키/권한 확인, Bitbucket branch permission 점검. `pipeline-drift-check.py --repair`. |
| 본문 변경 포함 커밋 (검증 가드 발동) | DAG fail. 릴리스 롤백 대상. |
| 봇 계정 잠김 | 새 서비스 계정으로 SSH 키 교체 후 Airflow secret 갱신. |

---

## 9. 이미지 처리

### 처리 모델
파이프라인은 본문을 수정하지 않는다. 이미지 설명은 프론트매터 `images` 예약 필드에 저장되며, 이것이 독립 이미지 청크(`chunk_kind="image"`)로 Milvus에 적재된다.

- 본문 파싱은 읽기 전용 — 이미지 참조 경로와 참조된 heading 경로만 추출.
- 이미지 캡션은 VLM이 생성. OCR 별도 스택 없음 — 이미지 내 텍스트 추출은 VLM 프롬프트에 포함.
- 이미지 청크는 본문 청크와 동일 컬렉션에 `chunk_kind`로 구분.

### 프론트매터 `images` 필드
```yaml
images:
  - path: images/arch.png
    caption: "API Gateway가 Auth Service에 인증을 위임, 성공 시 Order Service로 라우팅."
    section_path: "결제 시스템 > 환불 정책"
```

- `path`: 본문 이미지 참조를 레포 루트 기준으로 정규화한 상대 경로.
- `caption`: VLM 생성 캡션.
- `section_path`: 이미지가 처음 참조된 heading 경로. breadcrumb 역할.

사용자 직접 편집 허용되지 않음 (예약 필드). 사용자가 쓰고 싶으면 사용자 필드에 별도 기록.

### 이미지 파일 위치 규칙
- 허용: 참조 `.md`와 같은 디렉토리 또는 그 하위.
- 금지: 레포 상위로 올라가는 경로(`../`), 외부 URL, 심볼릭 링크, alias.
- CI 린터로 차단.

### 변경 감지 — 조상 디렉토리 grep
```python
img_path = "docs/payment/images/arch.png"
img_name = basename(img_path)
dir = dirname(img_path)

candidates = []
while dir not in (".", "/"):
    candidates += glob(f"{dir}/*.md") + glob(f"{dir}/**/*.md")
    dir = dirname(dir)

referring_mds = [md for md in candidates if references(md, img_path)]
```
정규화 검증: grep 후보 중 각 md의 이미지 참조를 레포 루트 기준 절대 경로로 정규화해 `img_path`와 일치하는 것만 채택.

### VLM 프롬프트 가이드라인
- 객관적 묘사 우선 (형태가 아닌 의미 묘사).
- 이미지 내 텍스트/라벨 추출 (OCR 역할).
- 목표 길이 150-400자 (검색 친화).
- 문서 언어에 맞춤 (한국어 문서 → 한국어 캡션).
- 프롬프트는 레포 `prompts/vlm-caption.md`에서 버전 관리. 변경 시 `vision_schema_version` 증가.

### 이미지 청크 구성
```
chunk_uid:       "{doc_id}:image:{index}"
chunk_kind:      "image"
chunk_text:      "{section_path}\n\n{caption}"
breadcrumb:      section_path 값 그대로
source_url:      Bitbucket의 이미지 파일 URL
line_start/end:  0
has_images:      부모 문서가 true
```
BGE-M3 dense + sparse 임베딩은 본문 청크와 동일 배치 호출.

### 다맥락 이미지 / 중복 참조
- 같은 이미지를 여러 `.md`가 참조: 각 문서의 프론트매터에 독립 항목. 캡션이 같더라도 `section_path`가 다를 수 있음.
- VLM 호출 중복 제거: (image_path, image_content_hash) 키로 같은 실행 내 캐시.

### 이미지 삭제 / 리네임 (수동 처리)
- 이미지 파일 삭제: 본문 참조가 남아 있으면 `images`에 추가하지 않고 alert 발송 (깨진 링크).
- 이미지 리네임: `git mv` + 본문 참조 path 같이 업데이트된 PR이 정상. 누락 시 alert.
- 자동 수정 없음 (본문 불가침 원칙).

### Trade-off 공식화
- 다맥락 이미지 캡션 동일 사용: VLM 프롬프트의 객관성으로 완화.
- 봇 커밋 영향 범위 확대: 이미지 한 개 변경이 다수 문서 프론트매터에 영향. 본문은 불가침이라 실질 blame 노이즈는 없음.
- 이미지 삭제/리네임 수동 처리: 파이프라인 감지 후 alert만.
- 이미지 내 텍스트 정밀도는 VLM 성능에 종속.
- 렌더링 시 캡션 노출 없음: 프론트매터는 일반 렌더러에서 숨겨짐. 편집기/검색 결과로 확인.

### 실패 모드
| 실패 | 동작 |
|---|---|
| VLM 엔드포인트 타임아웃/에러 | 문서 `indexing_status: failed`, 다음 실행 재시도. |
| 이미지 파일 누락 (깨진 링크) | `images`에 미추가, alert. |
| 이미지 포맷 비지원 | 캡션 skip, 해당 이미지 청크 미생성, 경고 로그. |
| VLM 극도로 짧은/빈 응답 | 최소 길이 검증으로 failed 판정. |

---

## 10. 정합성 & 백필

### 정합성 정의
최종 일관성(eventual consistency). 충분한 시간이 흐른 뒤 `main`의 모든 `.md` 문서(`rag_exclude=true` 제외) 집합과 Milvus doc_id 집합이 일치하며, 각 doc_id의 청크가 프론트매터의 `content_hash`와 `images` 상태에 부합한다.

### 자가 치유 경로 (상시 동작)
1. **failed 문서 자동 재시도**: target_set에 `indexing_status: failed` 문서 항상 포함.
2. **봇 push race 복구**: push 실패 시 `last_indexed_commit` 미전진 → 다음 폴링 재처리.
3. **이미지 변경 cascade**: 이미지 파일 변경 시 참조 md 자동 포함.

일상적 drift는 파이프라인이 스스로 해결. 개입 불필요.

### 상시 복구로 커버되지 않는 drift
| 유형 | 원인 | 감지 방법 |
|---|---|---|
| 누락 인덱싱 | 과거 버그/장애로 일부 doc_id 미생성 | git HEAD의 doc_id 집합 − Milvus 집합 |
| 잔존 삭제 대상 | 삭제 시 Milvus delete 실패 영구화 | Milvus 집합 − git HEAD 집합 |
| content_hash 불일치 | 프론트매터와 적재 상태 괴리 | 문서별 비교 |
| stale 청크 잔존 | Full replace 직후 일부 실패 | doc_id별 chunk_uid 집합과 기대 집합 차이 |
| 버전 전체 불일치 | 모델/스키마 상수 변경 | `.pipeline/state.json` vs 코드 상수 |

### CLI 1: `pipeline-drift-check.py`
```
pipeline-drift-check.py [--repair] [--verbose]

1. .pipeline/state.json 로드
2. git HEAD의 모든 .md → (doc_id, content_hash, images) 집합 A
3. Milvus → DISTINCT doc_id + 각 doc_id의 chunk_uid 집합 B
4. Drift 계산:
   A - B: 누락 인덱싱
   B - A: 잔존 삭제 대상
   교집합 중:
     content_hash 불일치: 본문 재인덱싱 필요
     images 불일치: 이미지 청크 재인덱싱 필요
     chunk_uid 집합 불일치: stale 정리 필요
5. 리포트 출력
6. --repair: 공용 처리 모듈 호출해 수정. 완료 후 봇 커밋.
```

### CLI 2: `pipeline-backfill.py`
```
pipeline-backfill.py [--select <glob>] [--reason <msg>] [--dry-run]

1. 버전 상수 불일치 확인
2. shadow 컬렉션 생성: {collection}__v{epoch}
3. 대상 문서 수집 (전체 또는 --select)
4. dynamic 병렬 적재 (공용 처리 모듈)
5. --dry-run이면 여기서 종료
6. alias 교체로 원자적 전환
7. 유예 기간 후 이전 컬렉션 drop (--drop-old-after-sec)
8. 프론트매터/state.json 갱신 후 봇 커밋
```

실행 시점: `embedding_model` 교체, 청킹 전략 변경, VLM 프롬프트 대폭 변경, 프론트매터 스키마 구조 변경.

### 버전 관리
| 필드 | 의미 | 불일치 시 |
|---|---|---|
| `pipeline_schema_version` | 프론트매터 스키마 | 전체 재인덱싱 (프론트매터 변환 포함) |
| `chunk_strategy_version` | 청킹 파라미터/라이브러리 | 본문 청크 재임베딩 |
| `embedding_model` | BGE-M3 체크포인트 | 전체 재임베딩 |
| `vision_schema_version` | VLM 프롬프트/이미지 처리 규약 | 이미지 청크 재생성 |

MVP는 어느 버전이든 바뀌면 전체 Full replace로 통합 처리. 부분 백필은 `--select`로.

### 운영 체크리스트
**정기 (월 1회)**
- [ ] `pipeline-drift-check.py` 실행 후 리포트 검토
- [ ] drift ≤ 5 → 무시 (자가 치유가 해결 가능)
- [ ] drift ≥ 10 → `--repair` 실행 + 원인 분석

**모델/스키마 업그레이드**
1. 버전 상수 증가 PR 머지
2. `pipeline-backfill.py --dry-run --reason "..."` 실행
3. shadow 결과 확인 (문서/청크 수, 샘플 검색)
4. `pipeline-backfill.py --reason "..."` 정상 실행
5. `--drop-old-after-sec 86400` 24시간 유예
6. 검색단 변경 공지

**장애 복구**
- 봇 push 반복 실패 → SSH 키/권한 점검 → 자연 복구
- Milvus 장애 → 복구 후 `pipeline-drift-check.py --repair` 1회
- 모델 엔드포인트 장애 → 복구 후 indexer가 failed 재시도

### 관찰 메트릭
- `.pipeline/state.json`의 `last_indexed_commit`와 `main` HEAD 간 commit lag.
- `indexing_status: failed` 문서 수 추이.
- 봇 push retry 횟수 분포.
- 월간 drift count 추이.

---

## 11. 보안 / 시크릿 / 접근 제어

### 네트워크 경계
- 모든 구성 요소가 사내 온프레미스.
- 외부 노출 파이프라인 엔드포인트 없음 (폴링).
- 아웃바운드: Bitbucket SSH, Milvus, 모델 엔드포인트. 방화벽에서 이 연결만 허용.
- Airflow UI: 사내 SSO 뒤, 사내망 접근만.

### 시크릿 관리
| 시크릿 | 저장소 | 주입 방식 | 로테이션 |
|---|---|---|---|
| 봇 SSH private key | Vault/K8s Secret | `~/.ssh/id_ed25519` 마운트 | 연 1회 수동 |
| Bitbucket known_hosts | Vault/Secret | `~/.ssh/known_hosts` | 호스트 변경 시 |
| Milvus 인증 | Vault/Secret | Airflow Connection 또는 환경변수 | 보안 정책 주기 |
| 모델 엔드포인트 토큰 | Vault/Secret | 환경변수 | 보안 정책 주기 |

원칙:
- 코드/레포에 평문 금지. CI에서 시크릿 패턴 차단 lint 권장.
- Airflow Variable은 비민감 설정만. 시크릿은 Connection 또는 외부 시크릿 소스.
- 최소 권한: 봇 SSH 키는 대상 레포 main push + read만.

### 문서 접근 제어
현재 단일 레포 = 단일 컬렉션이므로 문서 단위 권한 개념을 다루지 않는다.

- Bitbucket 레포 read 권한이 곧 검색 권한의 경계.
- 파이프라인 인덱싱 대상은 동일 "사내 internal" 신뢰 경계에 속한다는 전제.
- 검색단이 권한 필터링을 확장하려면 레포 분리로 해결 (새 컬렉션).
- `rag_exclude`는 권한 제어 도구가 아님. 검색 노이즈 제거용. 진짜 민감 문서는 이 레포에 올리지 않아야 함.

### 데이터 취급
- 저장 위치: Bitbucket 레포, Milvus, Airflow 워커 로컬 디스크, Airflow 로그.
- 암호화: 전송 중(SSH/TLS) 기본. 디스크 암호화는 사내 인프라 정책.
- PII/민감정보 검출/마스킹은 파이프라인 범위 아님. 작성자 책임.
- 사고 시 `rag_exclude: true` PR로 즉시 검색에서 제거.

### 백업/복구
- 레포: Bitbucket 자체 백업.
- `.pipeline/state.json`: 레포 커밋이므로 자동 백업.
- Milvus: 사내 Milvus 백업 정책 따름. 재해 시 `pipeline-backfill.py`로 레포에서 재구축 가능.

### 감사 / 로그
- 봇 커밋: 저자 `docs-bot` + `[pipeline-auto]` 태그로 식별. git log가 감사 로그.
- DAG 이력: Airflow 자체 로그.
- 실패 기록: 프론트매터 `indexing_status: failed` + Airflow task 로그.
- CLI 실행: 실행자, 시각, `--reason` 로깅.

### 운영 보안 주의
- 봇 SSH 키 유출 의심: Bitbucket에서 공개키 폐기 → 새 키 페어 → Vault 갱신 → 워커 재시작.
- 봇이 의도 외 파일 수정: 섹션 8 검증 가드로 런타임 차단. 선택적으로 서버 측 훅 추가.
- Airflow 워커 shell 접근 권한 최소화 (봇 키가 컨테이너 내 접근 가능).

---

## 12. 관찰성 & 실패 처리

### 관찰 목적
1. 파이프라인이 살아있고 동작하는가 (폴링/diff 처리).
2. 인덱스 신선도가 SLO 내인가.
3. 문제가 누적되고 있지 않은가 (failed, drift, push 실패).

### 실패 분류 및 대응
| 범주 | 예시 | 분류 | 대응 |
|---|---|---|---|
| 일시적 I/O | SSH 타임아웃, Milvus 네트워크 | 재시도 가능 | task 지수 백오프 3회 → 실패 시 문서 `failed`, DAG 계속 |
| 일시적 모델 오류 | rate limit, OOM | 재시도 가능 | 동일 |
| 영구 모델 오류 | 지원하지 않는 이미지 포맷 | 재시도 불가 | 즉시 `failed`, 경고 로그 |
| 깨진 링크 | 이미지 파일 누락 | 데이터 오류 | `images` 미추가, alert |
| 사용자 규칙 위반 | 예약 필드/`.pipeline/` 편집 | CI 차단 | 머지 전 피드백 |
| 봇 push race | 사용자 PR 동시 머지 | 재시도 가능 | 지수 백오프 retry → 최종 실패 시 DAG fail + alert |
| 검증 가드 발동 | 봇 커밋에 본문 포함 | 파이프라인 버그 | DAG fail + CRITICAL, 롤백 |
| Milvus 장애 | 컬렉션 접근 불가 | 인프라 | DAG fail, 복구 후 `pipeline-drift-check.py --repair` |

### 알림 경로
- Slack `#rag-pipeline-alerts` (권장).
- 이메일: 치명적만 이중화.
- Airflow on-failure callback → Slack/메일.

### 알림 임계값 (초기)
| 이벤트 | 수준 | 임계값 |
|---|---|---|
| `indexer_dag` 단일 실패 | INFO | 1회 |
| `indexer_dag` 연속 실패 | WARNING | 3회 |
| `indexer_dag` 연속 실패 | CRITICAL | 10회 |
| 봇 push 최종 실패 | WARNING | 1회 |
| 봇 push 최종 실패 반복 | CRITICAL | 3회 연속 |
| failed 문서 수 | WARNING | 10 초과 |
| failed 문서 수 | CRITICAL | 50 초과 |
| commit lag | WARNING | 10분 초과 |
| commit lag | CRITICAL | 1시간 초과 |
| 검증 가드 발동 | CRITICAL | 1회 |

### 메트릭 카탈로그
**처리**
- `pipeline_indexer_runs_total{result}`
- `pipeline_indexer_duration_seconds` (histogram)
- `pipeline_documents_processed_total{case}`
- `pipeline_chunks_written_total{chunk_kind}`
- `pipeline_chunks_deleted_total`

**상태 (gauge)**
- `pipeline_indexing_status_failed_count`
- `pipeline_commit_lag_seconds`
- `pipeline_collection_doc_count`
- `pipeline_collection_chunk_count`

**Git/push**
- `pipeline_bot_push_attempts_total{result}`
- `pipeline_bot_push_retry_count` (histogram)

**모델 호출**
- `pipeline_embedding_requests_total` / `_duration_seconds`
- `pipeline_llm_requests_total` / `_duration_seconds`
- `pipeline_vlm_requests_total` / `_duration_seconds` / `_cache_hit_ratio`

**CLI**
- `pipeline_drift_check_drift_items` (gauge)
- `pipeline_backfill_runs_total{result}`

### 대시보드 권장 구성
**Overview**: commit lag, indexer 성공/실패(24h), failed 문서 수(현재), 컬렉션 크기 추이.
**Detail**: case별 처리 분포, 모델 호출 지연 p50/p95/p99, push retry 분포, VLM 캐시 히트율.
**Alerts**: 활성 알림, 24시간 실패 타임라인.

### 로그
- Airflow task 로그: 사내 로그 집계 시스템. 최소 30일 retention.
- 애플리케이션 로그: 구조화 JSON (`doc_id`, `path`, `case`, `duration_ms`, `error_type`, `error_message`).
- 봇 git 커맨드 stderr/stdout 포함.
- 개인정보 취급: 본문 미포함, 경로/doc_id/메타만.

### SLO 위반 감지
`pipeline_commit_lag_seconds` 기반 자동 알림. 위반 잦으면 폴링 주기 단축 또는 모델 병렬성 확대 검토.

### 플레이북 요약
| 징후 | 1차 조치 | 2차 조치 |
|---|---|---|
| commit lag 증가 | indexer 로그 확인 | 모델 엔드포인트/폴링 주기 점검 |
| failed 급증 | 에러 분포 확인 | 모델 엔드포인트 복구 |
| 봇 push 반복 실패 | 계정 권한 확인 | SSH 키 교체, drift-check repair |
| 검증 가드 발동 | 즉시 롤백 | 코드 원인 분석, 핫픽스 |
| drift 다수 | `--repair` | 근본 수정 |

---

## 13. 성공 기준 & SLO

### 성공 기준 정의
1. **신선도(Freshness)**: 사용자 PR 머지 후 합리적 시간 내 Milvus에서 검색 가능.
2. **일관성(Consistency)**: `main` HEAD와 Milvus 상태 수렴.
3. **탄력성(Resilience)**: 개별 문서 실패, 엔드포인트 일시 장애, push race가 서비스를 멈추지 않음.

### SLO

**신선도**
| 항목 | 목표 |
|---|---|
| push → 반영 p50 | ≤ 5분 |
| push → 반영 p95 | ≤ 15분 |
| push → 반영 p99 | ≤ 30분 |

측정: PR 머지 커밋 시각 → 해당 문서 인덱싱 완료 시각.

**일관성**
| 항목 | 목표 |
|---|---|
| 24시간 이내 자동 수렴 | ≥ 99% |
| 월간 수동 drift-check --repair 횟수 | ≤ 1 |
| `indexing_status: failed` 평균 체류 시간 | ≤ 2시간 |

**탄력성**
| 항목 | 목표 |
|---|---|
| 개별 실패가 sibling 차단 | 0 |
| 5분 이하 엔드포인트 장애 자가 복구 | 100% |
| 봇 push race 5분 이내 해소 | ≥ 99% |

### 처리 용량 / 성능 기준
부하 가정:
- 문서 수천 개(≤ 5,000).
- 일간 평균 PR 머지: ≤ 20건, 변경 문서 ≤ 50건.
- 평균 문서당 청크: 10 (본문 8 + 이미지 2).
- 평균 이미지: 2/문서.

목표:
- `indexer_dag` diff 50건 처리: ≤ 5분.
- VLM 평균 지연: ≤ 3초/이미지 (캐시 제외).
- BGE-M3 throughput: ≥ 100 청크/초 (배치).
- 전체 백필(5,000 문서): ≤ 4시간.

실측으로 교정.

### 검색 품질 기반 데이터 기준
| 기준 | 내용 |
|---|---|
| 누락 없음 | `rag_exclude=true` 외 모든 문서 인덱싱 (drift 99% 수렴) |
| 청크 중복 없음 | 동일 doc_id 내 중복 chunk_uid 0건 |
| 메타 정합성 | 각 청크의 `doc_id`/`path`/`source_url`이 git 상태와 일치 |
| 캡션 커버리지 | 본문 이미지 참조의 ≥ 98%가 `images` 필드에 존재 |
| 청크 크기 안정성 | 평균 청크 토큰 수가 `chunk_size`의 ±30% 내 |

### 측정 방법
- 신선도: Airflow task가 (커밋 시각, 반영 시각) 메트릭 기록. Grafana 등에서 p50/p95/p99 집계.
- 일관성: `pipeline_indexing_status_failed_count` + 월간 drift 리포트.
- 탄력성: DAG fail 빈도, push 재시도 메트릭.
- 처리 용량: `pipeline_indexer_duration_seconds`, `pipeline_documents_processed_total`.
- 데이터 품질: 월간 샘플링 감사 (랜덤 100 문서의 프론트매터 ↔ Milvus 대조).

### SLO 위반 시 대응
- 신선도 위반: 실행 시간 분포 분석 → 엔드포인트 지연 확인 → 병렬도/폴링 주기 조정.
- 일관성 위반: drift 유형 분석, 반복 유형은 파이프라인 버그 수정.
- 탄력성 위반: Airflow `trigger_rule` 설정 점검, DAG regression 확인.

### 초기 베이스라인
첫 2-4주는 SLO를 "지켜야 할 계약"이 아닌 "베이스라인 측정"으로 취급. 실측 수집 후 공식 SLO로 승격.

---

## 14. 미해결 이슈 / 후속 결정

### 구축 착수 전 확정 필요 (조직 외부 조율)
| 항목 | 결정 주체 | 방법 |
|---|---|---|
| 대상 Bitbucket 레포 | 문서 소유 팀 | 레포 이름, SSH URL, 웹 URL 베이스 |
| 봇 계정 이름/이메일 (docs-bot 제안) | 운영 팀 | Bitbucket 계정 생성 |
| BGE-M3/LLM/VLM 서빙 엔드포인트 | 사내 ML 팀 | URL, 체크포인트, 동시성 상한 |
| LLM/VLM 모델 선정 | ML + 운영 | VLM 후보: Qwen2-VL, InternVL, MiniCPM-V. LLM: EXAONE, SOLAR, Qwen |
| Milvus 네임스페이스/컬렉션 이름 | 운영 팀 | 설정 파일 |
| Slack 채널/이메일 | 운영 팀 | `#rag-pipeline-alerts` |
| 시크릿 저장소 | 보안 팀 | Vault vs K8s Secret 표준 |
| Airflow 배포 환경 | 데이터 플랫폼 팀 | 기존 클러스터 활용 여부 |

### 구축 중 결정 (실측 기반)
| 항목 | 초기안 | 조정 계기 |
|---|---|---|
| 폴링 주기 | 3분 | 신선도 SLO 초과/부하 과다 |
| `max_map_length` | 4 | GPU 부하 튜닝 |
| 청크 파라미터 (1024/128) | 제안값 | 검색 품질 피드백 |
| HNSW `M`/`efC` | 24/200 | 재현율/지연 측정 |
| `push_max_retries` | 10 | race 빈도 실측 |
| VLM 캡션 길이 | 80-500 | 품질/분포 관찰 |
| 알림 임계값 | 초기값 | 2-4주 운영 후 재조정 |

### 운영 초기 베이스라인 (첫 2-4주)
- SLO 베이스라인 측정 후 공식 승격.
- `pipeline_commit_lag_seconds` 분포로 폴링 주기 적절성 판단.
- VLM 캐시 히트율로 캐시 정책 개선 필요성 판단.
- drift 유형 분포로 예상 외 패턴 발견.

### 의도적으로 닫아둔 확장
- 다중 레포 지원.
- GitHub/GitLab 등 다른 Git 호스팅.
- 문서 단위 권한 필터링.
- PDF/Office 등 첨부 처리.
- 이미지 청크 부분 업데이트 최적화.
- Late chunking / Parent-Child 청킹.
- 다국어 확장.
- 실시간 webhook 경로 복원.

### 알려진 한계 / 리스크
| 항목 | 내용 | 완화 |
|---|---|---|
| VLM 품질 의존 | 캡션 정확도가 검색 품질에 영향 | 모델 예제 기반 평가, 프롬프트 버전 관리 |
| 폴링 지연 체감 | 변경 직후 검색 시 미반영 가능 | 주기 공지, UI에서 안내 |
| 봇 프론트매터 커밋 볼륨 | 변경 많을수록 빈도 증가 | 본문 불가침이라 blame 노이즈 낮음 |
| 대량 리팩토링 PR | 처리 시간 증가 | 병렬도 증가, 진행률 알림 |
| LLM 요약 품질 편차 | summary/doc_type/topics 정확도 편차 | 초기 샘플링 감사, 모델 교체 |
| 동시 수정 race | 중간 버전 인덱싱 가능 | 최종 일관성 모델 수용 |
| 이미지 동일 이름 오인 | basename grep 한계 | CI 린터 강제 |

### 후속 문서
- `docs/pipeline-reserved-fields.md` — 예약 필드 가이드.
- `docs/pipeline-runbook.md` — 운영 플레이북.
- `prompts/vlm-caption.md` — VLM 프롬프트 (버전 관리).
- `prompts/llm-summary.md`, `prompts/llm-classify.md` — LLM 프롬프트.
- `docs/ci-rules.md` — CI lint 상세.
- `docs/search-contract.md` — 검색단과의 계약 확장.
