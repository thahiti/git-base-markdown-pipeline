# Plan 1: Indexer MVP 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bitbucket 단일 레포의 마크다운 문서를 Milvus에 인덱싱하는 파이프라인 MVP 구축. Added/Modified/Deleted + Exclude 전환 + Failed 재시도 + 이미지 cascade를 포함하는 `indexer_dag`가 수 분 주기로 동작한다.

**Architecture:** Python 패키지 `markdown_rag_pipeline`을 코어 라이브러리로 두고, 외부 시스템(Git, Milvus, BGE-M3, LLM, VLM)은 각각 어댑터/클라이언트 클래스로 분리해 테스트 가능하게 한다. Airflow `indexer_dag`는 공용 처리 모듈을 호출하는 얇은 래퍼. 코어 라이브러리는 추후 Plan 3의 CLI도구(drift-check, backfill)에서 재사용.

**Tech Stack:** Python 3.11+, langchain-text-splitters, pymilvus 2.5, GitPython/subprocess, pydantic, PyYAML, httpx, python-frontmatter, pytest, pytest-asyncio, Airflow 2.x

---

## File Structure

```
pyproject.toml                       # 패키지 메타/의존성
README.md                            # 레포 README

pipeline-config.sample.yaml          # 설정 예시

prompts/
├── vlm-caption.md                   # VLM 프롬프트 (버전 1)
├── llm-summary.md                   # LLM 요약 프롬프트
└── llm-classify.md                  # LLM 분류/토픽/언어 프롬프트

src/markdown_rag_pipeline/
├── __init__.py
├── config.py                        # 설정 모델 + 로더 (pydantic)
├── models.py                        # 공용 데이터 클래스 (Document, Chunk 등)
├── frontmatter.py                   # 프론트매터 read/write/예약 필드
├── state.py                         # .pipeline/state.json 로더/저장자
├── git_adapter.py                   # subprocess git 래퍼
├── bitbucket_url.py                 # Bitbucket 웹 URL 생성
├── chunking.py                      # LangChain 래퍼
├── image_ref.py                     # 본문 이미지 참조 수집, 조상 grep
├── clients/
│   ├── __init__.py
│   ├── embedding.py                 # BGE-M3 HTTP 클라이언트
│   ├── llm.py                       # LLM HTTP 클라이언트
│   └── vlm.py                       # VLM HTTP 클라이언트
├── milvus_adapter.py                # Milvus 스키마/업서트/삭제/stale
├── processing.py                    # 문서 케이스 처리 흐름
├── bot_commit.py                    # 봇 git 전략 (rebase -X ours, push retry)
└── orchestrator.py                  # target_set 계산, 실행 진입점

dags/
└── indexer_dag.py                   # Airflow DAG (얇은 래퍼)

tests/
├── conftest.py                      # 공용 픽스처
├── fixtures/
│   ├── sample_markdown/             # 다양한 .md 샘플
│   ├── fake_bge_m3.py               # Mock BGE-M3 서버
│   ├── fake_llm.py                  # Mock LLM 서버
│   └── fake_vlm.py                  # Mock VLM 서버
├── unit/
│   ├── test_config.py
│   ├── test_frontmatter.py
│   ├── test_state.py
│   ├── test_bitbucket_url.py
│   ├── test_chunking.py
│   ├── test_image_ref.py
│   ├── test_clients.py
│   ├── test_bot_commit.py
│   └── test_processing.py
└── integration/
    ├── test_git_adapter.py          # 로컬 임시 레포
    ├── test_milvus_adapter.py       # Milvus testcontainer
    ├── test_orchestrator.py         # 전체 흐름
    └── test_indexer_dag.py          # DAG 단일 실행
```

## 전체 태스크 (22개)

1. 프로젝트 스켈레톤 및 pyproject 설정
2. 공용 데이터 모델 (`models.py`)
3. 설정 모델/로더 (`config.py`)
4. 프론트매터 모듈 (`frontmatter.py`)
5. 상태 파일 모듈 (`state.py`)
6. Bitbucket 웹 URL 빌더 (`bitbucket_url.py`)
7. Git 어댑터 (`git_adapter.py`)
8. 청킹 래퍼 (`chunking.py`)
9. 이미지 참조 추출 (`image_ref.py` — 본문 파싱 부분)
10. 이미지 조상 grep (`image_ref.py` — cascade 부분)
11. BGE-M3 클라이언트 (`clients/embedding.py`)
12. LLM 클라이언트 (`clients/llm.py`)
13. VLM 클라이언트 (`clients/vlm.py`)
14. Milvus 어댑터 스키마 (`milvus_adapter.py` — 생성/연결)
15. Milvus 어댑터 업서트/삭제/stale
16. 봇 Git 전략 (`bot_commit.py`)
17. 문서 처리 — Case 1 Added (`processing.py`)
18. 문서 처리 — Case 2 Modified + images 동기화
19. 문서 처리 — Case 3 Deleted + Exclude 전환
20. 오케스트레이터 (`orchestrator.py`) — target_set, Failed 재시도 통합
21. Airflow DAG (`dags/indexer_dag.py`)
22. 엔드투엔드 통합 테스트

---

## Task 1: 프로젝트 스켈레톤 및 pyproject 설정

**Files:**
- Create: `pyproject.toml`
- Create: `src/markdown_rag_pipeline/__init__.py`
- Create: `src/markdown_rag_pipeline/clients/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`
- Create: `tests/integration/__init__.py`
- Create: `tests/conftest.py`
- Create: `README.md`

- [ ] **Step 1: `pyproject.toml` 작성**

```toml
[project]
name = "markdown-rag-pipeline"
version = "0.1.0"
description = "Bitbucket 마크다운 문서를 Milvus에 인덱싱하는 파이프라인"
requires-python = ">=3.11"
dependencies = [
    "pydantic>=2.5",
    "PyYAML>=6.0",
    "python-frontmatter>=1.1",
    "httpx>=0.27",
    "langchain-text-splitters>=0.2",
    "pymilvus>=2.5",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.23",
    "pytest-httpx>=0.30",
    "ruff>=0.3",
]
airflow = [
    "apache-airflow>=2.8",
]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"

[tool.ruff]
line-length = 100
target-version = "py311"
```

- [ ] **Step 2: 패키지 `__init__.py` 생성**

`src/markdown_rag_pipeline/__init__.py`:
```python
"""Bitbucket 마크다운 RAG 인덱싱 파이프라인."""

__version__ = "0.1.0"
```

`src/markdown_rag_pipeline/clients/__init__.py`:
```python
"""외부 서비스 클라이언트 (BGE-M3, LLM, VLM)."""
```

`tests/__init__.py`, `tests/unit/__init__.py`, `tests/integration/__init__.py`는 빈 파일.

- [ ] **Step 3: `tests/conftest.py` 공용 픽스처**

```python
"""공용 pytest 픽스처."""
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_workdir(tmp_path: Path) -> Path:
    """테스트용 임시 작업 디렉토리."""
    return tmp_path
```

- [ ] **Step 4: `README.md` 작성**

```markdown
# markdown-rag-pipeline

Bitbucket 레포의 마크다운 문서를 Milvus에 인덱싱하는 파이프라인.

## 설계 문서

`docs/superpowers/specs/2026-04-19-git-markdown-rag-pipeline-design.md`

## 구현 계획

`docs/superpowers/plans/` 디렉토리.

## 개발 환경

```bash
pip install -e ".[dev]"
pytest
```
```

- [ ] **Step 5: 의존성 설치 및 빈 테스트 실행**

```bash
pip install -e ".[dev]"
pytest
```
Expected: "no tests ran" (OK 종료 상태)

- [ ] **Step 6: 커밋**

```bash
git add pyproject.toml src/ tests/ README.md
git commit -m "chore: initialize python package skeleton"
```

---

## Task 2: 공용 데이터 모델 (`models.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/models.py`
- Create: `tests/unit/test_models.py`

모든 케이스 처리에서 사용할 자료형을 먼저 정의한다. `Document`, `Chunk`, `ImageRef`, `DiffEntry`, `DiffCase` enum.

- [ ] **Step 1: 실패하는 테스트 작성** (`tests/unit/test_models.py`)

```python
from markdown_rag_pipeline.models import Chunk, ChunkKind, DiffCase, DiffEntry, ImageRef


def test_chunk_uid_body():
    c = Chunk(
        doc_id="550e8400-e29b-41d4-a716-446655440000",
        chunk_kind=ChunkKind.BODY,
        chunk_index=3,
        chunk_text="hello",
        breadcrumb="Doc > Section",
        line_start=10,
        line_end=15,
    )
    assert c.chunk_uid == "550e8400-e29b-41d4-a716-446655440000:body:3"


def test_chunk_uid_image():
    c = Chunk(
        doc_id="550e8400-e29b-41d4-a716-446655440000",
        chunk_kind=ChunkKind.IMAGE,
        chunk_index=0,
        chunk_text="section\n\ncaption",
        breadcrumb="Doc > Section",
        line_start=0,
        line_end=0,
    )
    assert c.chunk_uid == "550e8400-e29b-41d4-a716-446655440000:image:0"


def test_image_ref_equal_ignores_caption():
    a = ImageRef(path="images/a.png", section_path="S", caption="v1")
    b = ImageRef(path="images/a.png", section_path="S", caption="v2")
    assert a.path_eq(b)


def test_diff_entry_case_from_git_status():
    assert DiffEntry.from_git("A", "doc.md").case == DiffCase.ADDED
    assert DiffEntry.from_git("M", "doc.md").case == DiffCase.MODIFIED
    assert DiffEntry.from_git("D", "doc.md").case == DiffCase.DELETED
    r = DiffEntry.from_git_rename("R100", "old.md", "new.md")
    assert r.case == DiffCase.RENAMED
    assert r.old_path == "old.md"
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

```bash
pytest tests/unit/test_models.py -v
```
Expected: ImportError — `models` 모듈 없음.

- [ ] **Step 3: `models.py` 구현**

```python
"""공용 데이터 모델."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class ChunkKind(str, Enum):
    BODY = "body"
    IMAGE = "image"


class DiffCase(str, Enum):
    ADDED = "added"
    MODIFIED = "modified"
    DELETED = "deleted"
    RENAMED = "renamed"


@dataclass
class ImageRef:
    """본문에서 참조된 이미지 하나."""
    path: str           # 레포 루트 기준 상대 경로
    section_path: str   # heading 경로 (e.g. "Doc > Section")
    caption: Optional[str] = None  # VLM 캡션. 프론트매터에 저장된 경우 채워짐.

    def path_eq(self, other: "ImageRef") -> bool:
        return self.path == other.path


@dataclass
class Chunk:
    """Milvus에 적재될 청크 한 개."""
    doc_id: str
    chunk_kind: ChunkKind
    chunk_index: int
    chunk_text: str
    breadcrumb: str
    line_start: int
    line_end: int
    dense_vector: Optional[list[float]] = None
    sparse_vector: Optional[dict[int, float]] = None
    # 문서 메타 (flatten — Milvus scalar에 직접 들어감)
    path: str = ""
    title: str = ""
    doc_type: str = ""
    topics: list[str] = field(default_factory=list)
    language: str = ""
    authors: list[str] = field(default_factory=list)
    created_at_ms: int = 0
    updated_at_ms: int = 0
    last_indexed_at_ms: int = 0
    has_images: bool = False
    source_url: str = ""
    user_meta: dict = field(default_factory=dict)

    @property
    def chunk_uid(self) -> str:
        return f"{self.doc_id}:{self.chunk_kind.value}:{self.chunk_index}"


@dataclass
class DiffEntry:
    case: DiffCase
    path: str
    old_path: Optional[str] = None  # RENAMED 때 사용

    @classmethod
    def from_git(cls, status: str, path: str) -> "DiffEntry":
        mapping = {"A": DiffCase.ADDED, "M": DiffCase.MODIFIED, "D": DiffCase.DELETED}
        return cls(case=mapping[status], path=path)

    @classmethod
    def from_git_rename(cls, status: str, old_path: str, new_path: str) -> "DiffEntry":
        # status 예: R100, R080 등
        return cls(case=DiffCase.RENAMED, path=new_path, old_path=old_path)
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

```bash
pytest tests/unit/test_models.py -v
```
Expected: 4 passed.

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/models.py tests/unit/test_models.py
git commit -m "feat(models): add core data models (Chunk, ImageRef, DiffEntry)"
```

---

## Task 3: 설정 모델/로더 (`config.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/config.py`
- Create: `pipeline-config.sample.yaml`
- Create: `tests/unit/test_config.py`

설계 섹션 6의 설정 스키마를 pydantic 모델로 구현.

- [ ] **Step 1: 샘플 설정 파일 작성** (`pipeline-config.sample.yaml`)

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
```

- [ ] **Step 2: 실패 테스트 작성** (`tests/unit/test_config.py`)

```python
from pathlib import Path

import pytest

from markdown_rag_pipeline.config import load_config, PipelineConfig


def test_load_sample_config():
    cfg = load_config(Path("pipeline-config.sample.yaml"))
    assert isinstance(cfg, PipelineConfig)
    assert cfg.milvus.collection_name == "docs"
    assert cfg.embedding.dense_dim == 1024
    assert cfg.chunking.chunk_size == 1024
    assert cfg.bot.push_max_retries == 10


def test_collection_name_required(tmp_path: Path):
    cfg_file = tmp_path / "c.yaml"
    cfg_file.write_text(
        """
repo: {ssh_url: a, branch: main, local_workdir: /w}
bitbucket: {web_url_base: http://x}
milvus: {host: h, port: 1, collection_name: ""}
embedding: {model_name: x, endpoint: http://x, dense_dim: 1}
llm: {endpoint: http://x, model_name: x}
vision:
  caption_endpoint: http://x
  caption_model: x
  prompt_path: p
  prompt_version: 1
  caption_min_chars: 1
  caption_max_chars: 2
  cache_by_image_hash: true
chunking:
  headers_to_split_on: [["#", "H1"]]
  chunk_size: 10
  chunk_overlap: 1
  strategy_version: 1
schedule: {indexer: "*/3 * * * *"}
bot:
  git_author_name: x
  git_author_email: x
  commit_tag: t
  push_max_retries: 1
  push_backoff_initial_sec: 1
  push_backoff_max_sec: 2
  push_backoff_jitter: 0.1
"""
    )
    with pytest.raises(ValueError, match="collection_name"):
        load_config(cfg_file)
```

- [ ] **Step 3: 테스트 실행 — 실패 확인**

```bash
pytest tests/unit/test_config.py -v
```
Expected: ImportError.

- [ ] **Step 4: `config.py` 구현**

```python
"""파이프라인 설정 로더."""
from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator


class RepoCfg(BaseModel):
    ssh_url: str
    branch: str = "main"
    local_workdir: str


class BitbucketCfg(BaseModel):
    web_url_base: str


class MilvusCfg(BaseModel):
    host: str
    port: int
    collection_name: str

    @field_validator("collection_name")
    @classmethod
    def _not_empty(cls, v: str) -> str:
        if not v:
            raise ValueError("collection_name must not be empty")
        return v


class EmbeddingCfg(BaseModel):
    model_name: str
    endpoint: str
    dense_dim: int


class LLMCfg(BaseModel):
    endpoint: str
    model_name: str


class VisionCfg(BaseModel):
    caption_endpoint: str
    caption_model: str
    prompt_path: str
    prompt_version: int
    caption_min_chars: int
    caption_max_chars: int
    cache_by_image_hash: bool


class ChunkingCfg(BaseModel):
    headers_to_split_on: list[list[str]]
    chunk_size: int
    chunk_overlap: int
    strategy_version: int


class ScheduleCfg(BaseModel):
    indexer: str


class BotCfg(BaseModel):
    git_author_name: str
    git_author_email: str
    commit_tag: str
    push_max_retries: int
    push_backoff_initial_sec: int
    push_backoff_max_sec: int
    push_backoff_jitter: float


class PipelineConfig(BaseModel):
    repo: RepoCfg
    bitbucket: BitbucketCfg
    milvus: MilvusCfg
    embedding: EmbeddingCfg
    llm: LLMCfg
    vision: VisionCfg
    chunking: ChunkingCfg
    schedule: ScheduleCfg
    bot: BotCfg


def load_config(path: Path) -> PipelineConfig:
    data = yaml.safe_load(path.read_text())
    return PipelineConfig.model_validate(data)
```

- [ ] **Step 5: 테스트 실행 — 통과 확인**

```bash
pytest tests/unit/test_config.py -v
```
Expected: 2 passed.

- [ ] **Step 6: 커밋**

```bash
git add src/markdown_rag_pipeline/config.py pipeline-config.sample.yaml tests/unit/test_config.py
git commit -m "feat(config): add pipeline config schema and loader"
```

---

## Task 4: 프론트매터 모듈 (`frontmatter.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/frontmatter.py`
- Create: `tests/unit/test_frontmatter.py`
- Create: `tests/fixtures/sample_markdown/added_new.md`
- Create: `tests/fixtures/sample_markdown/indexed_full.md`

프론트매터는 예약 필드와 사용자 필드를 분리 관리한다. `python-frontmatter`로 파싱하고 커스텀 직렬화로 예약/사용자 영역을 구분 기록.

- [ ] **Step 1: 샘플 픽스처 — `added_new.md`**

```markdown
# 신규 문서

이것은 프론트매터 없이 처음 커밋된 문서 본문이다.
```

- [ ] **Step 2: 샘플 픽스처 — `indexed_full.md`**

```markdown
---
doc_id: 550e8400-e29b-41d4-a716-446655440000
path: docs/test.md
content_hash: "sha256:abc"
last_indexed_commit: a1b2c3d
last_indexed_at: 2026-04-19T12:00:00Z
pipeline_schema_version: 1
chunk_strategy_version: 1
embedding_model: bge-m3@20260301
vision_schema_version: 1
title: 테스트 문서
summary: 프론트매터 파싱 검증용
doc_type: reference
topics: [test]
language: ko
authors: [alice]
created_at: 2026-04-01T00:00:00Z
updated_at: 2026-04-10T00:00:00Z
chunk_count: 3
has_images: false
indexing_status: ok
web_url: https://bitbucket.org/x/y/src/main/docs/test.md
images: []
owner_team: sre
review_status: approved
---

# 테스트 문서

본문 내용.
```

- [ ] **Step 3: 실패 테스트 작성** (`tests/unit/test_frontmatter.py`)

```python
from pathlib import Path

from markdown_rag_pipeline.frontmatter import (
    RESERVED_KEYS,
    parse_file,
    is_rag_excluded,
    separate_reserved_user,
    write_file,
    Reserved,
)

FIXTURES = Path("tests/fixtures/sample_markdown")


def test_parse_indexed_full():
    doc = parse_file(FIXTURES / "indexed_full.md")
    assert doc.reserved.doc_id == "550e8400-e29b-41d4-a716-446655440000"
    assert doc.reserved.indexing_status == "ok"
    assert doc.user_meta == {"owner_team": "sre", "review_status": "approved"}
    assert "# 테스트 문서" in doc.body


def test_parse_added_new_has_no_frontmatter():
    doc = parse_file(FIXTURES / "added_new.md")
    assert doc.reserved.doc_id is None
    assert doc.user_meta == {}
    assert "# 신규 문서" in doc.body


def test_separate_reserved_user():
    raw = {"doc_id": "x", "title": "t", "owner_team": "sre", "rag_exclude": False}
    reserved, user = separate_reserved_user(raw)
    assert reserved["doc_id"] == "x"
    assert user == {"owner_team": "sre", "rag_exclude": False}
    assert set(RESERVED_KEYS) >= {"doc_id", "title"}


def test_is_rag_excluded_true():
    raw = {"rag_exclude": True}
    assert is_rag_excluded(raw)


def test_write_round_trip(tmp_path: Path):
    src = FIXTURES / "indexed_full.md"
    doc = parse_file(src)
    dst = tmp_path / "out.md"
    write_file(dst, doc)
    roundtrip = parse_file(dst)
    assert roundtrip.reserved.doc_id == doc.reserved.doc_id
    assert roundtrip.user_meta == doc.user_meta
    assert roundtrip.body == doc.body
```

- [ ] **Step 4: 테스트 실행 — 실패 확인**

```bash
pytest tests/unit/test_frontmatter.py -v
```
Expected: ImportError.

- [ ] **Step 5: `frontmatter.py` 구현**

```python
"""프론트매터 read/write + 예약 필드 모델."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Optional

import frontmatter as fm
import yaml


# 예약 필드 목록 — 파이프라인이 관리
RESERVED_KEYS = (
    "doc_id",
    "path",
    "content_hash",
    "last_indexed_commit",
    "last_indexed_at",
    "pipeline_schema_version",
    "chunk_strategy_version",
    "embedding_model",
    "vision_schema_version",
    "title",
    "summary",
    "doc_type",
    "topics",
    "language",
    "authors",
    "created_at",
    "updated_at",
    "chunk_count",
    "has_images",
    "indexing_status",
    "web_url",
    "images",
)


@dataclass
class ImageEntry:
    """프론트매터 images 배열의 항목."""
    path: str
    caption: str
    section_path: str


@dataclass
class Reserved:
    doc_id: Optional[str] = None
    path: Optional[str] = None
    content_hash: Optional[str] = None
    last_indexed_commit: Optional[str] = None
    last_indexed_at: Optional[str] = None
    pipeline_schema_version: Optional[int] = None
    chunk_strategy_version: Optional[int] = None
    embedding_model: Optional[str] = None
    vision_schema_version: Optional[int] = None
    title: Optional[str] = None
    summary: Optional[str] = None
    doc_type: Optional[str] = None
    topics: list[str] = field(default_factory=list)
    language: Optional[str] = None
    authors: list[str] = field(default_factory=list)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    chunk_count: Optional[int] = None
    has_images: Optional[bool] = None
    indexing_status: Optional[str] = None
    web_url: Optional[str] = None
    images: list[ImageEntry] = field(default_factory=list)


@dataclass
class ParsedDoc:
    reserved: Reserved
    user_meta: dict[str, Any]
    body: str


def separate_reserved_user(raw: dict[str, Any]) -> tuple[dict, dict]:
    reserved = {k: v for k, v in raw.items() if k in RESERVED_KEYS}
    user = {k: v for k, v in raw.items() if k not in RESERVED_KEYS}
    return reserved, user


def is_rag_excluded(raw: dict[str, Any]) -> bool:
    return bool(raw.get("rag_exclude", False))


def parse_file(path: Path) -> ParsedDoc:
    post = fm.load(str(path))
    raw = dict(post.metadata)
    reserved_dict, user = separate_reserved_user(raw)

    images = [
        ImageEntry(
            path=i["path"],
            caption=i.get("caption", ""),
            section_path=i.get("section_path", ""),
        )
        for i in (reserved_dict.pop("images", None) or [])
    ]
    reserved = Reserved(**reserved_dict, images=images)

    return ParsedDoc(reserved=reserved, user_meta=user, body=post.content)


def write_file(path: Path, doc: ParsedDoc) -> None:
    reserved_dict: dict[str, Any] = {
        k: v for k, v in asdict(doc.reserved).items() if v is not None and v != []
    }
    if doc.reserved.images:
        reserved_dict["images"] = [asdict(i) for i in doc.reserved.images]

    merged = {**reserved_dict, **doc.user_meta}

    post = fm.Post(doc.body, **merged)
    path.write_text(
        fm.dumps(post, sort_keys=False, allow_unicode=True, default_flow_style=False)
    )
```

- [ ] **Step 6: 테스트 실행 — 통과 확인**

```bash
pytest tests/unit/test_frontmatter.py -v
```
Expected: 5 passed.

- [ ] **Step 7: 커밋**

```bash
git add src/markdown_rag_pipeline/frontmatter.py tests/unit/test_frontmatter.py tests/fixtures/
git commit -m "feat(frontmatter): add parse/write with reserved field separation"
```

---

## Task 5: 상태 파일 모듈 (`state.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/state.py`
- Create: `tests/unit/test_state.py`

`.pipeline/state.json` 읽기/쓰기. 파일 없거나 손상 시 "최초 실행" 모드로 `None` 반환.

- [ ] **Step 1: 실패 테스트 작성** (`tests/unit/test_state.py`)

```python
import json
from pathlib import Path

from markdown_rag_pipeline.state import PipelineState, read_state, write_state


def test_read_missing_returns_none(tmp_path: Path):
    assert read_state(tmp_path) is None


def test_read_corrupt_returns_none(tmp_path: Path):
    (tmp_path / ".pipeline").mkdir()
    (tmp_path / ".pipeline" / "state.json").write_text("not json")
    assert read_state(tmp_path) is None


def test_write_then_read(tmp_path: Path):
    state = PipelineState(
        schema_version=1,
        last_indexed_commit="a1b2c3d",
        last_indexed_at="2026-04-19T12:00:00Z",
        pipeline_schema_version=1,
        chunk_strategy_version=1,
        embedding_model="bge-m3@20260301",
        vision_schema_version=1,
    )
    write_state(tmp_path, state)

    loaded = read_state(tmp_path)
    assert loaded is not None
    assert loaded.last_indexed_commit == "a1b2c3d"
    assert loaded.embedding_model == "bge-m3@20260301"
    assert (tmp_path / ".pipeline" / "state.json").exists()


def test_write_creates_readme(tmp_path: Path):
    state = PipelineState(
        schema_version=1,
        last_indexed_commit="a",
        last_indexed_at="2026-04-19T12:00:00Z",
        pipeline_schema_version=1,
        chunk_strategy_version=1,
        embedding_model="x",
        vision_schema_version=1,
    )
    write_state(tmp_path, state)
    readme = (tmp_path / ".pipeline" / "README.md").read_text()
    assert "수동 편집 금지" in readme
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

```bash
pytest tests/unit/test_state.py -v
```
Expected: ImportError.

- [ ] **Step 3: `state.py` 구현**

```python
"""레포 내 .pipeline/state.json 읽기/쓰기."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional

STATE_DIR = ".pipeline"
STATE_FILE = "state.json"
README_FILE = "README.md"
README_CONTENT = """# .pipeline/

이 디렉토리는 파이프라인 전용 상태 저장소입니다. 수동 편집 금지.

- `state.json`: 마지막 인덱싱 commit과 버전 메타. indexer_dag가 관리합니다.
"""


@dataclass
class PipelineState:
    schema_version: int
    last_indexed_commit: str
    last_indexed_at: str
    pipeline_schema_version: int
    chunk_strategy_version: int
    embedding_model: str
    vision_schema_version: int


def _state_path(repo_root: Path) -> Path:
    return repo_root / STATE_DIR / STATE_FILE


def read_state(repo_root: Path) -> Optional[PipelineState]:
    p = _state_path(repo_root)
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text())
        return PipelineState(**data)
    except (json.JSONDecodeError, TypeError, KeyError):
        return None


def write_state(repo_root: Path, state: PipelineState) -> None:
    directory = repo_root / STATE_DIR
    directory.mkdir(parents=True, exist_ok=True)
    _state_path(repo_root).write_text(json.dumps(asdict(state), indent=2, ensure_ascii=False))
    readme = directory / README_FILE
    if not readme.exists():
        readme.write_text(README_CONTENT)
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

```bash
pytest tests/unit/test_state.py -v
```
Expected: 4 passed.

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/state.py tests/unit/test_state.py
git commit -m "feat(state): add .pipeline/state.json read/write"
```

---

## Task 6: Bitbucket 웹 URL 빌더 (`bitbucket_url.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/bitbucket_url.py`
- Create: `tests/unit/test_bitbucket_url.py`

- [ ] **Step 1: 실패 테스트**

```python
from markdown_rag_pipeline.bitbucket_url import build_file_url, build_image_url


def test_file_url_without_lines():
    url = build_file_url(
        base="https://bitbucket.org/ops/runbooks",
        branch="main",
        path="runbooks/k8s.md",
    )
    assert url == "https://bitbucket.org/ops/runbooks/src/main/runbooks/k8s.md"


def test_file_url_with_lines():
    url = build_file_url(
        base="https://bitbucket.org/ops/runbooks",
        branch="main",
        path="runbooks/k8s.md",
        line_start=10,
        line_end=25,
    )
    assert url.endswith("#lines-10:25")


def test_file_url_encodes_korean():
    url = build_file_url(
        base="https://bitbucket.org/ops/runbooks",
        branch="main",
        path="runbooks/장애대응.md",
    )
    assert "%EC%9E%A5%EC%95%A0%EB%8C%80%EC%9D%91" in url


def test_image_url_no_fragment():
    url = build_image_url(
        base="https://bitbucket.org/ops/runbooks",
        branch="main",
        path="runbooks/images/arch.png",
    )
    assert url == "https://bitbucket.org/ops/runbooks/src/main/runbooks/images/arch.png"
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `bitbucket_url.py` 구현**

```python
"""Bitbucket 웹 URL 빌더."""
from __future__ import annotations

from urllib.parse import quote


def _encode_path(path: str) -> str:
    return quote(path, safe="/")


def build_file_url(
    base: str,
    branch: str,
    path: str,
    line_start: int | None = None,
    line_end: int | None = None,
) -> str:
    url = f"{base.rstrip('/')}/src/{branch}/{_encode_path(path)}"
    if line_start is not None and line_end is not None:
        url += f"#lines-{line_start}:{line_end}"
    return url


def build_image_url(base: str, branch: str, path: str) -> str:
    return f"{base.rstrip('/')}/src/{branch}/{_encode_path(path)}"
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

Expected: 4 passed.

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/bitbucket_url.py tests/unit/test_bitbucket_url.py
git commit -m "feat(bitbucket_url): add file and image URL builders with fragment"
```

---

## Task 7: Git 어댑터 (`git_adapter.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/git_adapter.py`
- Create: `tests/integration/test_git_adapter.py`

subprocess로 `git` CLI를 감싼다. clone/fetch/diff/commit/push는 최소한만. diff는 `git diff --name-status -M` 로 rename 감지 포함.

- [ ] **Step 1: 실패 테스트**

```python
import subprocess
from pathlib import Path

import pytest

from markdown_rag_pipeline.git_adapter import GitAdapter
from markdown_rag_pipeline.models import DiffCase


def _init_repo(path: Path) -> None:
    subprocess.check_call(["git", "init", "-b", "main"], cwd=path)
    subprocess.check_call(["git", "config", "user.email", "t@t"], cwd=path)
    subprocess.check_call(["git", "config", "user.name", "t"], cwd=path)


def _commit_file(path: Path, rel: str, content: str, msg: str) -> str:
    (path / rel).parent.mkdir(parents=True, exist_ok=True)
    (path / rel).write_text(content)
    subprocess.check_call(["git", "add", "-A"], cwd=path)
    subprocess.check_call(["git", "commit", "-m", msg], cwd=path)
    return subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=path, text=True
    ).strip()


def test_diff_detects_added(tmp_path: Path):
    _init_repo(tmp_path)
    base = _commit_file(tmp_path, "a.md", "hello", "init")
    _commit_file(tmp_path, "b.md", "world", "add b")

    g = GitAdapter(tmp_path)
    diffs = g.diff(base, "HEAD")
    cases = {d.path: d.case for d in diffs}
    assert cases["b.md"] == DiffCase.ADDED


def test_diff_detects_modified(tmp_path: Path):
    _init_repo(tmp_path)
    _commit_file(tmp_path, "a.md", "hello", "init")
    base = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True
    ).strip()
    _commit_file(tmp_path, "a.md", "hello2", "update a")

    g = GitAdapter(tmp_path)
    diffs = g.diff(base, "HEAD")
    assert any(d.case == DiffCase.MODIFIED and d.path == "a.md" for d in diffs)


def test_diff_detects_deleted(tmp_path: Path):
    _init_repo(tmp_path)
    _commit_file(tmp_path, "a.md", "hello", "init")
    base = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True
    ).strip()
    (tmp_path / "a.md").unlink()
    subprocess.check_call(["git", "add", "-A"], cwd=tmp_path)
    subprocess.check_call(["git", "commit", "-m", "del"], cwd=tmp_path)

    g = GitAdapter(tmp_path)
    diffs = g.diff(base, "HEAD")
    assert any(d.case == DiffCase.DELETED and d.path == "a.md" for d in diffs)


def test_diff_detects_rename(tmp_path: Path):
    _init_repo(tmp_path)
    _commit_file(tmp_path, "old.md", "# same content\n" * 20, "init")
    base = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True
    ).strip()
    (tmp_path / "old.md").rename(tmp_path / "new.md")
    subprocess.check_call(["git", "add", "-A"], cwd=tmp_path)
    subprocess.check_call(["git", "commit", "-m", "mv"], cwd=tmp_path)

    g = GitAdapter(tmp_path)
    diffs = g.diff(base, "HEAD")
    renamed = [d for d in diffs if d.case == DiffCase.RENAMED]
    assert renamed and renamed[0].old_path == "old.md" and renamed[0].path == "new.md"


def test_head_sha(tmp_path: Path):
    _init_repo(tmp_path)
    sha = _commit_file(tmp_path, "a.md", "h", "init")

    g = GitAdapter(tmp_path)
    assert g.head_sha() == sha


def test_show_file_at(tmp_path: Path):
    _init_repo(tmp_path)
    sha = _commit_file(tmp_path, "a.md", "v1", "init")
    _commit_file(tmp_path, "a.md", "v2", "up")

    g = GitAdapter(tmp_path)
    assert g.show_file_at(sha, "a.md") == "v1"
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `git_adapter.py` 구현**

```python
"""Git CLI 래퍼."""
from __future__ import annotations

import subprocess
from pathlib import Path

from .models import DiffEntry


class GitAdapter:
    def __init__(self, repo_dir: Path):
        self.repo_dir = repo_dir

    def _run(self, *args: str, check: bool = True, input_text: str | None = None) -> str:
        result = subprocess.run(
            ["git", *args],
            cwd=self.repo_dir,
            text=True,
            capture_output=True,
            input=input_text,
        )
        if check and result.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)} failed: {result.stderr}")
        return result.stdout

    def clone(self, ssh_url: str) -> None:
        self.repo_dir.mkdir(parents=True, exist_ok=True)
        subprocess.check_call(["git", "clone", ssh_url, str(self.repo_dir)])

    def fetch(self, remote: str = "origin", ref: str = "main") -> None:
        self._run("fetch", remote, ref)

    def head_sha(self, ref: str = "HEAD") -> str:
        return self._run("rev-parse", ref).strip()

    def show_file_at(self, sha: str, path: str) -> str:
        return self._run("show", f"{sha}:{path}")

    def diff(self, base: str, head: str) -> list[DiffEntry]:
        """`git diff --name-status -M` 결과를 DiffEntry 리스트로."""
        out = self._run("diff", "--name-status", "-M", base, head)
        diffs: list[DiffEntry] = []
        for line in out.splitlines():
            if not line.strip():
                continue
            parts = line.split("\t")
            status = parts[0]
            if status.startswith("R"):
                diffs.append(DiffEntry.from_git_rename(status, parts[1], parts[2]))
            elif status in ("A", "M", "D"):
                diffs.append(DiffEntry.from_git(status, parts[1]))
            # 그 외 (C, T 등)는 무시
        return diffs

    def add(self, paths: list[str]) -> None:
        if paths:
            self._run("add", "--", *paths)

    def commit(self, message: str, author_name: str, author_email: str) -> None:
        self._run(
            "-c",
            f"user.name={author_name}",
            "-c",
            f"user.email={author_email}",
            "commit",
            "-m",
            message,
        )

    def pull_rebase_ours(self, remote: str = "origin", ref: str = "main") -> None:
        self._run("pull", "--rebase", "-X", "ours", remote, ref)

    def push(self, remote: str = "origin", ref: str = "main") -> None:
        self._run("push", remote, ref)

    def list_tracked(self, glob: str = "*.md") -> list[str]:
        out = self._run("ls-files", glob)
        return [line for line in out.splitlines() if line]
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

Expected: 6 passed.

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/git_adapter.py tests/integration/test_git_adapter.py
git commit -m "feat(git): add subprocess git adapter with diff and file ops"
```

---

## Task 8: 청킹 래퍼 (`chunking.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/chunking.py`
- Create: `tests/unit/test_chunking.py`

LangChain `MarkdownHeaderTextSplitter`와 `RecursiveCharacterTextSplitter`를 체이닝. breadcrumb은 LangChain metadata에서 단순 join.

- [ ] **Step 1: 실패 테스트**

```python
from markdown_rag_pipeline.chunking import chunk_markdown, ChunkingCfg


def _cfg() -> ChunkingCfg:
    return ChunkingCfg(
        headers_to_split_on=[["#", "H1"], ["##", "H2"], ["###", "H3"]],
        chunk_size=200,
        chunk_overlap=20,
        strategy_version=1,
    )


def test_chunking_preserves_breadcrumb():
    body = (
        "# Doc\n\n"
        "## Section A\n\nContent A.\n\n"
        "### Sub A1\n\nSub content.\n\n"
        "## Section B\n\nContent B.\n"
    )
    chunks = chunk_markdown(body, _cfg())
    assert all(c.breadcrumb for c in chunks)
    bc = [c.breadcrumb for c in chunks]
    assert any("Doc > Section A" in b for b in bc)
    assert any("Doc > Section A > Sub A1" in b for b in bc)
    assert any("Doc > Section B" in b for b in bc)


def test_chunking_splits_long_section():
    long = "가나다라마바사아자차카타파하. " * 100
    body = f"# Doc\n\n## S\n\n{long}\n"
    chunks = chunk_markdown(body, _cfg())
    assert len(chunks) > 1


def test_chunking_empty_body_returns_empty_list():
    assert chunk_markdown("", _cfg()) == []
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `chunking.py` 구현**

```python
"""LangChain 기반 마크다운 청킹."""
from __future__ import annotations

from dataclasses import dataclass

from langchain_text_splitters import (
    MarkdownHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)


@dataclass
class ChunkingCfg:
    headers_to_split_on: list[list[str]]
    chunk_size: int
    chunk_overlap: int
    strategy_version: int


@dataclass
class ChunkResult:
    chunk_text: str
    breadcrumb: str


def chunk_markdown(body: str, cfg: ChunkingCfg) -> list[ChunkResult]:
    if not body.strip():
        return []

    headers = [(h[0], h[1]) for h in cfg.headers_to_split_on]
    header_splitter = MarkdownHeaderTextSplitter(headers_to_split_on=headers)
    md_chunks = header_splitter.split_text(body)

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=cfg.chunk_size,
        chunk_overlap=cfg.chunk_overlap,
    )
    final = text_splitter.split_documents(md_chunks)

    results: list[ChunkResult] = []
    header_names = [h[1] for h in cfg.headers_to_split_on]
    for d in final:
        parts = [d.metadata.get(h) for h in header_names]
        breadcrumb = " > ".join(p for p in parts if p)
        results.append(ChunkResult(chunk_text=d.page_content, breadcrumb=breadcrumb))
    return results
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

Expected: 3 passed.

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/chunking.py tests/unit/test_chunking.py
git commit -m "feat(chunking): add langchain-based markdown chunker"
```

---

## Task 9: 이미지 참조 추출 (`image_ref.py` 본문 파싱)

**Files:**
- Create: `src/markdown_rag_pipeline/image_ref.py`
- Create: `tests/unit/test_image_ref.py`

마크다운 본문에서 `![alt](path)` 참조를 추출하고, 각 참조가 속한 heading 경로(`section_path`)를 계산한다. 외부 URL, 레포 상위 참조(`../`)는 제외 (실제 CI에서 차단되지만 런타임에서도 skip).

- [ ] **Step 1: 실패 테스트**

```python
from markdown_rag_pipeline.image_ref import extract_image_refs


def test_extracts_with_section_path():
    body = (
        "# Doc\n\n"
        "## Arch\n\n"
        "![arch](images/arch.png)\n\n"
        "## Flow\n\n"
        "prefix ![flow](./images/flow.png) suffix\n"
    )
    refs = extract_image_refs(body, md_dir="docs/payment")
    paths = {r.path: r.section_path for r in refs}
    assert paths["docs/payment/images/arch.png"] == "Doc > Arch"
    assert paths["docs/payment/images/flow.png"] == "Doc > Flow"


def test_ignores_external_url():
    body = "# D\n\n![x](https://cdn.example.com/a.png)\n"
    refs = extract_image_refs(body, md_dir="docs")
    assert refs == []


def test_ignores_parent_traversal():
    body = "# D\n\n![x](../outside.png)\n"
    refs = extract_image_refs(body, md_dir="docs/a")
    assert refs == []


def test_no_headings_uses_empty_section_path():
    body = "![x](a.png)"
    refs = extract_image_refs(body, md_dir="docs")
    assert refs and refs[0].section_path == ""
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `image_ref.py` 구현 (추출 부분만)**

```python
"""본문 이미지 참조 추출 + 조상 디렉토리 grep."""
from __future__ import annotations

import os
import posixpath
import re
from dataclasses import dataclass

IMAGE_RE = re.compile(r"!\[[^\]]*\]\(([^)]+)\)")
H1_RE = re.compile(r"^\s*#\s+(.+)$")
H2_RE = re.compile(r"^\s*##\s+(.+)$")
H3_RE = re.compile(r"^\s*###\s+(.+)$")


@dataclass
class ImageRef:
    path: str           # 레포 루트 기준 상대 경로
    section_path: str   # "H1 > H2 > H3"


def _normalize(md_dir: str, ref_path: str) -> str | None:
    # 외부 URL 제외
    if ref_path.startswith(("http://", "https://", "//")):
        return None
    # leading ./
    ref = ref_path.lstrip()
    joined = posixpath.normpath(posixpath.join(md_dir, ref))
    # 상위 이동 금지
    if joined.startswith("..") or joined == "." or joined == "":
        return None
    return joined


def extract_image_refs(body: str, md_dir: str) -> list[ImageRef]:
    h1 = ""
    h2 = ""
    h3 = ""
    refs: list[ImageRef] = []

    for line in body.splitlines():
        if m := H1_RE.match(line):
            h1, h2, h3 = m.group(1).strip(), "", ""
            continue
        if m := H2_RE.match(line):
            h2, h3 = m.group(1).strip(), ""
            continue
        if m := H3_RE.match(line):
            h3 = m.group(1).strip()
            continue

        for m in IMAGE_RE.finditer(line):
            raw = m.group(1).split()[0]  # title 제거
            normalized = _normalize(md_dir, raw)
            if normalized is None:
                continue
            parts = [p for p in (h1, h2, h3) if p]
            section_path = " > ".join(parts)
            refs.append(ImageRef(path=normalized, section_path=section_path))
    return refs
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

Expected: 4 passed.

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/image_ref.py tests/unit/test_image_ref.py
git commit -m "feat(image_ref): extract image references with heading section path"
```

---

## Task 10: 이미지 조상 grep cascade (`image_ref.py` 확장)

**Files:**
- Modify: `src/markdown_rag_pipeline/image_ref.py`
- Modify: `tests/unit/test_image_ref.py`

변경된 이미지 파일을 참조하는 `.md`를 조상 디렉토리 grep으로 찾는다.

- [ ] **Step 1: 추가 테스트 작성**

`tests/unit/test_image_ref.py` 끝에 추가:

```python
from pathlib import Path

from markdown_rag_pipeline.image_ref import find_referring_mds


def test_find_referring_mds(tmp_path: Path):
    (tmp_path / "docs" / "payment" / "images").mkdir(parents=True)
    img = tmp_path / "docs" / "payment" / "images" / "arch.png"
    img.write_bytes(b"\x89PNG")

    (tmp_path / "docs" / "payment" / "refund.md").write_text(
        "# R\n\n![arch](images/arch.png)\n"
    )
    (tmp_path / "docs" / "other.md").write_text(
        "# O\n\n![x](payment/images/arch.png)\n"
    )
    (tmp_path / "README.md").write_text("no image ref")

    results = find_referring_mds(tmp_path, "docs/payment/images/arch.png")
    rels = {str(p.relative_to(tmp_path)) for p in results}
    assert rels == {"docs/payment/refund.md", "docs/other.md"}


def test_find_referring_mds_ignores_unrelated_same_name(tmp_path: Path):
    (tmp_path / "foo").mkdir()
    (tmp_path / "bar").mkdir()
    (tmp_path / "foo" / "arch.png").write_bytes(b"")
    (tmp_path / "bar" / "arch.png").write_bytes(b"")
    (tmp_path / "foo" / "a.md").write_text("![](arch.png)")
    (tmp_path / "bar" / "b.md").write_text("![](arch.png)")

    hits = find_referring_mds(tmp_path, "foo/arch.png")
    rels = {str(p.relative_to(tmp_path)) for p in hits}
    assert rels == {"foo/a.md"}
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

Expected: ImportError 또는 AttributeError.

- [ ] **Step 3: `image_ref.py`에 추가**

```python
import os
from pathlib import Path


def find_referring_mds(repo_root: Path, image_path: str) -> list[Path]:
    """image_path를 참조하는 .md 파일들을 조상 디렉토리 grep으로 찾음."""
    img_name = os.path.basename(image_path)
    img_dir = os.path.dirname(image_path)

    candidate_dirs: list[str] = []
    dir_cursor = img_dir
    while dir_cursor not in ("", ".", "/"):
        candidate_dirs.append(dir_cursor)
        dir_cursor = os.path.dirname(dir_cursor)
    candidate_dirs.append("")  # 레포 루트

    candidates: set[Path] = set()
    for d in candidate_dirs:
        base = repo_root / d if d else repo_root
        if not base.exists():
            continue
        # 재귀 glob — 조상 디렉토리부터 하위 모두 포함
        for md in base.rglob("*.md"):
            try:
                content = md.read_text()
            except (OSError, UnicodeDecodeError):
                continue
            if img_name not in content:
                continue
            candidates.add(md)

    # 정규화 검증: 각 후보의 이미지 참조를 레포 루트 기준 경로로 환산
    confirmed: list[Path] = []
    for md in candidates:
        md_dir = str(md.parent.relative_to(repo_root))
        md_dir = "" if md_dir == "." else md_dir
        refs = extract_image_refs(md.read_text(), md_dir)
        if any(r.path == image_path for r in refs):
            confirmed.append(md)
    return sorted(confirmed)
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

Expected: 추가 테스트 2개 pass.

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/image_ref.py tests/unit/test_image_ref.py
git commit -m "feat(image_ref): find referring .md files via ancestor dir grep"
```

---

## Task 11: BGE-M3 클라이언트 (`clients/embedding.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/clients/embedding.py`
- Create: `tests/unit/test_embedding_client.py`
- Create: `tests/fixtures/fake_bge_m3.py`

HTTP로 BGE-M3 추론 서버에 배치 요청. 응답은 dense + sparse. Mock 서버는 pytest-httpx로.

- [ ] **Step 1: 실패 테스트**

```python
import pytest
from pytest_httpx import HTTPXMock

from markdown_rag_pipeline.clients.embedding import BgeM3Client


def test_embed_batch_returns_dense_and_sparse(httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        url="http://bge/embed",
        json={
            "dense": [[0.1] * 1024, [0.2] * 1024],
            "sparse": [{"1": 0.5, "7": 0.3}, {"2": 0.4}],
        },
    )
    client = BgeM3Client(endpoint="http://bge", dense_dim=1024)
    dense, sparse = client.embed_batch(["hello", "world"])
    assert len(dense) == 2 and len(dense[0]) == 1024
    assert sparse[0] == {1: 0.5, 7: 0.3}


def test_embed_batch_validates_dim(httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        url="http://bge/embed",
        json={"dense": [[0.1] * 512], "sparse": [{}]},
    )
    client = BgeM3Client(endpoint="http://bge", dense_dim=1024)
    with pytest.raises(ValueError, match="dim"):
        client.embed_batch(["a"])
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `clients/embedding.py` 구현**

```python
"""BGE-M3 HTTP 클라이언트."""
from __future__ import annotations

import httpx


class BgeM3Client:
    def __init__(self, endpoint: str, dense_dim: int, timeout: float = 30.0):
        self.endpoint = endpoint.rstrip("/")
        self.dense_dim = dense_dim
        self.timeout = timeout

    def embed_batch(
        self, texts: list[str]
    ) -> tuple[list[list[float]], list[dict[int, float]]]:
        """입력 텍스트 리스트 → (dense 벡터, sparse 딕셔너리) 튜플."""
        resp = httpx.post(
            f"{self.endpoint}/embed",
            json={"texts": texts},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        dense = data["dense"]
        for v in dense:
            if len(v) != self.dense_dim:
                raise ValueError(
                    f"dense dim mismatch: expected {self.dense_dim}, got {len(v)}"
                )
        sparse = [{int(k): float(v) for k, v in s.items()} for s in data["sparse"]]
        return dense, sparse
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

Expected: 2 passed.

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/clients/embedding.py tests/unit/test_embedding_client.py
git commit -m "feat(clients): add BGE-M3 embedding HTTP client"
```

---

## Task 12: LLM 클라이언트 (`clients/llm.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/clients/llm.py`
- Create: `prompts/llm-summary.md`
- Create: `prompts/llm-classify.md`
- Create: `tests/unit/test_llm_client.py`

문서 본문을 입력으로 받아 `summary`, `doc_type`, `topics`, `language`를 추출하는 단일 호출 메서드.

- [ ] **Step 1: 프롬프트 파일 작성**

`prompts/llm-classify.md`:
```markdown
다음 마크다운 문서를 분석하여 JSON 형식으로 응답하라.

응답 스키마:
{
  "summary": "한 줄 요약 (50-100자)",
  "doc_type": "spec | runbook | meeting | policy | faq | how-to | reference | other",
  "topics": ["키워드1", "키워드2", "..."],
  "language": "ko | en | mixed"
}

규칙:
- summary는 문서의 핵심을 간결히 한 문장.
- doc_type은 위 enum 중 하나.
- topics는 3-7개의 주요 키워드.
- language는 본문 주 언어.

문서:
---
{body}
---
```

`prompts/llm-summary.md`는 위와 동일하되 summary만 반환하는 버전(필요 시 분리 사용). 현재 단일 호출로 통합했으므로 파일은 향후 확장용 placeholder:
```markdown
<!-- 현재는 llm-classify.md를 단일 호출로 사용. 분리 호출이 필요해지면 이 파일을 활용. -->
```

- [ ] **Step 2: 실패 테스트**

```python
import pytest
from pytest_httpx import HTTPXMock

from markdown_rag_pipeline.clients.llm import LLMClient, Classification


def test_classify_parses_json(httpx_mock: HTTPXMock):
    httpx_mock.add_response(
        url="http://llm/chat",
        json={
            "content": '{"summary":"s","doc_type":"runbook","topics":["k","o"],"language":"ko"}'
        },
    )
    client = LLMClient(
        endpoint="http://llm",
        model_name="exaone",
        prompt_template="... {body} ...",
    )
    result = client.classify(body="# Doc\ncontent")
    assert isinstance(result, Classification)
    assert result.doc_type == "runbook"
    assert result.topics == ["k", "o"]
    assert result.language == "ko"


def test_classify_invalid_json_raises(httpx_mock: HTTPXMock):
    httpx_mock.add_response(url="http://llm/chat", json={"content": "not json"})
    client = LLMClient(
        endpoint="http://llm", model_name="x", prompt_template="{body}"
    )
    with pytest.raises(ValueError, match="parse"):
        client.classify(body="x")
```

- [ ] **Step 3: 테스트 실행 — 실패 확인**

- [ ] **Step 4: `clients/llm.py` 구현**

```python
"""LLM 분류/요약 클라이언트."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import httpx


@dataclass
class Classification:
    summary: str
    doc_type: str
    topics: list[str]
    language: str


class LLMClient:
    def __init__(
        self,
        endpoint: str,
        model_name: str,
        prompt_template: str,
        timeout: float = 60.0,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.model_name = model_name
        self.prompt_template = prompt_template
        self.timeout = timeout

    @classmethod
    def from_prompt_file(
        cls, endpoint: str, model_name: str, prompt_path: Path
    ) -> "LLMClient":
        return cls(
            endpoint=endpoint,
            model_name=model_name,
            prompt_template=prompt_path.read_text(),
        )

    def classify(self, body: str) -> Classification:
        prompt = self.prompt_template.format(body=body)
        resp = httpx.post(
            f"{self.endpoint}/chat",
            json={"model": self.model_name, "prompt": prompt},
            timeout=self.timeout,
        )
        resp.raise_for_status()
        content = resp.json()["content"]
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            raise ValueError(f"failed to parse LLM response: {e}")
        return Classification(
            summary=data["summary"],
            doc_type=data["doc_type"],
            topics=list(data["topics"]),
            language=data["language"],
        )
```

- [ ] **Step 5: 테스트 실행 — 통과 확인**

- [ ] **Step 6: 커밋**

```bash
git add src/markdown_rag_pipeline/clients/llm.py prompts/ tests/unit/test_llm_client.py
git commit -m "feat(clients): add LLM classification client with prompt file"
```

---

## Task 13: VLM 클라이언트 (`clients/vlm.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/clients/vlm.py`
- Create: `prompts/vlm-caption.md`
- Create: `tests/unit/test_vlm_client.py`

이미지 파일 경로 + 문서 언어를 입력 받아 캡션 텍스트를 반환. 내부적으로 hash 기반 캐시.

- [ ] **Step 1: `prompts/vlm-caption.md`**

```markdown
다음 이미지에 대해 {language} 로 캡션을 작성한다.

규칙:
- 객관적 묘사 우선 (형태가 아니라 의미).
- 이미지 내부 텍스트(라벨, 화살표 설명, 표 값)는 빠짐없이 추출.
- 길이는 약 150-400자.
- 지나친 미사여구 금지. 기술적으로 정확하게.
- JSON으로 {{"caption": "..."}} 만 반환하라.
```

- [ ] **Step 2: 실패 테스트**

```python
import hashlib
from pathlib import Path

import pytest
from pytest_httpx import HTTPXMock

from markdown_rag_pipeline.clients.vlm import VlmClient


def _img(tmp_path: Path) -> Path:
    p = tmp_path / "a.png"
    p.write_bytes(b"\x89PNG\x00fake")
    return p


def test_caption_returns_text(httpx_mock: HTTPXMock, tmp_path: Path):
    httpx_mock.add_response(
        url="http://vlm/caption",
        json={"content": '{"caption":"API Gateway to Auth Service flow."}'},
    )
    client = VlmClient(
        endpoint="http://vlm",
        model_name="qwen2-vl",
        prompt_template="lang={language}",
        min_chars=1,
        max_chars=1000,
    )
    cap = client.caption(_img(tmp_path), language="ko")
    assert "API Gateway" in cap


def test_caption_caches_by_hash(httpx_mock: HTTPXMock, tmp_path: Path):
    httpx_mock.add_response(
        url="http://vlm/caption",
        json={"content": '{"caption":"c1"}'},
    )
    client = VlmClient(
        endpoint="http://vlm", model_name="x", prompt_template="{language}",
        min_chars=1, max_chars=100, cache_enabled=True,
    )
    img = _img(tmp_path)
    a = client.caption(img, language="ko")
    b = client.caption(img, language="ko")
    assert a == b
    # Mock 한 번만 등록했으므로, 두 번째 호출이 캐시 적중이어서 성공


def test_caption_min_length_violation(httpx_mock: HTTPXMock, tmp_path: Path):
    httpx_mock.add_response(
        url="http://vlm/caption",
        json={"content": '{"caption":"짧음"}'},
    )
    client = VlmClient(
        endpoint="http://vlm", model_name="x", prompt_template="{language}",
        min_chars=100, max_chars=500,
    )
    with pytest.raises(ValueError, match="min"):
        client.caption(_img(tmp_path), language="ko")
```

- [ ] **Step 3: 테스트 실행 — 실패 확인**

- [ ] **Step 4: `clients/vlm.py` 구현**

```python
"""VLM 이미지 캡셔닝 클라이언트."""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path

import httpx


class VlmClient:
    def __init__(
        self,
        endpoint: str,
        model_name: str,
        prompt_template: str,
        min_chars: int,
        max_chars: int,
        timeout: float = 60.0,
        cache_enabled: bool = True,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.model_name = model_name
        self.prompt_template = prompt_template
        self.min_chars = min_chars
        self.max_chars = max_chars
        self.timeout = timeout
        self.cache_enabled = cache_enabled
        self._cache: dict[str, str] = {}

    @classmethod
    def from_prompt_file(
        cls,
        endpoint: str,
        model_name: str,
        prompt_path: Path,
        min_chars: int,
        max_chars: int,
        cache_enabled: bool = True,
    ) -> "VlmClient":
        return cls(
            endpoint=endpoint,
            model_name=model_name,
            prompt_template=prompt_path.read_text(),
            min_chars=min_chars,
            max_chars=max_chars,
            cache_enabled=cache_enabled,
        )

    def caption(self, image_path: Path, language: str) -> str:
        img_bytes = image_path.read_bytes()
        key = f"{hashlib.sha256(img_bytes).hexdigest()}:{language}"
        if self.cache_enabled and key in self._cache:
            return self._cache[key]

        prompt = self.prompt_template.format(language=language)
        resp = httpx.post(
            f"{self.endpoint}/caption",
            json={
                "model": self.model_name,
                "prompt": prompt,
                "image_b64": base64.b64encode(img_bytes).decode(),
            },
            timeout=self.timeout,
        )
        resp.raise_for_status()
        try:
            data = json.loads(resp.json()["content"])
            caption = data["caption"]
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(f"failed to parse VLM response: {e}")

        if len(caption) < self.min_chars:
            raise ValueError(
                f"caption too short: {len(caption)} < min {self.min_chars}"
            )
        caption = caption[: self.max_chars]

        if self.cache_enabled:
            self._cache[key] = caption
        return caption
```

- [ ] **Step 5: 테스트 실행 — 통과 확인**

- [ ] **Step 6: 커밋**

```bash
git add src/markdown_rag_pipeline/clients/vlm.py prompts/vlm-caption.md tests/unit/test_vlm_client.py
git commit -m "feat(clients): add VLM captioning client with hash cache"
```

---

## Task 14: Milvus 어댑터 — 스키마/연결 (`milvus_adapter.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/milvus_adapter.py`
- Create: `tests/integration/test_milvus_adapter.py`

**주의**: Milvus 통합 테스트는 testcontainer 또는 사내 테스트 인스턴스 필요. 로컬에 Milvus standalone(docker)를 띄워 테스트하는 것을 권장. 이 Task에서는 테스트 스킵 데코레이터로 Milvus 환경 변수 없을 때 건너뛰도록 한다.

- [ ] **Step 1: 통합 테스트 작성**

```python
import os

import pytest
from pymilvus import connections, utility

from markdown_rag_pipeline.milvus_adapter import MilvusAdapter


pytestmark = pytest.mark.skipif(
    not os.environ.get("MILVUS_TEST_HOST"),
    reason="MILVUS_TEST_HOST not set",
)


@pytest.fixture
def collection_name() -> str:
    return f"test_docs_{os.getpid()}"


@pytest.fixture
def adapter(collection_name: str):
    host = os.environ["MILVUS_TEST_HOST"]
    a = MilvusAdapter(host=host, port=19530, collection_name=collection_name, dense_dim=8)
    a.connect()
    a.ensure_collection()
    yield a
    connections.connect(host=host, port=19530)
    if utility.has_collection(collection_name):
        utility.drop_collection(collection_name)


def test_ensure_collection_creates(adapter, collection_name):
    assert utility.has_collection(collection_name)
```

- [ ] **Step 2: 테스트 실행 — 실패 또는 skip 확인**

```bash
pytest tests/integration/test_milvus_adapter.py -v
```
환경변수 없으면 skip, 있으면 fail (모듈 없음).

- [ ] **Step 3: `milvus_adapter.py` 구현 — 스키마/연결 부분**

```python
"""Milvus 컬렉션 어댑터."""
from __future__ import annotations

from pymilvus import (
    CollectionSchema,
    DataType,
    FieldSchema,
    Collection,
    connections,
    utility,
)


class MilvusAdapter:
    def __init__(
        self,
        host: str,
        port: int,
        collection_name: str,
        dense_dim: int,
        connection_alias: str = "default",
    ):
        self.host = host
        self.port = port
        self.collection_name = collection_name
        self.dense_dim = dense_dim
        self.alias = connection_alias
        self._collection: Collection | None = None

    def connect(self) -> None:
        connections.connect(alias=self.alias, host=self.host, port=self.port)

    def ensure_collection(self) -> None:
        if utility.has_collection(self.collection_name, using=self.alias):
            self._collection = Collection(self.collection_name, using=self.alias)
            return

        fields = [
            FieldSchema(name="chunk_uid", dtype=DataType.VARCHAR, max_length=128,
                        is_primary=True, auto_id=False),
            FieldSchema(name="doc_id", dtype=DataType.VARCHAR, max_length=36),
            FieldSchema(name="chunk_index", dtype=DataType.INT64),
            FieldSchema(name="chunk_kind", dtype=DataType.VARCHAR, max_length=8),
            FieldSchema(name="dense_vector", dtype=DataType.FLOAT_VECTOR, dim=self.dense_dim),
            FieldSchema(name="sparse_vector", dtype=DataType.SPARSE_FLOAT_VECTOR),
            FieldSchema(name="chunk_text", dtype=DataType.VARCHAR, max_length=20000),
            FieldSchema(name="breadcrumb", dtype=DataType.VARCHAR, max_length=1024),
            FieldSchema(name="path", dtype=DataType.VARCHAR, max_length=1024),
            FieldSchema(name="title", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="doc_type", dtype=DataType.VARCHAR, max_length=32),
            FieldSchema(name="topics", dtype=DataType.ARRAY, element_type=DataType.VARCHAR,
                        max_capacity=16, max_length=64),
            FieldSchema(name="language", dtype=DataType.VARCHAR, max_length=16),
            FieldSchema(name="authors", dtype=DataType.ARRAY, element_type=DataType.VARCHAR,
                        max_capacity=16, max_length=64),
            FieldSchema(name="created_at", dtype=DataType.INT64),
            FieldSchema(name="updated_at", dtype=DataType.INT64),
            FieldSchema(name="last_indexed_at", dtype=DataType.INT64),
            FieldSchema(name="has_images", dtype=DataType.BOOL),
            FieldSchema(name="source_url", dtype=DataType.VARCHAR, max_length=2048),
            FieldSchema(name="line_start", dtype=DataType.INT64),
            FieldSchema(name="line_end", dtype=DataType.INT64),
            FieldSchema(name="user_meta", dtype=DataType.JSON),
        ]
        schema = CollectionSchema(fields=fields, description="마크다운 RAG 청크 컬렉션")
        self._collection = Collection(
            name=self.collection_name, schema=schema, using=self.alias
        )

        # 인덱스
        self._collection.create_index(
            field_name="dense_vector",
            index_params={"index_type": "HNSW", "metric_type": "COSINE",
                          "params": {"M": 24, "efConstruction": 200}},
        )
        self._collection.create_index(
            field_name="sparse_vector",
            index_params={"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "IP"},
        )
        for scalar in ["doc_id", "chunk_kind", "path", "doc_type", "language"]:
            self._collection.create_index(
                field_name=scalar, index_name=f"idx_{scalar}",
                index_params={"index_type": "INVERTED"},
            )
        self._collection.load()

    @property
    def collection(self) -> Collection:
        if self._collection is None:
            raise RuntimeError("call ensure_collection() first")
        return self._collection
```

- [ ] **Step 4: 테스트 실행 — 통과 확인 (Milvus 환경에서)**

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/milvus_adapter.py tests/integration/test_milvus_adapter.py
git commit -m "feat(milvus): add collection schema and connection adapter"
```

---

## Task 15: Milvus 어댑터 — upsert/delete/stale

**Files:**
- Modify: `src/markdown_rag_pipeline/milvus_adapter.py`
- Modify: `tests/integration/test_milvus_adapter.py`

`upsert_chunks(chunks)`, `delete_by_doc(doc_id)`, `cleanup_stale(doc_id, keep_uids)`, `existing_chunk_uids(doc_id)`.

- [ ] **Step 1: 추가 테스트**

```python
from markdown_rag_pipeline.models import Chunk, ChunkKind


def _make_chunk(doc_id: str, kind: ChunkKind, i: int) -> Chunk:
    c = Chunk(
        doc_id=doc_id, chunk_kind=kind, chunk_index=i,
        chunk_text=f"t{i}", breadcrumb="B", line_start=0, line_end=1,
        dense_vector=[0.0] * 8, sparse_vector={1: 0.5},
        path="docs/x.md", title="T", doc_type="reference",
        topics=["a"], language="ko", authors=["u"],
        created_at_ms=1, updated_at_ms=2, last_indexed_at_ms=3,
        has_images=False, source_url="http://x", user_meta={},
    )
    return c


def test_upsert_and_existing_uids(adapter):
    chunks = [_make_chunk("D1", ChunkKind.BODY, i) for i in range(3)]
    adapter.upsert_chunks(chunks)
    adapter.flush()
    got = adapter.existing_chunk_uids("D1")
    assert set(got) == {"D1:body:0", "D1:body:1", "D1:body:2"}


def test_cleanup_stale(adapter):
    chunks_v1 = [_make_chunk("D2", ChunkKind.BODY, i) for i in range(5)]
    adapter.upsert_chunks(chunks_v1)
    adapter.flush()
    chunks_v2 = [_make_chunk("D2", ChunkKind.BODY, i) for i in range(2)]
    adapter.upsert_chunks(chunks_v2)
    adapter.flush()
    adapter.cleanup_stale("D2", keep_uids={"D2:body:0", "D2:body:1"})
    adapter.flush()
    assert set(adapter.existing_chunk_uids("D2")) == {"D2:body:0", "D2:body:1"}


def test_delete_by_doc(adapter):
    chunks = [_make_chunk("D3", ChunkKind.BODY, i) for i in range(2)]
    adapter.upsert_chunks(chunks)
    adapter.flush()
    adapter.delete_by_doc("D3")
    adapter.flush()
    assert adapter.existing_chunk_uids("D3") == []
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `milvus_adapter.py`에 추가**

```python
from .models import Chunk


class MilvusAdapter:
    # ... (기존 코드 유지)

    def _chunk_to_entity(self, c: Chunk) -> dict:
        return {
            "chunk_uid": c.chunk_uid,
            "doc_id": c.doc_id,
            "chunk_index": c.chunk_index,
            "chunk_kind": c.chunk_kind.value,
            "dense_vector": c.dense_vector,
            "sparse_vector": c.sparse_vector or {},
            "chunk_text": c.chunk_text,
            "breadcrumb": c.breadcrumb,
            "path": c.path,
            "title": c.title,
            "doc_type": c.doc_type,
            "topics": c.topics,
            "language": c.language,
            "authors": c.authors,
            "created_at": c.created_at_ms,
            "updated_at": c.updated_at_ms,
            "last_indexed_at": c.last_indexed_at_ms,
            "has_images": c.has_images,
            "source_url": c.source_url,
            "line_start": c.line_start,
            "line_end": c.line_end,
            "user_meta": c.user_meta,
        }

    def upsert_chunks(self, chunks: list[Chunk]) -> None:
        if not chunks:
            return
        entities = [self._chunk_to_entity(c) for c in chunks]
        self.collection.upsert(entities)

    def existing_chunk_uids(self, doc_id: str) -> list[str]:
        res = self.collection.query(
            expr=f"doc_id == '{doc_id}'",
            output_fields=["chunk_uid"],
        )
        return [r["chunk_uid"] for r in res]

    def cleanup_stale(self, doc_id: str, keep_uids: set[str]) -> None:
        existing = set(self.existing_chunk_uids(doc_id))
        stale = existing - keep_uids
        if stale:
            quoted = ",".join(f'"{u}"' for u in stale)
            self.collection.delete(expr=f"chunk_uid in [{quoted}]")

    def delete_by_doc(self, doc_id: str) -> None:
        self.collection.delete(expr=f"doc_id == '{doc_id}'")

    def flush(self) -> None:
        self.collection.flush()
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/milvus_adapter.py tests/integration/test_milvus_adapter.py
git commit -m "feat(milvus): add upsert, stale cleanup, and doc-level delete"
```

---

## Task 16: 봇 Git 전략 (`bot_commit.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/bot_commit.py`
- Create: `tests/unit/test_bot_commit.py`

봇 커밋 조립 + `-X ours` rebase + push retry (지수 백오프). push는 Git 어댑터 위임. retry 로직은 시간 sleep 대신 주입 가능한 sleep 함수로 테스트.

- [ ] **Step 1: 실패 테스트**

```python
from unittest.mock import MagicMock

import pytest

from markdown_rag_pipeline.bot_commit import BotCommitter, BotConfig


def _cfg() -> BotConfig:
    return BotConfig(
        author_name="docs-bot",
        author_email="bot@x",
        commit_tag="[pipeline-auto]",
        push_max_retries=3,
        push_backoff_initial_sec=1,
        push_backoff_max_sec=10,
        push_backoff_jitter=0.0,
    )


def test_verify_no_body_changes_passes():
    git = MagicMock()
    git._run.return_value = "---\ndoc_id: x\n---\n"  # staged diff only in frontmatter
    b = BotCommitter(git, _cfg())
    # 허용: YAML 프런트매터 라인만 포함된 diff
    b._verify_no_body_changes(
        diff_text="+++ b/a.md\n@@\n+doc_id: new\n"
    )


def test_verify_no_body_changes_rejects_body():
    git = MagicMock()
    b = BotCommitter(git, _cfg())
    with pytest.raises(RuntimeError, match="body"):
        b._verify_no_body_changes(
            diff_text="+++ b/a.md\n@@\n+doc_id: x\n+body line added\n"
        )


def test_commit_and_push_retries(monkeypatch):
    git = MagicMock()
    sleeps: list[float] = []
    call_count = {"push": 0}

    def fake_push(*a, **kw):
        call_count["push"] += 1
        if call_count["push"] < 3:
            raise RuntimeError("non-fast-forward")

    git.push.side_effect = fake_push
    git.pull_rebase_ours.return_value = None

    b = BotCommitter(git, _cfg())
    b._sleep = lambda s: sleeps.append(s)

    b.push_with_retry()
    assert call_count["push"] == 3
    assert len(sleeps) == 2


def test_commit_and_push_final_failure():
    git = MagicMock()
    git.push.side_effect = RuntimeError("rejected")
    b = BotCommitter(git, _cfg())
    b._sleep = lambda s: None
    with pytest.raises(RuntimeError, match="giving up"):
        b.push_with_retry()
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `bot_commit.py` 구현**

```python
"""봇 커밋 조립, rebase, push 재시도."""
from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from typing import Callable

from .git_adapter import GitAdapter


@dataclass
class BotConfig:
    author_name: str
    author_email: str
    commit_tag: str
    push_max_retries: int
    push_backoff_initial_sec: int
    push_backoff_max_sec: int
    push_backoff_jitter: float


class BotCommitter:
    def __init__(self, git: GitAdapter, cfg: BotConfig):
        self.git = git
        self.cfg = cfg
        self._sleep: Callable[[float], None] = time.sleep

    def _verify_no_body_changes(self, diff_text: str) -> None:
        """스테이징된 diff에서 YAML frontmatter 이외의 라인이 변경된 경우 예외."""
        in_frontmatter = False
        fence = 0
        for line in diff_text.splitlines():
            if line.startswith("+++") or line.startswith("---"):
                in_frontmatter = False
                fence = 0
                continue
            if line.startswith("@@"):
                continue
            if line.startswith("+") or line.startswith("-"):
                content = line[1:]
                if content.strip() == "---":
                    fence += 1
                    in_frontmatter = fence % 2 == 1
                    continue
                if not in_frontmatter and content.strip():
                    raise RuntimeError(
                        f"body line change detected in bot commit: {line!r}"
                    )

    def compose_and_push(
        self,
        changed_paths: list[str],
        state_file_path: str,
        head_sha_short: str,
        n_files: int,
    ) -> None:
        if not changed_paths:
            return
        paths = list(changed_paths) + [state_file_path]
        self.git.add(paths)
        staged = self.git._run("diff", "--cached", "--no-color")
        self._verify_no_body_changes(staged)

        msg = f"docs: index frontmatter (commit={head_sha_short}, {n_files} files) {self.cfg.commit_tag}"
        self.git.commit(msg, self.cfg.author_name, self.cfg.author_email)
        self.push_with_retry()

    def push_with_retry(self) -> None:
        for attempt in range(1, self.cfg.push_max_retries + 1):
            try:
                if attempt > 1:
                    self.git.pull_rebase_ours()
                self.git.push()
                return
            except RuntimeError as e:
                if attempt == self.cfg.push_max_retries:
                    raise RuntimeError(
                        f"giving up push after {attempt} attempts: {e}"
                    )
                delay = min(
                    self.cfg.push_backoff_initial_sec * (2 ** (attempt - 1)),
                    self.cfg.push_backoff_max_sec,
                )
                jitter = random.uniform(-self.cfg.push_backoff_jitter, self.cfg.push_backoff_jitter)
                self._sleep(delay * (1 + jitter))
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/bot_commit.py tests/unit/test_bot_commit.py
git commit -m "feat(bot_commit): add commit guard, rebase -X ours, push retry"
```

---

## Task 17: 문서 처리 — Case 1 Added (`processing.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/processing.py`
- Create: `tests/unit/test_processing.py`

의존성(git, LLM, VLM, Embedding, Milvus)은 생성자로 주입 가능해 mock 테스트 가능.

- [ ] **Step 1: 실패 테스트 — 단위 테스트 (Milvus 제외)**

```python
import uuid
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from markdown_rag_pipeline.frontmatter import ParsedDoc, Reserved
from markdown_rag_pipeline.models import ChunkKind, DiffCase, DiffEntry
from markdown_rag_pipeline.processing import DocProcessor, ProcessingContext
from markdown_rag_pipeline.clients.llm import Classification


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "a.md").write_text("# Hello\n\nBody.\n")
    return tmp_path


def test_added_assigns_doc_id_and_chunks(repo):
    llm = MagicMock()
    llm.classify.return_value = Classification(
        summary="s", doc_type="reference", topics=["t"], language="ko"
    )
    vlm = MagicMock()
    embed = MagicMock()
    embed.embed_batch.return_value = ([[0.1] * 4], [{0: 0.5}])
    milvus = MagicMock()
    git = MagicMock()
    git.head_sha.return_value = "abc1234"
    git._run.return_value = "alice\n"  # for authors/created_at queries

    ctx = ProcessingContext(
        repo_root=repo,
        git=git, llm=llm, vlm=vlm, embed=embed, milvus=milvus,
        bitbucket_base="https://bb/r", branch="main",
        chunking_cfg=None, vision_language_hint="ko",
    )
    # chunking_cfg None → real one 주입
    from markdown_rag_pipeline.chunking import ChunkingCfg
    ctx.chunking_cfg = ChunkingCfg(
        headers_to_split_on=[["#", "H1"]],
        chunk_size=100, chunk_overlap=10, strategy_version=1,
    )

    proc = DocProcessor(ctx)
    entry = DiffEntry(case=DiffCase.ADDED, path="docs/a.md")
    result = proc.process(entry)

    assert result.reserved.doc_id  # UUID 발급됨
    uuid.UUID(result.reserved.doc_id)
    assert result.reserved.indexing_status == "ok"
    assert milvus.upsert_chunks.called
    upserted = milvus.upsert_chunks.call_args[0][0]
    assert all(c.chunk_kind == ChunkKind.BODY for c in upserted)
    assert all(c.doc_id == result.reserved.doc_id for c in upserted)


def test_added_rag_exclude_skips(repo):
    (repo / "docs" / "a.md").write_text(
        "---\nrag_exclude: true\n---\n# X\n"
    )
    milvus = MagicMock()
    ctx = ProcessingContext(
        repo_root=repo,
        git=MagicMock(), llm=MagicMock(), vlm=MagicMock(),
        embed=MagicMock(), milvus=milvus,
        bitbucket_base="https://bb/r", branch="main",
        chunking_cfg=None, vision_language_hint="ko",
    )
    proc = DocProcessor(ctx)
    entry = DiffEntry(case=DiffCase.ADDED, path="docs/a.md")
    result = proc.process(entry)
    assert result.skipped is True
    assert not milvus.upsert_chunks.called
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `processing.py` 구현 — Case 1 Added 한정**

```python
"""문서별 케이스 처리."""
from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .bitbucket_url import build_file_url, build_image_url
from .chunking import ChunkingCfg, chunk_markdown
from .clients.embedding import BgeM3Client
from .clients.llm import LLMClient
from .clients.vlm import VlmClient
from .frontmatter import (
    ParsedDoc, Reserved, ImageEntry,
    parse_file, write_file, is_rag_excluded,
)
from .git_adapter import GitAdapter
from .image_ref import extract_image_refs
from .milvus_adapter import MilvusAdapter
from .models import Chunk, ChunkKind, DiffCase, DiffEntry


@dataclass
class ProcessingContext:
    repo_root: Path
    git: GitAdapter
    llm: LLMClient
    vlm: VlmClient
    embed: BgeM3Client
    milvus: MilvusAdapter
    bitbucket_base: str
    branch: str
    chunking_cfg: Optional[ChunkingCfg]
    vision_language_hint: str


@dataclass
class ProcessingResult:
    path: str
    reserved: Reserved = field(default_factory=Reserved)
    skipped: bool = False
    failed: bool = False
    error: str = ""


def _sha256(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now_ms() -> int:
    return int(time.time() * 1000)


class DocProcessor:
    PIPELINE_SCHEMA_VERSION = 1

    def __init__(self, ctx: ProcessingContext):
        self.ctx = ctx

    def process(self, entry: DiffEntry) -> ProcessingResult:
        if entry.case == DiffCase.ADDED:
            return self._process_added(entry)
        raise NotImplementedError(f"case {entry.case} not yet implemented")

    def _process_added(self, entry: DiffEntry) -> ProcessingResult:
        full_path = self.ctx.repo_root / entry.path
        doc = parse_file(full_path)

        raw_fm = {**doc.user_meta}
        if is_rag_excluded(raw_fm):
            return ProcessingResult(path=entry.path, skipped=True)

        doc_id = doc.reserved.doc_id or str(uuid.uuid4())
        doc.reserved.doc_id = doc_id
        doc.reserved.path = entry.path
        doc.reserved.content_hash = _sha256(doc.body)

        md_dir = str(full_path.parent.relative_to(self.ctx.repo_root))
        if md_dir == ".":
            md_dir = ""
        image_refs = extract_image_refs(doc.body, md_dir=md_dir)
        image_entries: list[ImageEntry] = []
        for r in image_refs:
            img_path = self.ctx.repo_root / r.path
            if not img_path.exists():
                continue
            caption = self.ctx.vlm.caption(img_path, self.ctx.vision_language_hint)
            image_entries.append(ImageEntry(
                path=r.path, caption=caption, section_path=r.section_path,
            ))
        doc.reserved.images = image_entries
        doc.reserved.has_images = bool(image_entries)

        classification = self.ctx.llm.classify(body=doc.body)
        doc.reserved.summary = classification.summary
        doc.reserved.doc_type = classification.doc_type
        doc.reserved.topics = classification.topics
        doc.reserved.language = classification.language

        head = self.ctx.git.head_sha()
        doc.reserved.last_indexed_commit = head
        doc.reserved.last_indexed_at = _now_iso()
        doc.reserved.pipeline_schema_version = self.PIPELINE_SCHEMA_VERSION
        doc.reserved.chunk_strategy_version = self.ctx.chunking_cfg.strategy_version
        doc.reserved.embedding_model = ""   # orchestrator가 주입해도 됨
        doc.reserved.vision_schema_version = 1

        # title: 본문 첫 H1
        for line in doc.body.splitlines():
            stripped = line.strip()
            if stripped.startswith("# "):
                doc.reserved.title = stripped[2:].strip()
                break

        doc.reserved.web_url = build_file_url(
            self.ctx.bitbucket_base, self.ctx.branch, entry.path
        )

        # 본문 청킹
        body_chunks = chunk_markdown(doc.body, self.ctx.chunking_cfg)
        # 이미지 청크 구성
        img_chunk_texts = [
            f"{e.section_path}\n\n{e.caption}" for e in image_entries
        ]
        all_texts = [c.chunk_text for c in body_chunks] + img_chunk_texts

        if not all_texts:
            doc.reserved.chunk_count = 0
            doc.reserved.indexing_status = "ok"
            write_file(full_path, doc)
            return ProcessingResult(path=entry.path, reserved=doc.reserved)

        dense, sparse = self.ctx.embed.embed_batch(all_texts)

        chunks: list[Chunk] = []
        now_ms = _now_ms()
        for i, bc in enumerate(body_chunks):
            chunks.append(Chunk(
                doc_id=doc_id, chunk_kind=ChunkKind.BODY, chunk_index=i,
                chunk_text=bc.chunk_text, breadcrumb=bc.breadcrumb,
                line_start=0, line_end=0,
                dense_vector=dense[i], sparse_vector=sparse[i],
                path=entry.path, title=doc.reserved.title or "",
                doc_type=doc.reserved.doc_type or "",
                topics=doc.reserved.topics, language=doc.reserved.language or "",
                authors=doc.reserved.authors,
                created_at_ms=0, updated_at_ms=0, last_indexed_at_ms=now_ms,
                has_images=doc.reserved.has_images or False,
                source_url=doc.reserved.web_url, user_meta=doc.user_meta,
            ))
        offset = len(body_chunks)
        for j, entry_img in enumerate(image_entries):
            chunks.append(Chunk(
                doc_id=doc_id, chunk_kind=ChunkKind.IMAGE, chunk_index=j,
                chunk_text=img_chunk_texts[j],
                breadcrumb=entry_img.section_path,
                line_start=0, line_end=0,
                dense_vector=dense[offset + j], sparse_vector=sparse[offset + j],
                path=entry.path, title=doc.reserved.title or "",
                doc_type=doc.reserved.doc_type or "",
                topics=doc.reserved.topics, language=doc.reserved.language or "",
                authors=doc.reserved.authors,
                created_at_ms=0, updated_at_ms=0, last_indexed_at_ms=now_ms,
                has_images=True,
                source_url=build_image_url(
                    self.ctx.bitbucket_base, self.ctx.branch, entry_img.path
                ),
                user_meta=doc.user_meta,
            ))

        self.ctx.milvus.upsert_chunks(chunks)
        doc.reserved.chunk_count = len(chunks)
        doc.reserved.indexing_status = "ok"

        write_file(full_path, doc)
        return ProcessingResult(path=entry.path, reserved=doc.reserved)
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/processing.py tests/unit/test_processing.py
git commit -m "feat(processing): implement case 1 (added) with images and chunks"
```

---

## Task 18: 문서 처리 — Case 2 Modified + images 동기화

**Files:**
- Modify: `src/markdown_rag_pipeline/processing.py`
- Modify: `tests/unit/test_processing.py`

기존 doc_id 유지, content_hash + images 비교. 변경 있으면 Full replace + stale 정리. images 동기화는 본문 readonly.

- [ ] **Step 1: 추가 테스트**

```python
def test_modified_content_unchanged_no_milvus(repo):
    # 파일에 이미 doc_id + content_hash 기록된 상태
    body = "# Same\n\nBody unchanged.\n"
    existing_doc_id = "550e8400-e29b-41d4-a716-446655440000"
    existing_hash = "sha256:" + hashlib.sha256(body.encode()).hexdigest()
    (repo / "docs" / "a.md").write_text(
        f"---\ndoc_id: {existing_doc_id}\n"
        f"content_hash: \"{existing_hash}\"\n"
        f"chunk_count: 1\n"
        f"indexing_status: ok\n"
        f"images: []\n"
        f"---\n{body}"
    )
    milvus = MagicMock()
    # ...(중략) ctx 생성
    ctx = _build_ctx(repo, milvus)
    proc = DocProcessor(ctx)
    entry = DiffEntry(case=DiffCase.MODIFIED, path="docs/a.md")
    result = proc.process(entry)
    assert not milvus.upsert_chunks.called
    assert result.reserved.doc_id == existing_doc_id


def test_modified_content_changed_full_replace(repo):
    # 기존 상태
    old_body = "# Title\n\nOld body.\n"
    existing_doc_id = "550e8400-e29b-41d4-a716-446655440000"
    (repo / "docs" / "a.md").write_text(
        f"---\ndoc_id: {existing_doc_id}\n"
        f"content_hash: \"sha256:oldhash\"\n"
        f"chunk_count: 3\n"
        f"indexing_status: ok\n"
        f"images: []\n"
        f"---\n# Title\n\nNew body paragraph.\n"
    )
    milvus = MagicMock()
    milvus.existing_chunk_uids.return_value = [
        f"{existing_doc_id}:body:0", f"{existing_doc_id}:body:1",
        f"{existing_doc_id}:body:2",
    ]
    ctx = _build_ctx(repo, milvus)
    proc = DocProcessor(ctx)
    entry = DiffEntry(case=DiffCase.MODIFIED, path="docs/a.md")
    proc.process(entry)
    assert milvus.upsert_chunks.called
    assert milvus.cleanup_stale.called
    keep = milvus.cleanup_stale.call_args[0][1]
    assert all(u.startswith(existing_doc_id) for u in keep)
```

공용 헬퍼 `_build_ctx`는 test 파일 상단에 정의 (LLM/VLM/Embed 모의 객체 반환).

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `processing.py` 확장**

```python
class DocProcessor:
    # ...
    def process(self, entry: DiffEntry) -> ProcessingResult:
        if entry.case == DiffCase.ADDED:
            return self._process_added(entry)
        if entry.case == DiffCase.MODIFIED:
            return self._process_modified(entry)
        raise NotImplementedError(f"case {entry.case} not yet implemented")

    def _process_modified(self, entry: DiffEntry) -> ProcessingResult:
        full_path = self.ctx.repo_root / entry.path
        doc = parse_file(full_path)
        if is_rag_excluded(doc.user_meta):
            return self._process_exclude(entry, doc)

        if doc.reserved.doc_id is None:
            # 비정상: Added 경로 fallback
            return self._process_added(entry)

        new_hash = _sha256(doc.body)
        # 이미지 동기화 (본문 readonly)
        md_dir = str(full_path.parent.relative_to(self.ctx.repo_root))
        if md_dir == ".":
            md_dir = ""
        image_refs = extract_image_refs(doc.body, md_dir=md_dir)
        prev_by_path = {e.path: e for e in doc.reserved.images}
        new_image_entries: list[ImageEntry] = []
        images_changed = False
        for r in image_refs:
            img_path = self.ctx.repo_root / r.path
            if not img_path.exists():
                continue
            prev = prev_by_path.get(r.path)
            if prev and prev.section_path == r.section_path:
                new_image_entries.append(prev)
                continue
            images_changed = True
            caption = (
                prev.caption if prev
                else self.ctx.vlm.caption(img_path, self.ctx.vision_language_hint)
            )
            new_image_entries.append(ImageEntry(
                path=r.path, caption=caption, section_path=r.section_path,
            ))
        if {e.path for e in doc.reserved.images} != {e.path for e in new_image_entries}:
            images_changed = True
        doc.reserved.images = new_image_entries
        doc.reserved.has_images = bool(new_image_entries)

        content_changed = new_hash != (doc.reserved.content_hash or "")
        if not content_changed and not images_changed:
            # 예약 필드만 원복
            return ProcessingResult(path=entry.path, reserved=doc.reserved)

        # Full replace
        doc.reserved.content_hash = new_hash
        classification = self.ctx.llm.classify(body=doc.body)
        doc.reserved.summary = classification.summary
        doc.reserved.doc_type = classification.doc_type
        doc.reserved.topics = classification.topics
        doc.reserved.language = classification.language

        head = self.ctx.git.head_sha()
        doc.reserved.last_indexed_commit = head
        doc.reserved.last_indexed_at = _now_iso()
        doc.reserved.chunk_strategy_version = self.ctx.chunking_cfg.strategy_version

        # 제목 갱신
        for line in doc.body.splitlines():
            if line.strip().startswith("# "):
                doc.reserved.title = line.strip()[2:].strip()
                break
        doc.reserved.web_url = build_file_url(
            self.ctx.bitbucket_base, self.ctx.branch, entry.path
        )

        body_chunks = chunk_markdown(doc.body, self.ctx.chunking_cfg)
        img_chunk_texts = [
            f"{e.section_path}\n\n{e.caption}" for e in new_image_entries
        ]
        all_texts = [c.chunk_text for c in body_chunks] + img_chunk_texts
        if not all_texts:
            self.ctx.milvus.delete_by_doc(doc.reserved.doc_id)
            doc.reserved.chunk_count = 0
            doc.reserved.indexing_status = "ok"
            write_file(full_path, doc)
            return ProcessingResult(path=entry.path, reserved=doc.reserved)

        dense, sparse = self.ctx.embed.embed_batch(all_texts)
        chunks = self._build_chunks(
            doc.reserved.doc_id, entry.path, doc.reserved,
            body_chunks, new_image_entries, dense, sparse, doc.user_meta,
        )
        new_keep_uids = {c.chunk_uid for c in chunks}
        self.ctx.milvus.upsert_chunks(chunks)
        self.ctx.milvus.cleanup_stale(doc.reserved.doc_id, new_keep_uids)

        doc.reserved.chunk_count = len(chunks)
        doc.reserved.indexing_status = "ok"
        write_file(full_path, doc)
        return ProcessingResult(path=entry.path, reserved=doc.reserved)

    def _build_chunks(
        self,
        doc_id: str, path: str, reserved: Reserved,
        body_chunks: list, image_entries: list[ImageEntry],
        dense: list[list[float]], sparse: list[dict[int, float]],
        user_meta: dict,
    ) -> list[Chunk]:
        chunks: list[Chunk] = []
        now_ms = _now_ms()
        for i, bc in enumerate(body_chunks):
            chunks.append(Chunk(
                doc_id=doc_id, chunk_kind=ChunkKind.BODY, chunk_index=i,
                chunk_text=bc.chunk_text, breadcrumb=bc.breadcrumb,
                line_start=0, line_end=0,
                dense_vector=dense[i], sparse_vector=sparse[i],
                path=path, title=reserved.title or "",
                doc_type=reserved.doc_type or "",
                topics=reserved.topics, language=reserved.language or "",
                authors=reserved.authors,
                created_at_ms=0, updated_at_ms=0, last_indexed_at_ms=now_ms,
                has_images=reserved.has_images or False,
                source_url=reserved.web_url or "", user_meta=user_meta,
            ))
        offset = len(body_chunks)
        for j, e in enumerate(image_entries):
            chunks.append(Chunk(
                doc_id=doc_id, chunk_kind=ChunkKind.IMAGE, chunk_index=j,
                chunk_text=f"{e.section_path}\n\n{e.caption}",
                breadcrumb=e.section_path,
                line_start=0, line_end=0,
                dense_vector=dense[offset + j], sparse_vector=sparse[offset + j],
                path=path, title=reserved.title or "",
                doc_type=reserved.doc_type or "",
                topics=reserved.topics, language=reserved.language or "",
                authors=reserved.authors,
                created_at_ms=0, updated_at_ms=0, last_indexed_at_ms=now_ms,
                has_images=True,
                source_url=build_image_url(
                    self.ctx.bitbucket_base, self.ctx.branch, e.path
                ),
                user_meta=user_meta,
            ))
        return chunks

    def _process_exclude(self, entry: DiffEntry, doc: ParsedDoc) -> ProcessingResult:
        if doc.reserved.doc_id:
            self.ctx.milvus.delete_by_doc(doc.reserved.doc_id)
        doc.reserved.indexing_status = "excluded"
        doc.reserved.last_indexed_at = _now_iso()
        doc.reserved.last_indexed_commit = self.ctx.git.head_sha()
        write_file(self.ctx.repo_root / entry.path, doc)
        return ProcessingResult(path=entry.path, reserved=doc.reserved, skipped=True)
```

Case 1의 인라인 청크 빌드 로직도 `_build_chunks`로 리팩터링.

- [ ] **Step 4: 테스트 실행 — 통과 확인**

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/processing.py tests/unit/test_processing.py
git commit -m "feat(processing): implement case 2 (modified) with images sync"
```

---

## Task 19: 문서 처리 — Case 3 Deleted + Exclude 전환

**Files:**
- Modify: `src/markdown_rag_pipeline/processing.py`
- Modify: `tests/unit/test_processing.py`

Exclude는 이미 Task 18 `_process_exclude`에 있으므로 Deleted만 추가.

- [ ] **Step 1: 추가 테스트**

```python
def test_deleted_retrieves_prior_doc_id_from_git(repo):
    git = MagicMock()
    git.show_file_at.return_value = (
        "---\n"
        "doc_id: 550e8400-e29b-41d4-a716-446655440000\n"
        "---\n"
        "# del\n"
    )
    milvus = MagicMock()
    ctx = _build_ctx(repo, milvus, git_override=git)
    ctx.last_indexed_commit = "a1b2c3d"  # orchestrator 주입
    proc = DocProcessor(ctx)
    entry = DiffEntry(case=DiffCase.DELETED, path="docs/gone.md")
    proc.process(entry)
    assert milvus.delete_by_doc.called
    milvus.delete_by_doc.assert_called_with("550e8400-e29b-41d4-a716-446655440000")


def test_deleted_missing_doc_id_skips(repo):
    git = MagicMock()
    git.show_file_at.return_value = "# bare\n"  # 프론트매터 없음
    milvus = MagicMock()
    ctx = _build_ctx(repo, milvus, git_override=git)
    ctx.last_indexed_commit = "a1b2c3d"
    proc = DocProcessor(ctx)
    entry = DiffEntry(case=DiffCase.DELETED, path="docs/noid.md")
    result = proc.process(entry)
    assert result.skipped is True
    assert not milvus.delete_by_doc.called
```

`ProcessingContext`에 `last_indexed_commit: Optional[str] = None` 필드 추가.

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `processing.py` 확장**

```python
@dataclass
class ProcessingContext:
    # 기존 필드 + 추가
    last_indexed_commit: Optional[str] = None


class DocProcessor:
    def process(self, entry: DiffEntry) -> ProcessingResult:
        if entry.case == DiffCase.ADDED:
            return self._process_added(entry)
        if entry.case == DiffCase.MODIFIED:
            return self._process_modified(entry)
        if entry.case == DiffCase.DELETED:
            return self._process_deleted(entry)
        raise NotImplementedError(f"case {entry.case} not yet implemented")

    def _process_deleted(self, entry: DiffEntry) -> ProcessingResult:
        if not self.ctx.last_indexed_commit:
            return ProcessingResult(path=entry.path, skipped=True)
        try:
            old_content = self.ctx.git.show_file_at(
                self.ctx.last_indexed_commit, entry.path
            )
        except RuntimeError:
            return ProcessingResult(path=entry.path, skipped=True)

        from io import StringIO
        import frontmatter as fm
        post = fm.load(StringIO(old_content))
        doc_id = post.metadata.get("doc_id")
        if not doc_id:
            return ProcessingResult(path=entry.path, skipped=True)
        if post.metadata.get("rag_exclude"):
            return ProcessingResult(path=entry.path, skipped=True)

        self.ctx.milvus.delete_by_doc(doc_id)
        return ProcessingResult(path=entry.path)
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/processing.py tests/unit/test_processing.py
git commit -m "feat(processing): implement case 3 (deleted) via prior commit lookup"
```

---

## Task 20: 오케스트레이터 (`orchestrator.py`)

**Files:**
- Create: `src/markdown_rag_pipeline/orchestrator.py`
- Create: `tests/integration/test_orchestrator.py`

`indexer_dag`의 런타임 엔트리. target_set 구성(diff + failed 재시도 + 이미지 cascade), 각 문서 처리, 실패 격리, `.pipeline/state.json` 갱신, 봇 커밋 push.

- [ ] **Step 1: 실패 테스트 (통합, git 임시 레포 사용)**

```python
# tests/integration/test_orchestrator.py
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from markdown_rag_pipeline.clients.llm import Classification
from markdown_rag_pipeline.orchestrator import Orchestrator, OrchestratorDeps


def _init_git(path: Path):
    subprocess.check_call(["git", "init", "-b", "main"], cwd=path)
    subprocess.check_call(["git", "config", "user.email", "t@t"], cwd=path)
    subprocess.check_call(["git", "config", "user.name", "t"], cwd=path)


def _commit(path: Path, rel: str, content: str, msg: str):
    (path / rel).parent.mkdir(parents=True, exist_ok=True)
    (path / rel).write_text(content)
    subprocess.check_call(["git", "add", "-A"], cwd=path)
    subprocess.check_call(["git", "commit", "-m", msg], cwd=path)


def test_orchestrator_first_run_indexes_all(tmp_path: Path):
    _init_git(tmp_path)
    _commit(tmp_path, "docs/a.md", "# A\nbody a", "init a")
    _commit(tmp_path, "docs/b.md", "# B\nbody b", "init b")

    llm = MagicMock()
    llm.classify.return_value = Classification(
        summary="s", doc_type="reference", topics=["t"], language="ko"
    )
    vlm = MagicMock()
    embed = MagicMock()
    embed.embed_batch.side_effect = lambda texts: (
        [[0.1] * 4 for _ in texts], [{} for _ in texts]
    )
    milvus = MagicMock()

    deps = OrchestratorDeps(
        repo_root=tmp_path, llm=llm, vlm=vlm, embed=embed, milvus=milvus,
        bitbucket_base="https://bb/r", branch="main",
        chunking_cfg=_cfg(), dense_dim=4,
        bot_author_name="docs-bot", bot_author_email="b@b",
        bot_commit_tag="[pipeline-auto]",
        push_max_retries=1, push_backoff_initial=1, push_backoff_max=10, push_backoff_jitter=0.0,
        vision_language_hint="ko",
    )
    o = Orchestrator(deps)
    o._push_enabled = False  # 테스트 환경: 원격 없음
    result = o.run_once()
    assert result.processed_count == 2
    assert milvus.upsert_chunks.call_count == 2
```

`_cfg()`는 유닛 테스트의 `ChunkingCfg` 생성 헬퍼.

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `orchestrator.py` 구현**

```python
"""파이프라인 오케스트레이터."""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .bot_commit import BotCommitter, BotConfig
from .chunking import ChunkingCfg
from .clients.embedding import BgeM3Client
from .clients.llm import LLMClient
from .clients.vlm import VlmClient
from .frontmatter import parse_file, write_file
from .git_adapter import GitAdapter
from .image_ref import find_referring_mds
from .milvus_adapter import MilvusAdapter
from .models import DiffCase, DiffEntry
from .processing import DocProcessor, ProcessingContext, ProcessingResult
from .state import PipelineState, read_state, write_state

log = logging.getLogger(__name__)


@dataclass
class OrchestratorDeps:
    repo_root: Path
    llm: LLMClient
    vlm: VlmClient
    embed: BgeM3Client
    milvus: MilvusAdapter
    bitbucket_base: str
    branch: str
    chunking_cfg: ChunkingCfg
    dense_dim: int
    bot_author_name: str
    bot_author_email: str
    bot_commit_tag: str
    push_max_retries: int
    push_backoff_initial: int
    push_backoff_max: int
    push_backoff_jitter: float
    vision_language_hint: str
    embedding_model_name: str = ""


@dataclass
class OrchestratorRunResult:
    processed_count: int
    failed_count: int
    skipped_count: int


_IMAGE_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg")


class Orchestrator:
    def __init__(self, deps: OrchestratorDeps):
        self.deps = deps
        self.git = GitAdapter(deps.repo_root)
        self._push_enabled = True

    def run_once(self) -> OrchestratorRunResult:
        state = read_state(self.deps.repo_root)
        last_commit = state.last_indexed_commit if state else None

        if last_commit:
            try:
                self.git.fetch()
            except RuntimeError:
                log.warning("git fetch failed")

        head = self.git.head_sha("origin/main") if last_commit else self.git.head_sha()

        diffs = self._compute_diffs(last_commit, head)
        failed_paths = self._collect_failed_paths()
        target_paths = self._build_target_set(diffs, failed_paths)

        ctx = ProcessingContext(
            repo_root=self.deps.repo_root,
            git=self.git, llm=self.deps.llm, vlm=self.deps.vlm,
            embed=self.deps.embed, milvus=self.deps.milvus,
            bitbucket_base=self.deps.bitbucket_base, branch=self.deps.branch,
            chunking_cfg=self.deps.chunking_cfg,
            vision_language_hint=self.deps.vision_language_hint,
            last_indexed_commit=last_commit,
        )
        proc = DocProcessor(ctx)

        results: list[ProcessingResult] = []
        processed_paths: list[str] = []
        for entry in target_paths:
            try:
                r = proc.process(entry)
                results.append(r)
                if not r.skipped and not r.failed:
                    processed_paths.append(entry.path)
            except Exception as e:
                log.exception("doc failed: %s", entry.path)
                self._mark_failed(entry.path, str(e))
                results.append(ProcessingResult(
                    path=entry.path, failed=True, error=str(e)
                ))

        new_state = PipelineState(
            schema_version=1,
            last_indexed_commit=head,
            last_indexed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            pipeline_schema_version=1,
            chunk_strategy_version=self.deps.chunking_cfg.strategy_version,
            embedding_model=self.deps.embedding_model_name,
            vision_schema_version=1,
        )
        write_state(self.deps.repo_root, new_state)

        if self._push_enabled:
            head_short = head[:7]
            bot = BotCommitter(
                self.git,
                BotConfig(
                    author_name=self.deps.bot_author_name,
                    author_email=self.deps.bot_author_email,
                    commit_tag=self.deps.bot_commit_tag,
                    push_max_retries=self.deps.push_max_retries,
                    push_backoff_initial_sec=self.deps.push_backoff_initial,
                    push_backoff_max_sec=self.deps.push_backoff_max,
                    push_backoff_jitter=self.deps.push_backoff_jitter,
                ),
            )
            bot.compose_and_push(
                changed_paths=processed_paths,
                state_file_path=".pipeline/state.json",
                head_sha_short=head_short,
                n_files=len(processed_paths),
            )

        return OrchestratorRunResult(
            processed_count=sum(1 for r in results if not r.failed and not r.skipped),
            failed_count=sum(1 for r in results if r.failed),
            skipped_count=sum(1 for r in results if r.skipped),
        )

    def _compute_diffs(
        self, last_commit: Optional[str], head: str
    ) -> list[DiffEntry]:
        if not last_commit:
            # 최초 실행: 모든 .md를 Added로
            return [DiffEntry(case=DiffCase.ADDED, path=p)
                    for p in self.git.list_tracked("*.md")]
        return self.git.diff(last_commit, head)

    def _collect_failed_paths(self) -> list[str]:
        failed: list[str] = []
        for md in self.deps.repo_root.rglob("*.md"):
            rel = str(md.relative_to(self.deps.repo_root))
            if rel.startswith(".pipeline/"):
                continue
            try:
                doc = parse_file(md)
            except Exception:
                continue
            if doc.reserved.indexing_status == "failed":
                failed.append(rel)
        return failed

    def _build_target_set(
        self, diffs: list[DiffEntry], failed_paths: list[str]
    ) -> list[DiffEntry]:
        seen: dict[str, DiffEntry] = {}

        def add(entry: DiffEntry):
            key = entry.path
            if key not in seen:
                seen[key] = entry

        for d in diffs:
            add(d)
            # 이미지 파일 변경 → 참조 md cascade
            if d.path.lower().endswith(_IMAGE_EXT) and d.case in (
                DiffCase.ADDED, DiffCase.MODIFIED, DiffCase.DELETED
            ):
                for md_path in find_referring_mds(self.deps.repo_root, d.path):
                    rel = str(md_path.relative_to(self.deps.repo_root))
                    add(DiffEntry(case=DiffCase.MODIFIED, path=rel))

        for p in failed_paths:
            add(DiffEntry(case=DiffCase.MODIFIED, path=p))

        # .pipeline/ 자체 및 비-.md 제외 (.md 변경만 처리)
        return [
            e for e in seen.values()
            if e.path.endswith((".md", ".markdown"))
            and not e.path.startswith(".pipeline/")
        ]

    def _mark_failed(self, path: str, error: str) -> None:
        full = self.deps.repo_root / path
        if not full.exists():
            return
        try:
            doc = parse_file(full)
            doc.reserved.indexing_status = "failed"
            write_file(full, doc)
        except Exception:
            log.exception("failed to mark %s as failed", path)
```

- [ ] **Step 4: 테스트 실행 — 통과 확인**

- [ ] **Step 5: 커밋**

```bash
git add src/markdown_rag_pipeline/orchestrator.py tests/integration/test_orchestrator.py
git commit -m "feat(orchestrator): add target_set, failure isolation, state update"
```

---

## Task 21: Airflow DAG (`dags/indexer_dag.py`)

**Files:**
- Create: `dags/indexer_dag.py`
- Create: `tests/integration/test_indexer_dag.py`

DAG는 얇은 래퍼. `Orchestrator.run_once()` 호출.

- [ ] **Step 1: DAG 테스트 (DAG 로드/파싱 정상 확인)**

```python
import pytest

pytest.importorskip("airflow")

from airflow.models import DagBag


def test_dag_loads():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert "indexer_dag" in bag.dags
    dag = bag.dags["indexer_dag"]
    assert dag.schedule_interval == "*/3 * * * *"
    assert dag.max_active_runs == 1
```

- [ ] **Step 2: 테스트 실행 — 실패 확인**

- [ ] **Step 3: `dags/indexer_dag.py` 구현**

```python
"""indexer_dag — 주기 폴링으로 Milvus 인덱스 동기화."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from markdown_rag_pipeline.clients.embedding import BgeM3Client
from markdown_rag_pipeline.clients.llm import LLMClient
from markdown_rag_pipeline.clients.vlm import VlmClient
from markdown_rag_pipeline.config import load_config
from markdown_rag_pipeline.milvus_adapter import MilvusAdapter
from markdown_rag_pipeline.orchestrator import Orchestrator, OrchestratorDeps


CONFIG_PATH = os.environ.get("PIPELINE_CONFIG_PATH", "/etc/pipeline/pipeline-config.yaml")


def _run_orchestrator():
    cfg = load_config(Path(CONFIG_PATH))

    llm = LLMClient.from_prompt_file(
        cfg.llm.endpoint, cfg.llm.model_name,
        Path("prompts/llm-classify.md"),
    )
    vlm = VlmClient.from_prompt_file(
        cfg.vision.caption_endpoint, cfg.vision.caption_model,
        Path(cfg.vision.prompt_path),
        cfg.vision.caption_min_chars, cfg.vision.caption_max_chars,
        cfg.vision.cache_by_image_hash,
    )
    embed = BgeM3Client(
        endpoint=cfg.embedding.endpoint, dense_dim=cfg.embedding.dense_dim,
    )
    milvus = MilvusAdapter(
        host=cfg.milvus.host, port=cfg.milvus.port,
        collection_name=cfg.milvus.collection_name,
        dense_dim=cfg.embedding.dense_dim,
    )
    milvus.connect()
    milvus.ensure_collection()

    from markdown_rag_pipeline.chunking import ChunkingCfg
    chunking_cfg = ChunkingCfg(
        headers_to_split_on=cfg.chunking.headers_to_split_on,
        chunk_size=cfg.chunking.chunk_size,
        chunk_overlap=cfg.chunking.chunk_overlap,
        strategy_version=cfg.chunking.strategy_version,
    )

    deps = OrchestratorDeps(
        repo_root=Path(cfg.repo.local_workdir),
        llm=llm, vlm=vlm, embed=embed, milvus=milvus,
        bitbucket_base=cfg.bitbucket.web_url_base,
        branch=cfg.repo.branch,
        chunking_cfg=chunking_cfg,
        dense_dim=cfg.embedding.dense_dim,
        bot_author_name=cfg.bot.git_author_name,
        bot_author_email=cfg.bot.git_author_email,
        bot_commit_tag=cfg.bot.commit_tag,
        push_max_retries=cfg.bot.push_max_retries,
        push_backoff_initial=cfg.bot.push_backoff_initial_sec,
        push_backoff_max=cfg.bot.push_backoff_max_sec,
        push_backoff_jitter=cfg.bot.push_backoff_jitter,
        vision_language_hint="ko",
        embedding_model_name=cfg.embedding.model_name,
    )
    Orchestrator(deps).run_once()


with DAG(
    dag_id="indexer_dag",
    start_date=datetime(2026, 4, 19, tzinfo=timezone.utc),
    schedule_interval="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
) as dag:
    PythonOperator(
        task_id="run_indexer",
        python_callable=_run_orchestrator,
    )
```

- [ ] **Step 4: 테스트 실행 — 통과 확인 (Airflow 환경)**

- [ ] **Step 5: 커밋**

```bash
git add dags/indexer_dag.py tests/integration/test_indexer_dag.py
git commit -m "feat(airflow): add indexer_dag as thin orchestrator wrapper"
```

---

## Task 22: 엔드투엔드 통합 테스트

**Files:**
- Create: `tests/integration/test_e2e.py`

실제 로컬 git 레포 + Mock LLM/VLM/Embed + 실제 Milvus (환경 변수 필요)로 엔드투엔드 시나리오를 검증한다.

시나리오:
1. 레포 초기화, `docs/a.md`, `docs/b.md` 커밋.
2. Orchestrator.run_once() → Milvus에 2개 문서 청크 들어감, 프론트매터에 `doc_id` 주입됨.
3. `docs/a.md` 본문 수정 후 커밋 → run_once() → chunk 재적재, stale 정리.
4. `docs/b.md` 삭제 커밋 → run_once() → Milvus에서 b.md 청크 사라짐.
5. `docs/a.md`에 `rag_exclude: true` 추가 커밋 → run_once() → a.md 청크도 사라짐.

- [ ] **Step 1: 테스트 작성**

```python
import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pymilvus import connections, utility

from markdown_rag_pipeline.clients.llm import Classification
from markdown_rag_pipeline.chunking import ChunkingCfg
from markdown_rag_pipeline.milvus_adapter import MilvusAdapter
from markdown_rag_pipeline.orchestrator import Orchestrator, OrchestratorDeps


pytestmark = pytest.mark.skipif(
    not os.environ.get("MILVUS_TEST_HOST"),
    reason="MILVUS_TEST_HOST not set",
)


def _git(path: Path, *args: str):
    subprocess.check_call(["git", *args], cwd=path)


def test_e2e_lifecycle(tmp_path: Path):
    _git(tmp_path, "init", "-b", "main")
    _git(tmp_path, "config", "user.email", "t@t")
    _git(tmp_path, "config", "user.name", "t")

    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "a.md").write_text("# A\nBody a.\n")
    (tmp_path / "docs" / "b.md").write_text("# B\nBody b.\n")
    _git(tmp_path, "add", "-A")
    _git(tmp_path, "commit", "-m", "init")

    llm = MagicMock()
    llm.classify.return_value = Classification(
        summary="s", doc_type="reference", topics=["t"], language="ko"
    )
    vlm = MagicMock()
    embed = MagicMock()
    embed.embed_batch.side_effect = lambda texts: (
        [[0.1] * 8 for _ in texts], [{} for _ in texts]
    )

    milvus_host = os.environ["MILVUS_TEST_HOST"]
    coll_name = f"e2e_{os.getpid()}"
    milvus = MilvusAdapter(
        host=milvus_host, port=19530,
        collection_name=coll_name, dense_dim=8,
    )
    milvus.connect()
    milvus.ensure_collection()

    try:
        deps = OrchestratorDeps(
            repo_root=tmp_path, llm=llm, vlm=vlm, embed=embed, milvus=milvus,
            bitbucket_base="https://bb/r", branch="main",
            chunking_cfg=ChunkingCfg(
                headers_to_split_on=[["#", "H1"]],
                chunk_size=100, chunk_overlap=10, strategy_version=1,
            ),
            dense_dim=8, bot_author_name="b", bot_author_email="b@b",
            bot_commit_tag="[pipeline-auto]",
            push_max_retries=1, push_backoff_initial=1,
            push_backoff_max=2, push_backoff_jitter=0.0,
            vision_language_hint="ko",
        )
        o = Orchestrator(deps)
        o._push_enabled = False

        # 1: 최초 인덱싱
        r1 = o.run_once()
        assert r1.processed_count == 2
        milvus.flush()
        # 확인: doc_id가 프론트매터에 주입되어 있음
        from markdown_rag_pipeline.frontmatter import parse_file
        a_doc = parse_file(tmp_path / "docs/a.md")
        b_doc = parse_file(tmp_path / "docs/b.md")
        assert a_doc.reserved.doc_id and b_doc.reserved.doc_id

        # 커밋 (Orchestrator._push_enabled=False이므로 직접 커밋)
        _git(tmp_path, "add", "-A")
        _git(tmp_path, "commit", "-m", "auto-frontmatter")

        # 2: a.md 본문 수정
        (tmp_path / "docs/a.md").write_text(
            a_doc.body.replace("Body a.", "Body a v2.")
        )
        # 프론트매터 유지 필요 → 재쓰기
        from markdown_rag_pipeline.frontmatter import write_file
        a_doc.body = a_doc.body.replace("Body a.", "Body a v2.")
        write_file(tmp_path / "docs/a.md", a_doc)
        _git(tmp_path, "add", "-A")
        _git(tmp_path, "commit", "-m", "update a")

        r2 = o.run_once()
        assert r2.processed_count == 1
        milvus.flush()

        # 3: b.md 삭제
        (tmp_path / "docs/b.md").unlink()
        _git(tmp_path, "add", "-A")
        _git(tmp_path, "commit", "-m", "delete b")
        r3 = o.run_once()
        milvus.flush()
        assert milvus.existing_chunk_uids(b_doc.reserved.doc_id) == []

        # 4: a.md에 rag_exclude:true 추가
        a_doc2 = parse_file(tmp_path / "docs/a.md")
        a_doc2.user_meta["rag_exclude"] = True
        write_file(tmp_path / "docs/a.md", a_doc2)
        _git(tmp_path, "add", "-A")
        _git(tmp_path, "commit", "-m", "exclude a")
        r4 = o.run_once()
        milvus.flush()
        assert milvus.existing_chunk_uids(a_doc.reserved.doc_id) == []
    finally:
        connections.connect(host=milvus_host, port=19530)
        if utility.has_collection(coll_name):
            utility.drop_collection(coll_name)
```

- [ ] **Step 2: 테스트 실행 — Milvus 환경에서 pass, 아니면 skip**

- [ ] **Step 3: 커밋**

```bash
git add tests/integration/test_e2e.py
git commit -m "test(e2e): add orchestrator lifecycle test (add/modify/delete/exclude)"
```

---

## Self-Review 체크리스트

플랜 완료 후 자체 점검:

1. **Spec 커버리지**:
   - 섹션 3 (프론트매터 예약 필드) → Task 4에서 구현.
   - 섹션 5 (Milvus 스키마) → Task 14/15.
   - 섹션 6 (파이프라인 아키텍처) → Task 20/21.
   - 섹션 7 (변경 처리 흐름) → Task 17/18/19/20.
   - 섹션 8 (봇 Git 전략) → Task 16.
   - 섹션 9 (이미지 처리) → Task 9/10/13 + Task 17/18 통합.
   - 섹션 11 (시크릿): SSH key 주입은 운영 설정 — Plan 1 코드 범위 외, 운영 매뉴얼에 위임.
   - 섹션 12 (관찰성): 메트릭/알림은 Plan 4로 분리.
   - 섹션 13 (SLO 측정): Plan 4로 분리.
   - **Case 4 (Renamed content 동일)**: Plan 2로 분리 (명시적으로 MVP Full replace로 대체).
   - **Case 5 (Renamed + content 변경)**: Plan 2로 분리.
   - **CLI (drift-check, backfill)**: Plan 3으로 분리.
   - **CI 린터**: Plan 3으로 분리.

2. **Placeholder 점검**: "TBD" 등 없음.

3. **Type 일관성**: `Chunk`, `ImageRef`, `DiffEntry`, `Reserved`, `ImageEntry`, `ProcessingContext` 등 데이터 모델 Task 2/4에서 정의 → 이후 일관 사용. `chunk_uid` 형식 `{doc_id}:body|image:{i}` 일관.

---

## 실행 참고

- **외부 서비스 의존**: BGE-M3/LLM/VLM/Milvus가 테스트 가능한 로컬 엔드포인트로 올라가 있다는 가정. 통합 테스트는 Milvus 환경 변수 `MILVUS_TEST_HOST` 설정 시 실행.
- **Airflow 환경**: Task 21 테스트는 airflow 패키지가 없으면 skip. 실제 배포 시 사내 Airflow 클러스터에 `dags/` 디렉토리 연결.
- **커밋 빈도**: 각 Task는 1-2개 커밋. TDD 사이클을 엄격 준수 (test → fail → impl → pass → commit).
