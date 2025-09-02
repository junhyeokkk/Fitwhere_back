#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sys, json, argparse, hashlib, datetime as dt
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import os, logging
from dataclasses import dataclass
from pathlib import Path
import os, mimetypes 
from decimal import Decimal, InvalidOperation
from datetime import datetime


# =========================
# Config
# =========================

LOADED_ENV_FROM = None
DOTENV_OVERRIDE = os.getenv("DOTENV_OVERRIDE", "false").strip().lower() in ("1","true","yes") #.env 값으로 OS 값을 덮어쓰고 싶다면 실행 시 DOTENV_OVERRIDE=true

try:
    from dotenv import load_dotenv, find_dotenv  # pip install python-dotenv

    env_file = os.getenv("ENV_FILE")  # 절대경로 권장: /opt/app/config/prod.env
    if env_file and Path(env_file).exists():
        load_dotenv(env_file, override=DOTENV_OVERRIDE)
        LOADED_ENV_FROM = env_file
    else:
        # 1) etl_pipeline.py와 같은 폴더의 .env
        local_env = Path(__file__).resolve().parent / ".env"
        if local_env.exists():
            load_dotenv(str(local_env), override=DOTENV_OVERRIDE)
            LOADED_ENV_FROM = str(local_env)
        else:
            # 2) 현재 작업 디렉터리(CWD)에서 위로 탐색
            found = find_dotenv(usecwd=True)
            if found:
                load_dotenv(found, override=DOTENV_OVERRIDE)
                LOADED_ENV_FROM = found
except Exception:
    # python-dotenv 미설치 등은 조용히 패스 (운영 환경변수만 사용)
    LOADED_ENV_FROM = None


def _env(name: str, default=None):
    v= os.getenv(name,default)
    return None if v=="" else v

def _env_int(name:str, default: int) -> int:
    try:
        return int(os.getenv(name,default))
    except Exception:
        return default
    
def _env_bool(name:str, default: bool) -> bool:
    v= os.getenv(name)
    if v is None: return default
    return v.strip().lower() in ("1","true","t","y","yes")



@dataclass(frozen=True)
class Config:
    # DB
    pg_host: str
    pg_port: int
    pg_db: str
    pg_user: str
    pg_pass: str

    # Storage
    storage_backend: str          # 'local' | 's3'
    s3_bucket: str | None
    s3_region: str | None
    s3_prefix_raw: str | None     # 예: 'raw/'
    s3_prefix_processed: str | None  # 예: 'processed/'

    # Local staging
    local_root: str               # 로컬 업로드 보관 루트

    # Tables (스키마명 포함 가능)
    tbl_stage_file: str
    tbl_stage_raw: str
    tbl_org: str
    tbl_facility: str
    tbl_facility_org: str

    # ETL
    batch_size: int
    dry_run: bool
    log_level: str

    def pg_dsn(self) -> dict:
        return {
            "host": self.pg_host,
            "port": self.pg_port,
            "dbname": self.pg_db,
            "user": self.pg_user,
            "password": self.pg_pass,
        }


def load_config() -> Config:
    cfg = Config(
        # DB
        pg_host = _env("PGHOST", "localhost"),
        pg_port = _env_int("PGPORT", 5432),
        pg_db   = _env("PGDATABASE", "fitwhere"),
        pg_user = _env("PGUSER", "fitwhere"),
        pg_pass = _env("PGPASSWORD", "fitwhere"),

        # Storage
        storage_backend = _env("STORAGE_BACKEND", "local").lower(),  # 'local' | 's3'
        s3_bucket = _env("S3_BUCKET", None),
        s3_region = _env("S3_REGION", "ap-northeast-2"),
        s3_prefix_raw = _env("S3_PREFIX_RAW", "raw/"),
        s3_prefix_processed = _env("S3_PREFIX_PROCESSED", "processed/"),

        # Local staging
        local_root = _env("LOCAL_STAGE_ROOT", "./stage_uploads"),

        # Tables (필요시 스키마 접두어 포함)
        tbl_stage_file = _env("TBL_STAGE_FILE", "stage_file"),
        tbl_stage_raw  = _env("TBL_STAGE_RAW", "stage_sfms_facility_raw"),
        tbl_org        = _env("TBL_ORG", "org"),
        tbl_facility   = _env("TBL_FACILITY", "sfms_facility"),
        tbl_facility_org = _env("TBL_FACILITY_ORG", "facility_org"),

        # ETL
        batch_size = _env_int("BATCH_SIZE", 1000),
        dry_run    = _env_bool("DRY_RUN", False),
        log_level  = _env("LOG_LEVEL", "INFO").upper(),
    )

    # 기본 유효성
    if cfg.storage_backend not in ("local","s3"):
        raise ValueError("STORAGE_BACKEND must be 'local' or 's3'")

    if cfg.storage_backend == "s3" and not cfg.s3_bucket:
        raise ValueError("S3 backend requires S3_BUCKET")

    # 로깅 세팅
    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s"
    )

    # 로컬 폴더 준비
    if cfg.storage_backend == "local":
        os.makedirs(cfg.local_root, exist_ok=True)

    return cfg



# 전역에서 한 번 로드해서 사용
CFG = load_config()
PG = CFG.pg_dsn()
BATCH_SIZE = CFG.batch_size


import logging
masked = (CFG.pg_pass[:2] + "****") if CFG.pg_pass else None
logging.info("CFG loaded from=%s host=%s db=%s user=%s pass=%s backend=%s",
             LOADED_ENV_FROM, CFG.pg_host, CFG.pg_db, CFG.pg_user, masked, CFG.storage_backend)


def _s(x):
    """안전한 문자열화 + trim"""
    return ("" if x is None else str(x)).strip()

def _adm10(x):
    """행정코드(10자리)만 통과, 아니면 None"""
    s = _s(x)
    return s if len(s) == 10 else None

def _f(x):
    """float 변환: '', 'None', 'nan' 등은 None 처리"""
    s = _s(x)
    if s == "" or s.lower() in ("none", "nan", "null"):
        return None
    try:
        return float(s)
    except Exception:
        return None

def yn_bool(x, default=None):
    """'Y'/'N', true/false/1/0 문자열을 bool로"""
    s = _s(x).lower()
    if s in ("y", "yes", "true", "t", "1"):
        return True
    if s in ("n", "no", "false", "f", "0"):
        return False
    return default

def iter_stage_rows_with_meta(file_id: int, itersize: int | None = None):
    """
    stage_sfms_facility_raw에서 (payload, row_hash)를 스트리밍으로 순회.
    - server-side cursor 사용 (메모리 절약)
    - payload가 str이면 json.loads로 dict 변환
    - 테이블명/배치크기는 CFG 사용
    """
    itersize = itersize or BATCH_SIZE
    sql = f"""
        SELECT payload, row_hash
        FROM {CFG.tbl_stage_raw}
        WHERE file_id = %s
        ORDER BY import_id
    """
    conn = psycopg2.connect(**PG)
    try:
        # 서버사이드 커서(이름 있는 커서) → 대량 데이터 스트리밍
        cur = conn.cursor(name="stage_stream_cursor")
        cur.itersize = itersize
        cur.execute(sql, (file_id,))
        for payload, row_hash in cur:
            if isinstance(payload, str):
                try:
                    payload = json.loads(payload)
                except Exception:
                    # 이미 dict/jsonb 어댑터로 들어온 경우도 있으니 조용히 패스
                    pass
            yield payload, row_hash
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def upsert_facilities_from_stage(file_id: int) -> int:
    """
    stage -> sfms_facility 업서트
    - BATCH_SIZE 단위로 execute_batch
    - DRY_RUN이면 DB 쓰기 생략하고 로그만
    - last_file_id / last_row_hash 포함 (to_facility_tuple에서 세팅)
    """
    total = 0
    buffer = []
    for payload, row_hash in iter_stage_rows_with_meta(file_id, BATCH_SIZE):
        buffer.append(to_facility_tuple(payload, file_id, row_hash))
        if len(buffer) >= BATCH_SIZE:
            if CFG.dry_run:
                logging.info("[DRY_RUN] would upsert %d facilities into %s", len(buffer), CFG.tbl_facility)
            else:
                with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
                    execute_batch(cur, FACILITY_UPSERT_SQL, buffer, page_size=BATCH_SIZE)
                    conn.commit()
            total += len(buffer)
            buffer.clear()

    # 잔여분 flush
    if buffer:
        if CFG.dry_run:
            logging.info("[DRY_RUN] would upsert %d facilities into %s", len(buffer), CFG.tbl_facility)
        else:
            with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
                execute_batch(cur, FACILITY_UPSERT_SQL, buffer, page_size=BATCH_SIZE)
                conn.commit()
        total += len(buffer)
        buffer.clear()

    logging.info("facility upserted: %d rows (file_id=%s)", total, file_id)
    return total


# =========================
# Utils
# =========================
def _s(x): return ("" if x is None else str(x)).strip()
def _adm10(x):
    s = _s(x)
    return s if len(s) == 10 else None
def canon_name(name: str|None) -> str|None:
    s = _s(name)
    s = re.sub(r'[\s\\(\\)\\[\\]\\{\\}·ㆍ・•\\.,-]+', '', s).upper()
    return s or None
def derive_ctp_from_sgg(sgg: str|None) -> str|None:
    s = _s(sgg)
    return (s[:2] + "00000000") if len(s) == 10 else None

def sha1_json(d: dict) -> str:
    j = json.dumps(d, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(j.encode("utf-8")).hexdigest()

def facility_key(row: dict) -> str:
    # 이름 + 주소측 표준코드 + 소유측 코드로 안정화 (원하면 규칙 변경 OK)
    name = _s(row.get("FCLTY_NM")).upper()
    owner_adm = _s(row.get("POSESN_MBY_SIGNGU_CD") or row.get("POSESN_MBY_CTPRVN_CD"))
    addr_adm  = _s(row.get("ROAD_NM_EMD_CD") or row.get("ROAD_NM_SIGNGU_CD") or row.get("ROAD_NM_CTPRVN_CD"))
    raw = "|".join([name, owner_adm, addr_adm])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

# =========================
# Storage (local now, S3 later)
# =========================
def read_excel_any(path_or_uri: str) -> pd.DataFrame:
    """
    현재는 local 파일 경로 사용.
    S3 준비 후에는 s3fs 사용: pandas.read_excel("s3://bucket/key", storage_options=...)
    """
    if CFG.storage_backend == "s3":
        try:
            import s3fs  # noqa
            return pd.read_excel(
                path_or_uri, dtype=str,
                storage_options={"client_kwargs": {"region_name": S3_REGION}}
            )
        except Exception as e:
            raise RuntimeError("S3 backend 사용 전: pip install s3fs openpyxl 필요") from e
    # local
    return pd.read_excel(path_or_uri, dtype=str)


def _md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def _guess_ct(path: str) -> str:
    ct, _ = mimetypes.guess_type(path)
    return ct or "application/octet-stream"

def detect_dataset(df: pd.DataFrame) -> str:
    cols = {c.upper() for c in df.columns}
    if {"PROGRM_NM","PROGRM_TY_NM"} & cols: return "program"
    if {"FCLTY_NM","FCLTY_TY_NM"} & cols:  return "facility"
    return "unknown"

def stage_local_file_record(local_path: str,dataset: str) -> int:
    """
    로컬 파일을 stage_file에 등록(UPSERT).
    같은 경로/버전이면 기존 row를 갱신하고 file_id를 재사용.
    """
    meta = json.dumps({"dataset":dataset},ensure_ascii=False)
    s3_bucket = "local"
    s3_key = os.path.abspath(local_path)
    size = os.path.getsize(local_path)
    etag = _md5(local_path)                 # 로컬 파일 체크섬
    content_type = _guess_ct(local_path)
    uploaded_by = os.getenv("USER") or os.getenv("USERNAME") or "system"

    sql = f"""
        INSERT INTO {CFG.tbl_stage_file}
        (s3_bucket, s3_key, version_id, etag, size_bytes, content_type, uploaded_by, status,metadata)
        VALUES
        (%s,        %s,     NULL,       %s,   %s,         %s,           %s,         'PENDING',%s)
        ON CONFLICT (s3_bucket, s3_key)
        DO UPDATE SET
        etag        = EXCLUDED.etag,
        size_bytes  = EXCLUDED.size_bytes,
        content_type= EXCLUDED.content_type,
        uploaded_by = EXCLUDED.uploaded_by,
        uploaded_at = now(),
        status      = 'PENDING',
        metadata=EXCLUDED.metadata
        RETURNING file_id;
        """
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(sql, (s3_bucket, s3_key, etag, size, content_type, uploaded_by,meta))
        file_id = cur.fetchone()[0]
        conn.commit()
        return file_id

# =========================
# Staging rows(JSONB)
# =========================
def ensure_stage_tables():
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS stage_sfms_facility_raw (
          import_id     bigserial PRIMARY KEY,
          file_id       bigint REFERENCES stage_file(file_id) ON DELETE CASCADE,
          payload       jsonb NOT NULL,
          row_hash      text,
          imported_at   timestamptz DEFAULT now(),
          UNIQUE (file_id, row_hash)
        );
        """)
        # JSONB 검색 최적화(선택)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_stage_payload_gin
          ON stage_sfms_facility_raw USING gin (payload jsonb_path_ops);
        """)
        conn.commit()

def dataframe_to_stage(file_id: int, df: pd.DataFrame) -> int:
    df.columns = [c.upper() for c in df.columns]
    rows = []
    for _, s in df.iterrows():
        rec = {c: (None if pd.isna(s[c]) else _s(s[c])) for c in df.columns}
        rows.append((file_id, json.dumps(rec, ensure_ascii=False), sha1_json(rec)))
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO stage_sfms_facility_raw(file_id, payload, row_hash)
            VALUES (%s,%s::jsonb,%s)
            ON CONFLICT (file_id, row_hash) DO NOTHING
        """, rows, page_size=1000)
        cur.execute("UPDATE stage_file SET status='IMPORTED' WHERE file_id=%s", (file_id,))
        conn.commit()
        return len(rows)

def iter_stage_rows(file_id: int):
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("SELECT payload FROM stage_sfms_facility_raw WHERE file_id=%s", (file_id,))
        for (payload,) in cur.fetchall():
            yield payload  # dict

# =========================
# ORG / FACILITY upsert & link
# =========================
OWNER_CLASS = {
    '1': 'LOC_GOV',  # 지방자치단체
    # 중앙부처/정부기관
    '2':'GOV_AGENCY','4':'GOV_AGENCY','5':'GOV_AGENCY','6':'GOV_AGENCY',
    '7':'GOV_AGENCY','8':'GOV_AGENCY','9':'GOV_AGENCY','10':'GOV_AGENCY',
    '11':'GOV_AGENCY','12':'GOV_AGENCY','15':'GOV_AGENCY','16':'GOV_AGENCY',
    '17':'GOV_AGENCY','18':'GOV_AGENCY','19':'GOV_AGENCY','20':'GOV_AGENCY',
    # 공단/공사 등
    '3':'PUBLIC_INST','13':'PUBLIC_INST','21':'PUBLIC_INST'
}
def _ownerType(x): return '21' if x is None or _s(x)=='' else _s(x)

def build_org_tuple_from_row(r: dict):
    """
    반환: (org_type_code, adm_code, parent_adm_code, level, display_name, org_key)
    LOC_GOV: org_key=adm_code(시군구>시도)
    그 외: org_key=canon(display_name)
    """
    owner_type_id = _ownerType(r.get("POSESN_MBY_CD"))
    owner_name    = _s(r.get("POSESN_MBY_NM")) or None
    org_type      = OWNER_CLASS.get(owner_type_id, 'PUBLIC_INST')

    if org_type == 'LOC_GOV':
        sgg_cd = _adm10(r.get("POSESN_MBY_SIGNGU_CD"))
        ctp_cd = _adm10(r.get("POSESN_MBY_CTPRVN_CD")) or derive_ctp_from_sgg(sgg_cd)
        if not (sgg_cd or ctp_cd):
            return None
        ctp_nm = _s(r.get("POSESN_MBY_CTPRVN_NM"))
        sgg_nm = _s(r.get("POSESN_MBY_SIGNGU_NM"))
        if sgg_cd:
            adm_code, parent, level = sgg_cd, ctp_cd, 2
            disp = (ctp_nm + " " + sgg_nm).strip() or sgg_cd
        else:
            adm_code, parent, level = ctp_cd, None, 1
            disp = ctp_nm or ctp_cd
        org_key = adm_code
        return (org_type, adm_code, parent, level, disp, org_key)

    # 그 외(정부기관/공단…)
    disp    = owner_name or "확인필요"
    org_key = canon_name(disp)
    if not org_key:
        return None
    return (org_type, None, None, None, disp, org_key)

ORG_UPSERT_SQL = """
INSERT INTO org (org_type_code, adm_code, parent_adm_code, level, display_name, org_key)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (org_type_code, org_key) DO UPDATE
SET parent_adm_code = COALESCE(EXCLUDED.parent_adm_code, org.parent_adm_code),
    level           = COALESCE(EXCLUDED.level, org.level),
    display_name    = EXCLUDED.display_name,
    updated_at      = now();
"""

def upsert_orgs_from_stage(file_id: int) -> int:
    # dedupe by (org_type_code, org_key)
    uniq = {}
    for r in iter_stage_rows(file_id):
        tup = build_org_tuple_from_row(r)
        if not tup: continue
        key = (tup[0], tup[5])
        if key not in uniq:
            uniq[key] = tup
        else:
            o = list(uniq[key]); n = tup
            if not o[2] and n[2]: o[2] = n[2]          # parent
            if (o[3] or 0) < (n[3] or 0): o[3] = n[3]  # level
            if len(_s(n[4])) > len(_s(o[4])): o[4] = n[4]  # name
            uniq[key] = tuple(o)
    rows = list(uniq.values())
    if not rows: return 0
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_batch(cur, ORG_UPSERT_SQL, rows, page_size=1000)
        conn.commit()
    return len(rows)

# ---- Facility upsert ----
# ---- Facility upsert (정규화) ----
# 기존 문자열 삭제하고 이걸로 교체
FACILITY_UPSERT_SQL = f"""
INSERT INTO {CFG.tbl_facility} (
  facility_key, fclty_nm, induty_nm, fclty_ty_nm, fclty_ty_cd,
  owner_type_code, owner_name,
  addr_ctprvn_cd, addr_signgu_cd, addr_emd_cd, addr_li_cd,
  addr_adm_code, addr_display,
  lon, lat,
  fclty_ar_co, acmd_nmpr_co, adtm_co,
  fclty_hmpg_url, nation_alsfc_at, fclty_state_cd, deleted_flag, keyword,
  last_file_id, last_row_hash
)
VALUES (
  %s,%s,%s,%s,%s,
  %s,%s,
  %s,%s,%s,%s,
  %s,%s,
  %s,%s,
  %s,%s,%s,
  %s,%s,%s,%s,%s,
  %s,%s
)
ON CONFLICT (facility_key) DO UPDATE
SET fclty_nm        = EXCLUDED.fclty_nm,
    induty_nm       = EXCLUDED.induty_nm,
    fclty_ty_nm     = EXCLUDED.fclty_ty_nm,
    fclty_ty_cd     = EXCLUDED.fclty_ty_cd,
    owner_type_code = EXCLUDED.owner_type_code,
    owner_name      = EXCLUDED.owner_name,
    addr_ctprvn_cd  = EXCLUDED.addr_ctprvn_cd,
    addr_signgu_cd  = EXCLUDED.addr_signgu_cd,
    addr_emd_cd     = EXCLUDED.addr_emd_cd,
    addr_li_cd      = EXCLUDED.addr_li_cd,
    addr_adm_code   = EXCLUDED.addr_adm_code,
    addr_display    = EXCLUDED.addr_display,
    lon             = EXCLUDED.lon,
    lat             = EXCLUDED.lat,
    fclty_ar_co     = EXCLUDED.fclty_ar_co,
    acmd_nmpr_co    = EXCLUDED.acmd_nmpr_co,
    adtm_co         = EXCLUDED.adtm_co,
    fclty_hmpg_url  = EXCLUDED.fclty_hmpg_url,
    nation_alsfc_at = EXCLUDED.nation_alsfc_at,
    fclty_state_cd  = EXCLUDED.fclty_state_cd,
    deleted_flag    = EXCLUDED.deleted_flag,
    keyword         = EXCLUDED.keyword,
    last_file_id    = EXCLUDED.last_file_id,
    last_row_hash   = EXCLUDED.last_row_hash,
    updated_at      = now();
"""



def derive_addr_adm_code(r: dict) -> str|None:
    return (_adm10(r.get("ROAD_NM_EMD_CD")) or
            _adm10(r.get("ROAD_NM_SIGNGU_CD")) or
            _adm10(r.get("ROAD_NM_CTPRVN_CD")))

def addr_display_from(r: dict) -> str|None:
    disp = _s(r.get("RDNMADR_NM"))
    if disp: return disp
    parts = [_s(r.get("ROAD_NM_CTPRVN_NM")),
            _s(r.get("ROAD_NM_SIGNGU_NM")),
            _s(r.get("ROAD_NM_EMD_NM")),
            _s(r.get("ROAD_NM_LI_NM"))]
    disp2 = " ".join([p for p in parts if p])
    return disp2 or None

def derive_keywords(r: dict) -> list[str] | None:
    kws = set()
    for s in (_s(r.get("INDUTY_NM")), _s(r.get("FCLTY_TY_NM"))):
        if s: kws.add(s)
    # 필요 시 커스텀 매핑 추가 (예: '축구장' -> '축구')
    return list(kws) or None

def to_facility_tuple(r: dict, file_id: int|None=None, row_hash: str|None=None):
    owner_type_code =_ownerType(r.get("POSESN_MBY_CD"))
    owner_name= _s(r.get("POSESN_MBY_NM"))
    addr_ctp  = _adm10(r.get("ROAD_NM_CTPRVN_CD"))
    addr_sgg  = _adm10(r.get("ROAD_NM_SIGNGU_CD"))
    addr_emd  = _adm10(r.get("ROAD_NM_EMD_CD"))
    addr_li   = _adm10(r.get("ROAD_NM_LI_CD"))
    addr_adm  = derive_addr_adm_code(r)
    addr_disp = addr_display_from(r)
    keywords = derive_keywords(r)

    return (
        facility_key(r),
        _s(r.get("FCLTY_NM")),
        _s(r.get("INDUTY_NM")),
        _s(r.get("FCLTY_TY_NM")),
        _s(r.get("FCLTY_TY_CD")),
        owner_type_code,owner_name,
        addr_ctp, addr_sgg, addr_emd, addr_li,
        addr_adm, addr_disp,
        _f(r.get("FCLTY_LO")), _f(r.get("FCLTY_LA")),
        _n(r.get("FCLTY_AR_CO")), _n(r.get("ACMD_NMPR_CO")), _n(r.get("ADTM_CO")),
        _s(r.get("FCLTY_HMPG_URL")),
        True if _s(r.get("NATION_ALSFC_AT")).upper()=='Y' else (False if _s(r.get("NATION_ALSFC_AT")).upper()=='N' else None),
        _s(r.get("FCLTY_STATE_CD")),
        True if _s(r.get("DEL_AT")).upper()=='Y' else False,
        keywords,
        file_id, row_hash
    )


# ----- PROGRAM---------------


WD_MAP = {'월':'MON','화':'TUE','수':'WED','목':'THU','금':'FRI','토':'SAT','일':'SUN',
          'MON':'MON','TUE':'TUE','WED':'WED','THU':'THU','FRI':'FRI','SAT':'SAT','SUN':'SUN'}

_PCT_RE = re.compile(r'%+$')
_CUR_RE = re.compile(r'[원,\s]') 

def _n(x):
    """
    NUMERIC/금액용.
    - '30,800원', '45,100', '45100%', '  30 800 ' 모두 허용
    - 끝의 %는 버리고, 콤마/원/공백 제거 후 Decimal
    실패 시 None
    """
    s = _s(x)
    if not s or s.lower() in ('none','nan','null'): 
        return None
    s = _PCT_RE.sub('', s)          # 끝의 % 제거
    s = _CUR_RE.sub('', s)          # 콤마/원/공백 제거
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None

def _i(x):
    """정원/횟수 등 정수용. 위와 동일 규칙 + 음수/0 방지(Optional)"""
    v = _n(x)
    if v is None:
        return None
    try:
        iv = int(v)
        return iv if iv >= 0 else None
    except Exception:
        return None

def _date8(x):
    s=_s(x)
    if len(s)==8 and s.isdigit():
        try: return datetime.strptime(s, "%Y%m%d").date()
        except: return None
    return None

def _date_any(x):
    """
    'YYYYMMDD', 'YYYY-MM-DD', 'YYYY.M.D', '2025/8/31', 
    심지어 엑셀 serial(숫자)도 처리.
    실패 시 None
    """
    s = _s(x)
    if not s:
        return None
    # 엑셀 시리얼(대충 1900기준)
    if s.isdigit() and len(s) <= 5:
        try:
            base = dt.date(1899, 12, 30)  # Excel 1900 시스템 보정
            return base + dt.timedelta(days=int(s))
        except Exception:
            pass
    # 포맷 후보들
    fmts = ["%Y%m%d", "%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d", "%Y.%m.%d.",
            "%Y.%m.%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"]
    for f in fmts:
        try:
            return datetime.strptime(s, f).date()
        except Exception:
            continue
    return None


def parse_weekdays(s: str|None) -> list[str] | None:
    """
    '월수금', '화/목', '화ㆍ목', '월,수,금', '월 수 금' 모두 허용
    괄호안 표기도 제거: '월(수금)' → ['MON','WED','FRI']
    """
    if not s:
        return None
    s = re.sub(r'[()\[\]{}]+', ' ', s)
    # '월수금' 같은 붙임표현을 안전 분할: 한글 요일 단위로 쪼갬
    tokens = re.split(r'[\s,/·ㆍ・•|]+', s.strip())
    out = []
    for tok in tokens:
        tok = tok.strip()
        if not tok:
            continue
        # 붙임표현 대비: '월수금' → ['월','수','금']
        if re.fullmatch(r'[월화수목금토일]+', tok):
            for ch in tok:
                out.append(WD_MAP.get(ch))
        else:
            out.append(WD_MAP.get(tok.upper(), WD_MAP.get(tok.capitalize())))
    out = [x for x in out if x]
    # 중복 제거, 고정 순서
    order = ['MON','TUE','WED','THU','FRI','SAT','SUN']
    out = sorted(set(out), key=order.index)
    return out or None

def normalize_weekday_set(s: str|None) -> str|None:
    """
    표준 문자열: '월수금' → 'MON,WED,FRI'
    """
    lst = parse_weekdays(s)
    return ",".join(lst) if lst else None



def parse_time_range(raw: str|None):
    """
    다음 패턴을 모두 허용:
    - '09:00~10:30' / '09:00-10:30' / '9:0~10:30' (0 padding 자동)
    - '0900~1030' (콜론 없는 4자리)
    - '19:20' (끝시간 누락)
    - '1920' (끝시간 누락, 4자리)
    반환: (start_time, end_time)  (datetime.time | None)
    """
    s = _s(raw)
    if not s:
        return (None, None)

    s = s.replace('–','-').replace('~','-').strip()

    def _t4_to_hhmm(t):
        # '930' → '09:30', '1920' → '19:20'
        t = t.strip()
        if ':' in t:
            hh, mm = t.split(':')
            return f"{int(hh):02d}:{int(mm):02d}"
        if t.isdigit():
            if len(t) in (3,4):
                hh = int(t[:-2]); mm = int(t[-2:])
                return f"{hh:02d}:{mm:02d}"
        return None

    parts = [p for p in re.split(r'\s*-\s*', s) if p]
    st = _t4_to_hhmm(parts[0]) if parts else None
    en = _t4_to_hhmm(parts[1]) if len(parts) > 1 else None

    def _to_time(x):
        if not x: return None
        try: return datetime.strptime(x, "%H:%M").time()
        except: return None

    return (_to_time(st), _to_time(en))


_CANON_RE = re.compile(r'[\s\(\)\[\]\{\}·ㆍ・•\.,-]+')

def canon_name(name: str|None) -> str|None:
    """
    프로그램명 정규화: 공백/괄호/특수문자 제거 + 대문자
    예) '19시 월수금 교정1 (성인,청소년)' → '19시월수금교정1성인청소년' → 대문자
    """
    s = _s(name)
    s = _CANON_RE.sub('', s).upper()
    return s or None

def canon_program_core(name: str|None) -> str|None:
    s = _s(name)
    s = re.sub(r'\([^)]*\)', ' ', s)
    s = re.sub(r'\b(월|화|수|목|금|토|일|월수금|화목|토일)\b', ' ', s)
    s = re.sub(r'\d{1,2}\s*시', ' ', s)
    s = re.sub(r'\d{3,4}', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s or None

def sha1(text: str) -> str:
    return hashlib.sha1(text.encode('utf-8')).hexdigest()

def program_facility_key(row: dict) -> str:
    name = _s(row.get("FCLTY_NM")).upper()
    adm  = _s(row.get("EMD_CD") or row.get("SIGNGU_CD") or row.get("CTPRVN_CD"))
    raw  = "|".join([name, adm])
    return sha1(raw)


def program_facility_key_from_row(row: dict) -> str:
    name = _s(row.get("FCLTY_NM")).upper()
    adm  = _s(row.get("EMD_CD") or row.get("SIGNGU_CD") or row.get("CTPRVN_CD"))
    raw  = "|".join([name, adm])
    return sha1(raw)

def build_session_natural_key(r: dict) -> str:
    """
    '같은 시간/요일/시설/핵심명'을 하나의 세션으로 묶는 자연키.
    이 키가 같고 날짜만 다르면 같은 세션으로 간주하여 TERM을 여러 개 둘 수 있음.
    """
    fac_key = program_facility_key(r)  # 기존 함수 재사용
    core = canon_program_core(r.get("PROGRM_NM")) or _s(r.get("PROGRM_NM"))
    wset = normalize_weekday_set(_s(r.get("PROGRM_ESTBL_WKDAY_NM"))) or ''
    st, en = parse_time_range(_s(r.get("PROGRM_ESTBL_TIZN_VALUE")))
    st_s = st.strftime("%H:%M") if st else ''
    en_s = en.strftime("%H:%M") if en else ''
    raw = "|".join([fac_key, _s(r.get("PROGRM_TY_NM")).upper(), core.upper(), wset, st_s, en_s])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def is_consecutive(prev_end: dt.date|None, start: dt.date|None) -> bool:
    """연속 기간 병합 후보 판단용(보고서/로그에서 사용)."""
    if not prev_end or not start: 
        return False
    return (start - prev_end) == dt.timedelta(days=1)


def program_key(row: dict) -> str:
    fk = program_facility_key(row)
    nm = _s(row.get("PROGRM_NM")).upper()
    bd = _s(row.get("PROGRM_BEGIN_DE"))
    raw = "|".join([fk, nm, bd])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()

def derive_prog_keywords(r: dict) -> list[str] | None:
    kws=set()
    for s in (_s(r.get("PROGRM_TY_NM")), _s(r.get("PROGRM_TRGET_NM"))):
        if s: kws.add(s)
    return list(kws) or None

@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


def ensure_program_mst_sess_term_tables(pg: PgConfig):
    ddl = """
    CREATE TABLE IF NOT EXISTS program_master (
      master_id   BIGSERIAL PRIMARY KEY,
      sport_id    BIGINT,
      name        VARCHAR(255) NOT NULL,
      target      VARCHAR(100),
      level       VARCHAR(50),
      base_price  INTEGER,
      master_key  TEXT NOT NULL UNIQUE,
      created_at  timestamptz DEFAULT now(),
      updated_at  timestamptz DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS program_session (
      session_id    BIGSERIAL PRIMARY KEY,
      master_id     BIGINT NOT NULL REFERENCES program_master(master_id),
      facility_id   BIGINT,
      weekday_set   VARCHAR(50) NOT NULL,
      start_time    TIME NOT NULL,
      end_time      TIME NOT NULL,
      location_id   BIGINT,
      capacity_base INTEGER,
      session_key   TEXT NOT NULL UNIQUE,
      created_at    timestamptz DEFAULT now(),
      updated_at    timestamptz DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS program_term (
      term_id     BIGSERIAL PRIMARY KEY,
      session_id  BIGINT NOT NULL REFERENCES program_session(session_id),
      start_date  DATE NOT NULL,
      end_date    DATE NOT NULL,
      price       INTEGER,
      capacity    INTEGER,
      enrollment_url TEXT,
      created_at  timestamptz DEFAULT now(),
      updated_at  timestamptz DEFAULT now(),
      CHECK (end_date >= start_date)
    );
    CREATE INDEX IF NOT EXISTS idx_program_term_session ON program_term(session_id, start_date, end_date);
    """
    with psycopg2.connect(host=pg.host, port=pg.port, dbname=pg.dbname, user=pg.user, password=pg.password) as conn, conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()

# =========================
# Programs: upsert & link
# =========================



MASTER_UPSERT_SQL = """
INSERT INTO program_master (sport_id, name, target, level, base_price, master_key)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT (master_key) DO UPDATE
SET sport_id = EXCLUDED.sport_id,
    target   = EXCLUDED.target,
    level    = EXCLUDED.level,
    base_price = COALESCE(EXCLUDED.base_price, program_master.base_price),
    updated_at = now()
RETURNING master_id;
"""

SESSION_UPSERT_SQL = """
INSERT INTO program_session (master_id, facility_id, weekday_set, start_time, end_time, location_id, capacity_base, session_key)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (session_key) DO UPDATE
SET facility_id   = COALESCE(EXCLUDED.facility_id, program_session.facility_id),
    location_id   = COALESCE(EXCLUDED.location_id, program_session.location_id),
    capacity_base = COALESCE(EXCLUDED.capacity_base, program_session.capacity_base),
    updated_at    = now()
RETURNING session_id;
"""

TERM_INSERT_SQL = """
INSERT INTO program_term (session_id, start_date, end_date, price, capacity, enrollment_url)
VALUES (%s,%s,%s,%s,%s,%s)
ON CONFLICT DO NOTHING
RETURNING term_id;
"""



PROGRAM_UPSERT_SQL = """
INSERT INTO sfms_program (
  program_key, facility_key, facility_id,
  progrm_ty_nm, progrm_nm, progrm_trget_nm, descr,
  progrm_prc, progrm_prc_ty_nm, progrm_rcrit_nmpr_co,
  begin_date, end_date, estbl_wkday_nm, weekdays, estbl_tizn_value, start_time, end_time,
  hmpg_url, contact_tel,
  keyword, deleted_flag,
  last_file_id, last_row_hash
) VALUES (
  %s,%s,NULL,
  %s,%s,%s,%s,
  %s,%s,%s,
  %s,%s,%s,%s,%s,%s,%s,
  %s,%s,
  %s,%s,
  %s,%s
)
ON CONFLICT (program_key) DO UPDATE
SET progrm_ty_nm         = EXCLUDED.progrm_ty_nm,
    progrm_nm            = EXCLUDED.progrm_nm,
    progrm_trget_nm      = EXCLUDED.progrm_trget_nm,
    descr                = EXCLUDED.descr,
    progrm_prc           = EXCLUDED.progrm_prc,
    progrm_prc_ty_nm     = EXCLUDED.progrm_prc_ty_nm,
    progrm_rcrit_nmpr_co = EXCLUDED.progrm_rcrit_nmpr_co,
    begin_date           = EXCLUDED.begin_date,
    end_date             = EXCLUDED.end_date,
    estbl_wkday_nm       = EXCLUDED.estbl_wkday_nm,
    weekdays             = EXCLUDED.weekdays,
    estbl_tizn_value     = EXCLUDED.estbl_tizn_value,
    start_time           = EXCLUDED.start_time,
    end_time             = EXCLUDED.end_time,
    hmpg_url             = EXCLUDED.hmpg_url,
    contact_tel          = EXCLUDED.contact_tel,
    keyword              = EXCLUDED.keyword,
    deleted_flag         = EXCLUDED.deleted_flag,
    last_file_id         = EXCLUDED.last_file_id,
    last_row_hash        = EXCLUDED.last_row_hash,
    updated_at           = now();
"""

# ========== Sport / Facility lookup ==========

def lookup_sport_id(cur, sport_name: str|None) -> int|None:
    if not sport_name:
        return None
    cur.execute("SELECT sport_id FROM sport_category WHERE sport_name_kr = %s OR sport_name_en = %s OR sport_code = %s LIMIT 1",
                (sport_name, sport_name, sport_name))
    row = cur.fetchone()
    return row[0] if row else None

def lookup_facility_id_by_key(cur, facility_key: str|None) -> int|None:
    if not facility_key:
        return None
    cur.execute(f"SELECT id FROM {CFG.tbl_facility} WHERE facility_key = %s LIMIT 1", (facility_key,))
    row = cur.fetchone()
    return row[0] if row else None

def build_master_key(row: dict) -> str:
    sport = _s(row.get("PROGRM_TY_NM")).upper()
    core  = canon_program_core(row.get("PROGRM_NM")) or _s(row.get("PROGRM_NM"))
    target = _s(row.get("PROGRM_TRGET_NM")).upper()
    level  = ""
    raw = "|".join([sport, (core or '').upper(), target, level])
    return sha1(raw)

def build_session_key(row: dict, master_key: str) -> str:
    wset = normalize_weekday_set(_s(row.get("PROGRM_ESTBL_WKDAY_NM"))) or ''
    st, en = parse_time_range(_s(row.get("PROGRM_ESTBL_TIZN_VALUE")))
    st_s = st.strftime("%H:%M") if st else ''
    en_s = en.strftime("%H:%M") if en else ''
    fac_key = program_facility_key_from_row(row)
    raw = "|".join([master_key, fac_key, wset, st_s, en_s])
    return sha1(raw)

def upsert_one_row_as_mst_sess_term(cur, row: dict):
    begin = _date_any(row.get("PROGRM_BEGIN_DE"))
    end   = _date_any(row.get("PROGRM_END_DE"))
    st, en = parse_time_range(row.get("PROGRM_ESTBL_TIZN_VALUE"))
    wset = normalize_weekday_set(row.get("PROGRM_ESTBL_WKDAY_NM"))
    price = _n_num(row.get("PROGRM_PRC"))
    cap   = _i_num(row.get("PROGRM_RCRIT_NMPR_CO"))
    url   = _s(row.get("HMPG_URL")) or None

    master_key = build_master_key(row)
    sport_id = lookup_sport_id(cur, _s(row.get("PROGRM_TY_NM")) or None)
    master_params = (
        sport_id,
        canon_program_core(row.get("PROGRM_NM")) or _s(row.get("PROGRM_NM")),
        _s(row.get("PROGRM_TRGET_NM")) or None,
        None,  # level
        int(price) if price is not None else None,
        master_key
    )
    cur.execute(MASTER_UPSERT_SQL, master_params)
    master_id = cur.fetchone()[0]

    session_key = build_session_key(row, master_key)
    fac_id = lookup_facility_id_by_key(cur, program_facility_key_from_row(row))
    session_params = (
        master_id, fac_id, wset, st, en, None,  # location_id
        int(cap) if cap is not None else None,
        session_key
    )
    cur.execute(SESSION_UPSERT_SQL, session_params)
    session_id = cur.fetchone()[0]

    if begin and end:
        term_params = (session_id, begin, end, int(price) if price is not None else None,
            int(cap) if cap is not None else None, url)
        cur.execute(TERM_INSERT_SQL, term_params)

def upsert_programs_master_session_term(pg: PgConfig, file_id: int, stage_table: str = STAGE_PROGRAM_TABLE, batch_size: int = 1000):
    with psycopg2.connect(host=pg.host, port=pg.port, dbname=pg.dbname, user=pg.user, password=pg.password) as conn:
        conn.autocommit = False
        with conn.cursor(name="stage_program_scan") as scan:
            scan.itersize = batch_size
            scan.execute(f"SELECT payload FROM {stage_table} WHERE file_id=%s ORDER BY import_id", (file_id,))
            processed = 0
            with conn.cursor() as cur:
                for (payload,) in scan:
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    upsert_one_row_as_mst_sess_term(cur, payload)
                    processed += 1
                    if processed % batch_size == 0:
                        conn.commit()
                conn.commit()
    return True


# def lookup_sport_id(cur, sport_name: str|None) -> int|None:
#     if not sport_name:
#         return None
#     cur.execute("SELECT sport_id FROM sport_category WHERE sport_name_kr = %s OR sport_name_en = %s OR sport_code = %s LIMIT 1",
#                 (sport_name, sport_name, sport_name))
#     row = cur.fetchone()
#     return row[0] if row else None

# def lookup_facility_id_by_key(cur, facility_key: str|None) -> int|None:
#     if not facility_key:
#         return None
#     cur.execute("SELECT id FROM sfms_facility WHERE facility_key = %s LIMIT 1", (facility_key,))
#     row = cur.fetchone()
#     return row[0] if row else None

# ========== Keys ==========
# def build_master_key(row: dict) -> str:
#     sport = _s(row.get("PROGRM_TY_NM")).upper()
#     core  = canon_program_core(row.get("PROGRM_NM")) or _s(row.get("PROGRM_NM"))
#     target = _s(row.get("PROGRM_TRGET_NM")).upper()
#     level  = ""
#     raw = "|".join([sport, core.upper(), target, level])
#     return sha1(raw)

# def build_session_key(row: dict, master_key: str) -> str:
#     wset = normalize_weekday_set(_s(row.get("PROGRM_ESTBL_WKDAY_NM"))) or ''
#     st, en = parse_time_range(_s(row.get("PROGRM_ESTBL_TIZN_VALUE")))
#     st_s = st.strftime("%H:%M") if st else ''
#     en_s = en.strftime("%H:%M") if en else ''
#     fac_key = program_facility_key(row)
#     raw = "|".join([master_key, fac_key, wset, st_s, en_s])
#     return sha1(raw)



# def upsert_one_row_as_mst_sess_term(cur, row: dict):
    # begin = _date_any(row.get("PROGRM_BEGIN_DE"))
    # end   = _date_any(row.get("PROGRM_END_DE"))
    # st, en = parse_time_range(row.get("PROGRM_ESTBL_TIZN_VALUE"))
    # wset = normalize_weekday_set(row.get("PROGRM_ESTBL_WKDAY_NM"))
    # price = _n(row.get("PROGRM_PRC"))
    # cap   = _i(row.get("PROGRM_RCRIT_NMPR_CO"))
    # url   = _s(row.get("HMPG_URL")) or None

    # # MASTER
    # master_key = build_master_key(row)
    # sport_id = lookup_sport_id(cur, _s(row.get("PROGRM_TY_NM")) or None)
    # master_params = (
    #     sport_id,
    #     canon_program_core(row.get("PROGRM_NM")) or _s(row.get("PROGRM_NM")),
    #     _s(row.get("PROGRM_TRGET_NM")) or None,
    #     None,
    #     int(price) if price is not None else None,
    #     master_key
    # )
    # cur.execute(MASTER_UPSERT_SQL, master_params)
    # master_id = cur.fetchone()[0]

    # # SESSION
    # session_key = build_session_key(row, master_key)
    # fac_id = lookup_facility_id_by_key(cur, program_facility_key(row))
    # session_params = (
    #     master_id, fac_id, wset, st, en, None,
    #     int(cap) if cap is not None else None,
    #     session_key
    # )
    # cur.execute(SESSION_UPSERT_SQL, session_params)
    # session_id = cur.fetchone()[0]

    # # TERM
    # if begin and end:
    #     term_params = (session_id, begin, end, int(price) if price is not None else None,
    #                    int(cap) if cap is not None else None, url)
    #     cur.execute(TERM_INSERT_SQL, term_params)

# ========== Bulk upsert ==========
def upsert_programs_master_session_term(pg: PgConfig, file_id: int, stage_table: str = "stage_sfms_program_raw", batch_size: int = 1000):
    with psycopg2.connect(host=pg.host, port=pg.port, dbname=pg.dbname, user=pg.user, password=pg.password) as conn:
        conn.autocommit = False
        with conn.cursor(name="stage_program_scan") as scan:
            scan.itersize = batch_size
            scan.execute(f"SELECT payload FROM {stage_table} WHERE file_id=%s ORDER BY import_id", (file_id,))
            processed = 0
            with conn.cursor() as cur:
                for (payload,) in scan:
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    upsert_one_row_as_mst_sess_term(cur, payload)
                    processed += 1
                    if processed % batch_size == 0:
                        conn.commit()
                conn.commit()
    return True


def to_program_tuple(r: dict, file_id: int|None=None, row_hash: str|None=None):
    st, en = parse_time_range(r.get("PROGRM_ESTBL_TIZN_VALUE"))
    return (
        program_key(r),
        program_facility_key(r),
        _s(r.get("PROGRM_TY_NM")),
        _s(r.get("PROGRM_NM")),
        _s(r.get("PROGRM_TRGET_NM")),
        None,  # descr: 원본에 별도 설명 컬럼 없으면 None
        _n(r.get("PROGRM_PRC")),
        _s(r.get("PROGRM_PRC_TY_NM")),
        _n(r.get("PROGRM_RCRIT_NMPR_CO")),
        _date8(r.get("PROGRM_BEGIN_DE")),
        _date8(r.get("PROGRM_END_DE")),
        _s(r.get("PROGRM_ESTBL_WKDAY_NM")),
        parse_weekdays(_s(r.get("PROGRM_ESTBL_WKDAY_NM"))),
        _s(r.get("PROGRM_ESTBL_TIZN_VALUE")),
        st, en,
        _s(r.get("HMPG_URL")),
        _s(r.get("FCLTY_TEL_NO")),
        derive_prog_keywords(r),
        False,         # 삭제여부 컬럼이 없으므로 기본 False
        file_id, row_hash
    )

def ensure_sport_tables_and_seed(pg: PgConfig):
    ddl = """
    CREATE TABLE IF NOT EXISTS sport_category (
    sport_id      BIGSERIAL PRIMARY KEY,
    sport_code    VARCHAR(50) UNIQUE,
    sport_name_kr VARCHAR(100) NOT NULL UNIQUE,
    sport_name_en VARCHAR(100) UNIQUE,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    timestamptz DEFAULT now(),
    updated_at    timestamptz DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_sport_category_names
    ON sport_category (LOWER(sport_name_kr), LOWER(sport_name_en), LOWER(sport_code));

    CREATE TABLE IF NOT EXISTS sport_alias (
    alias_id    BIGSERIAL PRIMARY KEY,
    sport_id    BIGINT NOT NULL REFERENCES sport_category(sport_id) ON DELETE CASCADE,
    alias_name  VARCHAR(100) NOT NULL,
    created_at  timestamptz DEFAULT now(),
    UNIQUE (sport_id, LOWER(alias_name))
    );
    CREATE INDEX IF NOT EXISTS idx_sport_alias_name
    ON sport_alias (LOWER(alias_name));
    """
    seeds = [
        ('SWIM','수영','Swimming'),
        ('FITNESS','헬스','Fitness'),
        ('SOCCER','축구','Soccer'),
        ('BADMINTON','배드민턴','Badminton'),
        ('BASKET','농구','Basketball'),
        ('PINGPONG','탁구','Table Tennis'),
        ('TENNIS','테니스','Tennis'),
        ('GOLF','골프','Golf'),
        ('KUKSUNDO','국선도','Kuksundo'),
        ('RACQUETB','라켓볼','Racquetball'),
    ]
    alias_values = {
        '헬스': ['피트니스','PT','GYM','Fitness'],
        '수영': ['SWIM','수영장'],
        '축구': ['SOCCER','풋살'],   # ← 정책 결정 필요
        '탁구': ['TABLE TENNIS','핑퐁'],
        '골프': ['GOLF'],
        '테니스':['TENNIS'],
        '농구': ['BASKETBALL'],
        '배드민턴':['BADMINTON'],
        '국선도':['KUKSUNDO'],
        '라켓볼':['RACQUETBALL'],
    }

    upsert_cat = """
    INSERT INTO sport_category (sport_code, sport_name_kr, sport_name_en)
    VALUES (%s,%s,%s)
    ON CONFLICT (sport_name_kr) DO UPDATE
    SET sport_code = EXCLUDED.sport_code,
        sport_name_en = EXCLUDED.sport_name_en,
        updated_at = now()
    RETURNING sport_id, sport_name_kr;
    """
    upsert_alias = """
    INSERT INTO sport_alias (sport_id, alias_name)
    VALUES (%s,%s)
    ON CONFLICT (sport_id, LOWER(alias_name)) DO NOTHING;
    """

    with psycopg2.connect(host=pg.host, port=pg.port, dbname=pg.dbname, user=pg.user, password=pg.password) as conn, conn.cursor() as cur:
        cur.execute(ddl)
        for code, kr, en in seeds:
            cur.execute(upsert_cat, (code, kr, en))
            sid, kr_name = cur.fetchone()
            for alias in alias_values.get(kr_name, []):
                cur.execute(upsert_alias, (sid, alias))
        conn.commit()

def upsert_programs_from_stage(file_id: int) -> int:
    total=0; buf=[]
    for payload, row_hash in iter_stage_program_rows_with_meta(file_id, BATCH_SIZE):
        buf.append(to_program_tuple(payload, file_id, row_hash))
        if len(buf) >= BATCH_SIZE:
            with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
                execute_batch(cur, PROGRAM_UPSERT_SQL, buf, page_size=BATCH_SIZE)
                conn.commit()
            total += len(buf); buf.clear()
    if buf:
        with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
            execute_batch(cur, PROGRAM_UPSERT_SQL, buf, page_size=BATCH_SIZE)
            conn.commit()
        total += len(buf)
    logging.info("sfms_program upserted: %d (file_id=%s)", total, file_id)
    return total

def link_program_to_facility_by_file(file_id: int):
    """
    stage_sfms_program_raw 원본 payload로부터
    - 프로그램 시설명: 대소문자/공백/기호 제거
    - 행정코드: COALESCE(EMD_CD, SIGNGU_CD, CTPRVN_CD)
    를 뽑아 시설의 (정규화이름, addr_adm_code)와 조인해 program.facility_id를 채웁니다.
    """
    sql = r"""
    WITH p AS (
      SELECT pr.id AS program_id,
             UPPER(regexp_replace(COALESCE(sp.payload->>'FCLTY_NM',''),
                 '[\s\(\)\[\]\{\}·ㆍ・•\.,-]+','','g')) AS nm,
             COALESCE(NULLIF(sp.payload->>'EMD_CD',''),
                      NULLIF(sp.payload->>'SIGNGU_CD',''),
                      NULLIF(sp.payload->>'CTPRVN_CD','')) AS adm
      FROM sfms_program pr
      JOIN stage_sfms_program_raw sp
        ON pr.last_file_id = sp.file_id
       AND pr.last_row_hash = sp.row_hash
      WHERE pr.last_file_id = %(file_id)s
    ),
    f AS (
      SELECT f.id AS facility_id,
             UPPER(regexp_replace(COALESCE(f.fclty_nm,''),
                 '[\s\(\)\[\]\{\}·ㆍ・•\.,-]+','','g')) AS nm,
             f.addr_adm_code AS adm
      FROM sfms_facility f
      WHERE f.addr_adm_code IS NOT NULL
    ),
    match AS (
      SELECT p.program_id, f.facility_id
      FROM p JOIN f ON p.nm = f.nm AND p.adm = f.adm
    )
    UPDATE sfms_program sp
       SET facility_id = m.facility_id
    FROM match m
    WHERE sp.id = m.program_id
      AND sp.facility_id IS NULL;
    """
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(sql, {"file_id": file_id})
        updated = cur.rowcount
        conn.commit()
        logging.info("program.facility_id linked by file=%s : %d rows", file_id, updated)


def backfill_program_type_from_facility():
    """
    p.progrm_ty_nm 이 NULL 인 경우, 연결된 시설의 fclty_nm/fclty_ty_nm을 기준으로
    대표 카테고리를 채운다. (간단 룰셋 예시)
    """
    sql = r"""
    WITH map AS (
      SELECT f.id AS facility_id,
             CASE
               WHEN lower(f.fclty_nm) LIKE '%수영%' OR lower(f.fclty_ty_nm) LIKE '%수영%' THEN '수영'
               WHEN lower(f.fclty_nm) LIKE '%헬스%' OR lower(f.fclty_ty_nm) LIKE '%헬스%' THEN '헬스'
               WHEN lower(f.fclty_nm) LIKE '%축구%' OR lower(f.fclty_ty_nm) LIKE '%축구%' THEN '축구'
               WHEN lower(f.fclty_nm) LIKE '%풋살%' OR lower(f.fclty_ty_nm) LIKE '%풋살%' THEN '풋살'
               WHEN lower(f.fclty_nm) LIKE '%농구%' OR lower(f.fclty_ty_nm) LIKE '%농구%' THEN '농구'
               WHEN lower(f.fclty_nm) LIKE '%배드민턴%' OR lower(f.fclty_ty_nm) LIKE '%배드민턴%' THEN '배드민턴'
               WHEN lower(f.fclty_nm) LIKE '%테니스%' OR lower(f.fclty_ty_nm) LIKE '%테니스%' THEN '테니스'
               WHEN lower(f.fclty_nm) LIKE '%탁구%' OR lower(f.fclty_ty_nm) LIKE '%탁구%' THEN '탁구'
               WHEN lower(f.fclty_nm) LIKE '%요가%' OR lower(f.fclty_ty_nm) LIKE '%요가%' THEN '요가'
               WHEN lower(f.fclty_nm) LIKE '%필라테스%' OR lower(f.fclty_ty_nm) LIKE '%필라테스%' THEN '필라테스'
               ELSE NULL
             END AS inferred_type
      FROM sfms_facility f
    )
    UPDATE sfms_program p
       SET progrm_ty_nm = m.inferred_type
    FROM map m
    WHERE p.facility_id = m.facility_id
      AND p.progrm_ty_nm IS NULL
      AND m.inferred_type IS NOT NULL;
    """
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(sql)
        n1 = cur.rowcount

        # (옵션) 아직도 NULL이면 프로그램명 키워드로 보정
        cur.execute(r"""
          UPDATE sfms_program
             SET progrm_ty_nm = '수영'
           WHERE progrm_ty_nm IS NULL
             AND (lower(progrm_nm) LIKE '%수영%' OR (keyword IS NOT NULL AND '수영' = ANY(keyword)));
        """)
        n2 = cur.rowcount

        conn.commit()
        logging.info("progrm_ty_nm backfilled by facility=%d, by name=%d", n1, n2)




STAGE_PROGRAM_TABLE = "stage_sfms_program_raw"

def dataframe_to_stage_program(file_id: int, df: pd.DataFrame) -> int:
    df.columns = [c.upper() for c in df.columns]
    rows = []
    for _, s in df.iterrows():
        rec = {c: (None if pd.isna(s[c]) else _s(s[c])) for c in df.columns}
        rows.append((file_id, json.dumps(rec, ensure_ascii=False), sha1_json(rec)))
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        execute_batch(cur, f"""
            INSERT INTO {STAGE_PROGRAM_TABLE}(file_id, payload, row_hash)
            VALUES (%s,%s::jsonb,%s)
            ON CONFLICT (file_id, row_hash) DO NOTHING
        """, rows, page_size=BATCH_SIZE)
        cur.execute(f"UPDATE {CFG.tbl_stage_file} SET status='IMPORTED' WHERE file_id=%s", (file_id,))
        conn.commit()
        return len(rows)


def iter_stage_program_rows_with_meta(file_id: int, itersize: int | None = None):
    itersize = itersize or BATCH_SIZE
    sql = f"""
      SELECT payload, row_hash
      FROM {STAGE_PROGRAM_TABLE}
      WHERE file_id=%s
      ORDER BY import_id
    """
    conn = psycopg2.connect(**PG)
    try:
        cur = conn.cursor(name="program_stream")
        cur.itersize = itersize
        cur.execute(sql, (file_id,))
        for payload, row_hash in cur:
            if isinstance(payload, str):
                try: payload = json.loads(payload)
                except: pass
            yield payload, row_hash
    finally:
        try: cur.close()
        except: pass
        conn.close()




# ---- Link OWNER (facility_org + owner_org_id) ----
FACILITY_OWNER_LINK_SQL_BY_FILE = """
WITH src AS (
  SELECT f.id AS facility_id, s.payload
  FROM sfms_facility f
  JOIN stage_sfms_facility_raw s
    ON s.file_id = %(file_id)s
   AND f.last_file_id = s.file_id
   AND f.last_row_hash = s.row_hash
),
keymap AS (
  SELECT
    facility_id,
    CASE (payload->>'POSESN_MBY_CD')
      WHEN '1' THEN 'LOC_GOV'
      WHEN '2' THEN 'GOV_AGENCY'  WHEN '4' THEN 'GOV_AGENCY'
      WHEN '5' THEN 'GOV_AGENCY'  WHEN '6' THEN 'GOV_AGENCY'
      WHEN '7' THEN 'GOV_AGENCY'  WHEN '8' THEN 'GOV_AGENCY'
      WHEN '9' THEN 'GOV_AGENCY'  WHEN '10' THEN 'GOV_AGENCY'
      WHEN '11' THEN 'GOV_AGENCY' WHEN '12' THEN 'GOV_AGENCY'
      WHEN '15' THEN 'GOV_AGENCY' WHEN '16' THEN 'GOV_AGENCY'
      WHEN '17' THEN 'GOV_AGENCY' WHEN '18' THEN 'GOV_AGENCY'
      WHEN '19' THEN 'GOV_AGENCY' WHEN '20' THEN 'GOV_AGENCY'
      WHEN '3' THEN 'PUBLIC_INST' WHEN '13' THEN 'PUBLIC_INST'
      WHEN '21' THEN 'PUBLIC_INST'
      ELSE 'PUBLIC_INST'
    END AS org_type_code,
    CASE (payload->>'POSESN_MBY_CD')
      WHEN '1' THEN COALESCE(NULLIF(payload->>'POSESN_MBY_SIGNGU_CD',''),
                             NULLIF(payload->>'POSESN_MBY_CTPRVN_CD',''))
      ELSE UPPER(regexp_replace(COALESCE(payload->>'POSESN_MBY_NM',''),
            '[\\s\\(\\)\\[\\]\\{\\}·ㆍ・•\\.,-]+','','g'))
    END AS org_key
  FROM src
)
-- facility_org(OWNER) 삽입
INSERT INTO facility_org (facility_id, org_id, role)
SELECT k.facility_id, o.org_id, 'OWNER'
FROM keymap k
JOIN org o
  ON o.org_type_code = k.org_type_code
 AND o.org_key       = k.org_key
LEFT JOIN facility_org fo
  ON fo.facility_id = k.facility_id
 AND fo.org_id      = o.org_id
 AND fo.role        = 'OWNER'
WHERE k.org_key IS NOT NULL
  AND fo.facility_id IS NULL;
"""

BACKFILL_OWNER_ID_SQL_BY_FILE = """
WITH src AS (
  SELECT f.id AS facility_id, s.payload
  FROM sfms_facility f
  JOIN stage_sfms_facility_raw s
    ON s.file_id = %(file_id)s
   AND f.last_file_id = s.file_id
   AND f.last_row_hash = s.row_hash
),
keymap AS (
  SELECT
    facility_id,
    CASE (payload->>'POSESN_MBY_CD')
      WHEN '1' THEN 'LOC_GOV'
      WHEN '2' THEN 'GOV_AGENCY'  WHEN '4' THEN 'GOV_AGENCY'
      WHEN '5' THEN 'GOV_AGENCY'  WHEN '6' THEN 'GOV_AGENCY'
      WHEN '7' THEN 'GOV_AGENCY'  WHEN '8' THEN 'GOV_AGENCY'
      WHEN '9' THEN 'GOV_AGENCY'  WHEN '10' THEN 'GOV_AGENCY'
      WHEN '11' THEN 'GOV_AGENCY' WHEN '12' THEN 'GOV_AGENCY'
      WHEN '15' THEN 'GOV_AGENCY' WHEN '16' THEN 'GOV_AGENCY'
      WHEN '17' THEN 'GOV_AGENCY' WHEN '18' THEN 'GOV_AGENCY'
      WHEN '19' THEN 'GOV_AGENCY' WHEN '20' THEN 'GOV_AGENCY'
      WHEN '3' THEN 'PUBLIC_INST' WHEN '13' THEN 'PUBLIC_INST'
      WHEN '21' THEN 'PUBLIC_INST'
      ELSE 'PUBLIC_INST'
    END AS org_type_code,
    CASE (payload->>'POSESN_MBY_CD')
      WHEN '1' THEN COALESCE(NULLIF(payload->>'POSESN_MBY_SIGNGU_CD',''),
                             NULLIF(payload->>'POSESN_MBY_CTPRVN_CD',''))
      ELSE UPPER(regexp_replace(COALESCE(payload->>'POSESN_MBY_NM',''),
            '[\\s\\(\\)\\[\\]\\{\\}·ㆍ・•\\.,-]+','','g'))
    END AS org_key
  FROM src
)
UPDATE sfms_facility f
SET owner_org_id = o.org_id
FROM keymap k
JOIN org o
  ON o.org_type_code = k.org_type_code
 AND o.org_key       = k.org_key
WHERE f.id = k.facility_id
  AND f.owner_org_id IS NULL;
"""

def link_owner_relations(file_id: int):
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute(FACILITY_OWNER_LINK_SQL_BY_FILE, {"file_id": file_id})
        ins = cur.rowcount
        cur.execute(BACKFILL_OWNER_ID_SQL_BY_FILE, {"file_id": file_id})
        upd = cur.rowcount
        conn.commit()
        print(f"facility_org OWNER inserted={ins}, owner_org_id updated={upd}")

# =========================
# CLI
# =========================
def main():
    ap = argparse.ArgumentParser(description="Facilities ETL (local now, S3-ready)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    # Facility 라인
    fac_full = sub.add_parser("facility-full", help="stage -> import(org/facility) -> link")
    fac_full.add_argument("path")

    fac_stage = sub.add_parser("facility-stage", help="stage facility excel")
    fac_stage.add_argument("path")

    fac_imp = sub.add_parser("facility-import", help="import facilities from staged file_id")
    fac_imp.add_argument("file_id", type=int)

    # Program 라인
    prg_full = sub.add_parser("program-full", help="stage -> import(program) -> link")
    prg_full.add_argument("path")

    prg_stage = sub.add_parser("program-stage", help="stage program excel")
    prg_stage.add_argument("path")

    prg_imp = sub.add_parser("program-import", help="import programs from staged file_id")
    prg_imp.add_argument("file_id", type=int)

    args = ap.parse_args()

    if args.cmd =="facility-full":
        # 1) stage_file
        if CFG.storage_backend == "local":
            file_id = stage_local_file_record(args.path ,dataset="facility")
        else:
            raise RuntimeError("S3 backend not implemented yet. Set STORAGE_BACKEND=local for now.")
        

        # 2) stage rows
        ensure_stage_tables()
        df = read_excel_any(args.path)
        if file_id is None:
            # S3 모드가 되면 stage_file에 s3 메타 먼저 넣고 file_id 받아오면 됨
            raise RuntimeError("S3 모드는 나중에 활성화하세요 (STORAGE_BACKEND=s3)")
        nrows = dataframe_to_stage(file_id, df)
        print(f"[stage] file_id={file_id}, rows={nrows}")
        # 3) upsert org / facility / link
        n_org = upsert_orgs_from_stage(file_id)
        n_fac = upsert_facilities_from_stage(file_id)
        link_owner_relations(file_id)
        print(f"[facility-full] org={n_org}, facility={n_fac} done")
        return

    if args.cmd == "facility-stage":
        if CFG.storage_backend == "local":
            file_id = stage_local_file_record(args.path,dataset="facility")
        else:
            raise RuntimeError("S3 backend not implemented yet.")
        
        ensure_stage_tables()
        df = read_excel_any(args.path)
        nrows = dataframe_to_stage(file_id, df)
        print(f"[facility-stage] file_id={file_id}, rows={nrows}")
        return

    if args.cmd == "facility-import":
        file_id = args.file_id
        n_org = upsert_orgs_from_stage(file_id)
        n_fac = upsert_facilities_from_stage(file_id)
        link_owner_relations(file_id)
        print(f"[facility-import] file_id={file_id}, org={n_org}, facility={n_fac}")
        return

    if args.cmd == "org":
        with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
            cur.execute("SELECT file_id FROM stage_file ORDER BY file_id DESC LIMIT 1")
            row = cur.fetchone()
        if not row:
            print("no staged file"); return
        print("latest file_id:", row[0])
        print("org upserted:", upsert_orgs_from_stage(row[0])); return

    if args.cmd == "facility":
        with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
            cur.execute("SELECT file_id FROM stage_file ORDER BY file_id DESC LIMIT 1")
            row = cur.fetchone()
        if not row:
            print("no staged file"); return
        print("latest file_id:", row[0])
        print("facility upserted:", upsert_facilities_from_stage(row[0])); return
    
    if args.cmd == "program-full":
        file_id = stage_local_file_record(args.path, dataset="program")
        df = read_excel_any(args.path)
        n = dataframe_to_stage_program(file_id, df)
        upsert_programs_from_stage(file_id)
        # 4) 프로그램 → 시설 매칭 (파일 기준, 원본 payload 사용)
        link_program_to_facility_by_file(file_id)

        # 5) 시설정보를 이용해 프로그램 유형 보정 (수영장 → '수영' 등)
        backfill_program_type_from_facility()

        print(f"[program-full] file_id={file_id}, staged={n}")
        return

    if args.cmd == "program-stage":
        file_id = stage_local_file_record(args.path, dataset="program")
        ensure_program_mst_sess_term_tables()
        df = read_excel_any(args.path)
        n = dataframe_to_stage_program(file_id, df)
        print(f"[program-stage] file_id={file_id}, rows={n}")
        return

    if args.cmd == "program-import":
        fid = args.file_id
        n = upsert_programs_from_stage(fid)
        link_program_to_facility_by_file()
        print(f"[program-import] file_id={fid}, programs={n}")
        return

    if args.cmd == "link":
        link_owner_relations(); return

if __name__ == "__main__":
    main()
