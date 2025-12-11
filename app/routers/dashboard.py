import os
import time
import re
from datetime import datetime  # ✅ timezone 제거
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import get_db  # 프로젝트 경로에 맞게 필요하면 수정

print(">>> loaded routers.dashboard (MySQL version):", __file__, flush=True)

router = APIRouter(prefix="/api", tags=["dashboard"])

# ---------------- Debug ----------------
DEBUG = os.getenv("DASHBOARD_DEBUG", "0").lower() in ("1", "true", "yes", "on")

def dlog(*args):
    if DEBUG:
        print(*args, flush=True)

def t_ms(t0):
    return round((time.perf_counter() - t0) * 1000, 1)

# ---------------- Env ----------------
WORKCELL_TABLE = os.getenv("WORKCELL_KPI_TABLE", "workcell_kpi")
DEVICE_TABLE = os.getenv("DEVICE_KPI_TABLE", "device_kpi")

RECENT_THRESHOLD_SEC = int(os.getenv("DASHBOARD_RECENT_SEC", "3600"))
OEE_IDEAL_CYCLE_SEC = float(os.getenv("OEE_IDEAL_CYCLE_SEC", "25"))
TAKT_TIME_SEC = float(os.getenv("TAKT_TIME_SEC", "30"))  # ✅ Takt time 기본 30초

STATIC_DEVICES = {
    "A1": ["robot01", "robot02", "conveyor01", "conveyor02"],
}

_DEV_CACHE: Dict[str, Dict[str, Any]] = {}
_DEV_CACHE_TTL = 300  # sec

_LOOKBACK_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$", re.I)


def parse_lookback_to_interval(s: str):
    m = _LOOKBACK_RE.match(s or "")
    if not m:
        return 6, "HOUR"
    n = int(m.group(1))
    u = m.group(2).lower()
    unit_map = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY", "w": "WEEK"}
    unit = unit_map.get(u, "DAY").upper()
    if unit == "DAY" and n > 7:
        n = 7
    if unit == "WEEK" and n >= 1:
        n, unit = 7, "DAY"
    return n, unit


def mysql_interval_expr(n: int, unit: str) -> str:
    return f"INTERVAL {max(1, int(n))} {unit.upper()}"


def _safe_float(v):
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _parse_ts(ts):
    """datetime 또는 string → datetime(로컬/naive) 로 변환 (UTC 취급 X)"""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        # DB에서 가져온 DATETIME 이라고 가정하고 그대로 사용
        return ts
    # 문자열 처리
    try:
        # "Z"가 붙어 있어도 그냥 떼고 naive 로 파싱
        s = str(ts).replace("Z", "")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _to_utc_iso(ts) -> Optional[str]:
    """
    이름은 그대로 두지만, 실제로는 그냥 로컬 datetime → ISO 문자열로만 변환.
    (UTC 변환 안 함)
    """
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.isoformat()
    t = _parse_ts(ts)
    return t.isoformat() if t else None


def _validate_workcell(workcell: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", workcell or ""):
        raise HTTPException(status_code=400, detail="Invalid workcell")
    return workcell


# ---------------- DB helpers ----------------
def _fetch_one(db: Session, sql: str, params: dict) -> Optional[dict]:
    dlog("[SQL ONE]\n", sql, "\nparams:", params)
    row = db.execute(text(sql), params).mappings().first()
    return dict(row) if row else None


def _fetch_all(db: Session, sql: str, params: dict) -> List[dict]:
    dlog("[SQL ALL]\n", sql, "\nparams:", params)
    rows = db.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]


def _get_latest_workcell_time(db: Session, workcell: str):
    sql = f"""
        SELECT MAX(time) AS max_time
        FROM {WORKCELL_TABLE}
        WHERE workcell = :workcell
    """
    row = _fetch_one(db, sql, {"workcell": workcell})
    latest = row["max_time"] if row and row["max_time"] is not None else None
    return latest, 0.0


# ---------------- ROUTES ----------------
@router.get("/_whoami")
def whoami():
    return {
        "module_file": __file__,
        "DEBUG": DEBUG,
        "WORKCELL_TABLE": WORKCELL_TABLE,
        "DEVICE_TABLE": DEVICE_TABLE,
        "RECENT_THRESHOLD_SEC": RECENT_THRESHOLD_SEC,
        "OEE_IDEAL_CYCLE_SEC": OEE_IDEAL_CYCLE_SEC,
        "TAKT_TIME_SEC": TAKT_TIME_SEC,  # ✅ 확인용
    }


# ---------------- Dashboard KPI ----------------
@router.get("/dashboard")
def get_dashboard(
    workcell: str = Query(...),
    lookback: str = Query("6h"),
    force: int = Query(0),
    db: Session = Depends(get_db),
):
    workcell = _validate_workcell(workcell)
    dlog("\n[/api/dashboard] params:", {"workcell": workcell, "lookback": lookback})

    # lookback parsing
    if force == 1:
        m = _LOOKBACK_RE.match(lookback or "")
        if m:
            n0 = int(m.group(1))
            unit0 = {
                "s": "SECOND", "m": "MINUTE", "h": "HOUR",
                "d": "DAY", "w": "WEEK",
            }.get(m.group(2).lower(), "DAY")
        else:
            n0, unit0 = 6, "HOUR"
    else:
        n0, unit0 = parse_lookback_to_interval(lookback)

    interval_mysql = mysql_interval_expr(n0, unit0)
    window_sec = n0 * {"SECOND":1,"MINUTE":60,"HOUR":3600,"DAY":86400,"WEEK":604800}.get(unit0,3600)

    sql = f"""
        SELECT time, total_count, bad_count, cycle_time, queue_time, wip
        FROM {WORKCELL_TABLE}
        WHERE workcell = :workcell
          AND time > NOW(6) - {interval_mysql}
        ORDER BY time DESC
        LIMIT 1
    """

    row = _fetch_one(db, sql, {"workcell": workcell})
    if not row:
        return {
            "total_count": None,
            "yield_pct": None,
            "cycle_time_s": None,
            "takt_adherence_pct": None,
            "throughput_uph": None,
            "queue_time_s": None,
            "wip_ct": None,
            "run_time_h": None,
            "performance_pct": None,
            "quality_ratio_pct": None,
            "availability_pct": None,
            "oee_pct": None,
            "_source": "no_data_recent",
        }

    ts = _parse_ts(row.get("time"))
    now_local = datetime.now()  # ✅ UTC X, 로컬 시간 기준
    age_sec = (now_local - ts).total_seconds() if ts else None

    if age_sec is not None and age_sec > RECENT_THRESHOLD_SEC:
        return {
            "total_count": None,
            "yield_pct": None,
            "cycle_time_s": None,
            "takt_adherence_pct": None,
            "throughput_uph": None,
            "queue_time_s": None,
            "wip_ct": None,
            "run_time_h": None,
            "performance_pct": None,
            "quality_ratio_pct": None,
            "availability_pct": None,
            "oee_pct": None,
            "_source": "too_old",
        }

    total = _safe_float(row.get("total_count"))
    bad = _safe_float(row.get("bad_count"))
    cycle_time = _safe_float(row.get("cycle_time"))
    queue_time = _safe_float(row.get("queue_time"))
    wip = _safe_float(row.get("wip"))

    # ---- window 기준 run_time ----
    run_time_h = (window_sec / 3600.0) if window_sec else None
    availability_pct = 100.0 if total is not None else None

    # ---- Performance (기존 로직 유지) ----
    performance_pct = None
    if total is not None and window_sec > 0 and cycle_time:
        performance_pct = OEE_IDEAL_CYCLE_SEC * 100.0 / cycle_time

    # ---- Quality ----
    quality_ratio_pct = None
    if total and total > 0 and bad is not None:
        good = max(total - bad, 0)
        quality_ratio_pct = (good / total) * 100.0

    # ---- Throughput ----
    throughput_uph = None
    if total and run_time_h:
        throughput_uph = total / run_time_h

    # ---- Takt Adherence ----
    takt_adherence_pct = None
    if cycle_time and cycle_time > 0:
        takt_adherence_pct = TAKT_TIME_SEC * 100.0 / cycle_time

    # ---- OEE ----
    oee_pct = None
    if availability_pct and performance_pct and quality_ratio_pct:
        oee_pct = (
            (availability_pct/100.0) *
            (performance_pct/100.0) *
            (quality_ratio_pct/100.0) * 100.0
        )

    return {
        "total_count": total,
        "yield_pct": round(quality_ratio_pct, 2) if quality_ratio_pct else None,
        "cycle_time_s": round(cycle_time, 2) if cycle_time else None,
        "takt_adherence_pct": round(takt_adherence_pct, 2) if takt_adherence_pct else None,
        "throughput_uph": round(throughput_uph, 1) if throughput_uph else None,
        "queue_time_s": round(queue_time, 2) if queue_time else None,
        "wip_ct": round(wip, 1) if wip else None,
        "run_time_h": round(run_time_h, 2) if run_time_h else None,
        "performance_pct": round(performance_pct, 2) if performance_pct else None,
        "quality_ratio_pct": round(quality_ratio_pct, 2) if quality_ratio_pct else None,
        "availability_pct": round(availability_pct, 2) if availability_pct else None,
        "oee_pct": round(oee_pct, 2) if oee_pct else None,
        "_source": "mysql+derived",
    }


# ---------------- Dashboard Equipment Status ----------------
@router.get("/dashboard_devices")
def dashboard_devices(
    workcell: str = Query(...),
    db: Session = Depends(get_db),
):
    workcell = _validate_workcell(workcell)

    # ---- Cache ----
    now_ts = time.time()
    c = _DEV_CACHE.get(workcell)
    if c and (now_ts - c.get("ts", 0) < _DEV_CACHE_TTL):
        return {
            "devices": c.get("names", []),
            "_source": "mysql(cache)",
            "_interval": c.get("interval", "ALL"),
            "status": c.get("status", {}),
            "_status_interval": c.get("status_interval", "1HOUR"),
        }

    # ---- Distinct device names ----
    sql_names = f"""
        SELECT DISTINCT device_name
        FROM {DEVICE_TABLE}
        WHERE device_name IS NOT NULL
          AND device_name <> ''
        ORDER BY device_name
    """
    names_rows = _fetch_all(db, sql_names, {})
    names = sorted({r["device_name"] for r in names_rows})

    # ---- status (최근 1시간) ----
    status_interval_mysql = mysql_interval_expr(1, "HOUR")
    sql_status = f"""
        SELECT d.device_name, d.time, d.status
        FROM {DEVICE_TABLE} AS d
        JOIN (
            SELECT device_name, MAX(time) AS max_time
            FROM {DEVICE_TABLE}
            WHERE workcell = :workcell
              AND time > NOW(6) - {status_interval_mysql}
            GROUP BY device_name
        ) AS latest
          ON latest.device_name = d.device_name
         AND latest.max_time   = d.time
        WHERE d.workcell = :workcell
        ORDER BY d.device_name
    """
    status_rows = _fetch_all(db, sql_status, {"workcell": workcell})

    now_local = datetime.now()  # ✅ 로컬 시간
    status_map = {}

    for r in status_rows:
        dev = r["device_name"]
        ts = _parse_ts(r["time"])
        st = r.get("status")

        if not dev or not ts:
            continue

        age_sec = (now_local - ts).total_seconds() if ts else None
        st_int = int(st) if st is not None else None
        recent = (
            age_sec is not None
            and age_sec <= RECENT_THRESHOLD_SEC
            and st_int in (1, 2, 3)
        )

        status_map[dev] = {
            "status": st_int if recent else None,
            "time": ts.isoformat() if ts else None,
            "age_sec": age_sec,
            "recent": recent,
        }

    # ---- Cache 저장 ----
    if names:
        _DEV_CACHE[workcell] = {
            "names": names,
            "ts": now_ts,
            "interval": "ALL",
            "status": status_map,
            "status_interval": "1HOUR",
        }
        return {
            "devices": names,
            "_source": "mysql(all-distinct+status)",
            "status": status_map,
            "_status_interval": "1HOUR",
        }

    # ---- fallback: static devices ----
    static_names = STATIC_DEVICES.get(workcell)
    if static_names:
        return {
            "devices": static_names,
            "_source": "static-default",
            "status": {},
        }

    return {
        "devices": [],
        "_source": "mysql(empty)",
        "status": {},
    }


# alias
@router.get("/devices")
def devices_alias(workcell: str = Query(...), db: Session = Depends(get_db)):
    return dashboard_devices(workcell=workcell, db=db)


# ---------------- Analytics ----------------
@router.get("/analytics")
def get_analytics(
    workcell: str = Query(...),
    time_range: str = Query(
        "24h",
        regex="^(24h|7d|30d)$",
        alias="range"
    ),
    db: Session = Depends(get_db),
):
    workcell = _validate_workcell(workcell)

    # 최신 anchor time (DB 시간 그대로 사용)
    latest_ts, latest_query_ms = _get_latest_workcell_time(db, workcell)
    anchor_iso = _to_utc_iso(latest_ts)  # 이름만 utc, 실제로는 그냥 iso string
    dlog("[/api/analytics] anchor:", anchor_iso)

    # time_range → interval
    if time_range == "7d":
        n, unit = 7, "DAY"
    elif time_range == "30d":
        n, unit = 30, "DAY"
    else:
        n, unit = 24, "HOUR"

    interval_mysql = mysql_interval_expr(n, unit)
    dlog(f"[/api/analytics] interval={interval_mysql}")

    # ---- workcell_kpi 시계열 ----
    sql_wc = f"""
        SELECT time, total_count, bad_count, cycle_time
        FROM {WORKCELL_TABLE}
        WHERE workcell = :workcell
          AND time > NOW(6) - {interval_mysql}
        ORDER BY time
    """
    wc_rows = _fetch_all(db, sql_wc, {"workcell": workcell})
    wc_series = {
        "time": [],
        "oee_pct": [],
        "availability_pct": [],
        "performance_pct": [],
        "quality_ratio_pct": [],
        "throughput_uph": [],
        "cycle_time_s": [],
        "takt_adherence_pct": [],  # ✅ Takt 시계열 추가
    }

    times = []
    total_arr = []
    bad_arr = []
    cycle_arr = []

    for r in wc_rows:
        ts = _parse_ts(r["time"])
        wc_series["time"].append(ts.isoformat() if ts else None)
        times.append(ts)
        total_arr.append(_safe_float(r["total_count"]))
        bad_arr.append(_safe_float(r["bad_count"]))
        cycle_arr.append(_safe_float(r["cycle_time"]))

    # ---- run_time 계산: (max time - min time) ----
    real_first = next((t for t in times if t is not None), None)
    real_last = next((t for t in reversed(times) if t is not None), None)

    if real_first and real_last:
        run_time_sec = (real_last - real_first).total_seconds()
        if run_time_sec < 0:
            run_time_sec = None
    else:
        run_time_sec = None

    # ---- Derived 계산 ----
    for idx, ts in enumerate(times):
        total = total_arr[idx]
        bad = bad_arr[idx]
        cycle = cycle_arr[idx]

        # single-point availability
        availability_pct = 100.0 if total is not None else None

        # performance using actual run_time_sec
        performance_pct = None
        if total is not None and run_time_sec and run_time_sec > 0 and cycle:
            performance_pct = OEE_IDEAL_CYCLE_SEC * 100.0 / cycle

        # quality
        quality_pct = None
        if total and total > 0 and bad is not None:
            good = max(total - bad, 0)
            quality_pct = (good / total) * 100.0

        # throughput
        throughput = None
        if total and run_time_sec and run_time_sec > 0:
            throughput = total / (run_time_sec / 3600.0)

        # OEE
        oee_pct = None
        if availability_pct and performance_pct and quality_pct:
            oee_pct = (
                (availability_pct/100) *
                (performance_pct/100) *
                (quality_pct/100) * 100.0
            )

        # Takt adherence
        takt_pct = None
        if cycle and cycle > 0:
            takt_pct = TAKT_TIME_SEC * 100.0 / cycle

        wc_series["availability_pct"].append(
            round(availability_pct, 2) if availability_pct else None
        )
        wc_series["performance_pct"].append(
            round(performance_pct, 2) if performance_pct else None
        )
        wc_series["quality_ratio_pct"].append(
            round(quality_pct, 2) if quality_pct else None
        )
        wc_series["throughput_uph"].append(
            round(throughput, 2) if throughput else None
        )
        wc_series["oee_pct"].append(
            round(oee_pct, 2) if oee_pct else None
        )
        wc_series["cycle_time_s"].append(
            round(cycle, 2) if cycle else None
        )
        wc_series["takt_adherence_pct"].append(
            round(takt_pct, 2) if takt_pct else None
        )

    # ---- device 목록 ----
    sql_dev = f"""
        SELECT DISTINCT device_name
        FROM {DEVICE_TABLE}
        WHERE workcell = :workcell
          AND device_name IS NOT NULL
          AND device_name <> ''
    """
    dev_rows = _fetch_all(db, sql_dev, {"workcell": workcell})
    dev_names = sorted({r["device_name"] for r in dev_rows})

    # ---- device_kpi cycle distribution ----
    if not dev_names:
        cycle_dist = {}
    else:
        placeholders = ", ".join(f":d{i}" for i in range(len(dev_names)))
        sql_cd = f"""
            SELECT device_name, ct
            FROM {DEVICE_TABLE}
            WHERE workcell = :workcell
              AND device_name IN ({placeholders})
              AND ct IS NOT NULL
              AND time > NOW(6) - {interval_mysql}
        """
        params = {"workcell": workcell}
        params.update({f"d{i}": name for i, name in enumerate(dev_names)})

        cd_rows = _fetch_all(db, sql_cd, params)

        cycle_dist = {d: [] for d in dev_names}
        for r in cd_rows:
            name = r["device_name"]
            val = _safe_float(r["ct"])
            if val is not None:
                cycle_dist[name].append(val)

    return {
        "workcell_ts": wc_series,
        "cycle_dist": cycle_dist,
        "_meta": {
            "workcell": workcell,
            "range": time_range,
            "interval_expr": interval_mysql,
            "anchor_time": anchor_iso,
            "latest_time_raw": str(latest_ts) if latest_ts else None,
            "device_names": dev_names,
            "ideal_cycle_time": OEE_IDEAL_CYCLE_SEC,
            "takt_time_sec": TAKT_TIME_SEC,   # ✅ 메타에도 표시
            "run_time_sec": run_time_sec,
        },
    }