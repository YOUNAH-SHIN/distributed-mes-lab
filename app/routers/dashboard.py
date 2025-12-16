# app/routers/dashboard.py  (study / sanitized version)
import os
import time
import re
from datetime import datetime  # ✅ timezone 제거
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import get_db

print(">>> loaded routers.dashboard (MySQL study version):", __file__, flush=True)

router = APIRouter(prefix="/api", tags=["dashboard"])

# ---------------- Debug ----------------
DEBUG = os.getenv("DASHBOARD_DEBUG", "0").lower() in ("1", "true", "yes", "on")


def dlog(*args):
    if DEBUG:
        print(*args, flush=True)


def t_ms(t0):
    return round((time.perf_counter() - t0) * 1000, 1)


LINE_TABLE = os.getenv("LINE_KPI_TABLE", "line_summary")
NODE_TABLE = os.getenv("NODE_KPI_TABLE", "node_snapshot")

RECENT_THRESHOLD_SEC = int(os.getenv("DASHBOARD_RECENT_SEC", "3600"))

IDEAL_LATENCY_SEC = float(os.getenv("IDEAL_LATENCY_SEC", "25"))
TARGET_STEP_SEC = float(os.getenv("TARGET_STEP_SEC", "30"))

# 제출용: static fallback 예시 유지
STATIC_NODES = {
    "A1": ["robot-a", "robot-b", "conveyor-a", "conveyor-b"],
}

_NODE_CACHE: Dict[str, Dict[str, Any]] = {}
_NODE_CACHE_TTL = 300  # sec

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
    """datetime 또는 string → datetime(naive) 로 변환"""
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts
    try:
        s = str(ts).replace("Z", "")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _to_iso(ts) -> Optional[str]:
    if ts is None:
        return None
    if isinstance(ts, datetime):
        return ts.isoformat()
    t = _parse_ts(ts)
    return t.isoformat() if t else None


def _validate_line(line_id: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", line_id or ""):
        raise HTTPException(status_code=400, detail="Invalid line_id")
    return line_id


# ---------------- DB helpers ----------------
def _fetch_one(db: Session, sql: str, params: dict) -> Optional[dict]:
    dlog("[SQL ONE]\n", sql, "\nparams:", params)
    row = db.execute(text(sql), params).mappings().first()
    return dict(row) if row else None


def _fetch_all(db: Session, sql: str, params: dict) -> List[dict]:
    dlog("[SQL ALL]\n", sql, "\nparams:", params)
    rows = db.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]


def _get_latest_line_time(db: Session, line_id: str):
    sql = f"""
        SELECT MAX(recorded_at) AS max_time
        FROM {LINE_TABLE}
        WHERE line_id = :line_id
    """
    row = _fetch_one(db, sql, {"line_id": line_id})
    latest = row["max_time"] if row and row.get("max_time") is not None else None
    return latest, 0.0


# ---------------- ROUTES ----------------
@router.get("/_whoami")
def whoami():
    return {
        "module_file": __file__,
        "DEBUG": DEBUG,
        "LINE_TABLE": LINE_TABLE,
        "NODE_TABLE": NODE_TABLE,
        "RECENT_THRESHOLD_SEC": RECENT_THRESHOLD_SEC,
        "IDEAL_LATENCY_SEC": IDEAL_LATENCY_SEC,
        "TARGET_STEP_SEC": TARGET_STEP_SEC,
    }


# ---------------- Dashboard KPI ----------------
@router.get("/dashboard")
def get_dashboard(
    line_id: str = Query(..., description="라인/워크셀 ID (예: A1)"),
    lookback: str = Query("6h", description="조회 윈도우 예: 30m, 6h, 1d"),
    force: int = Query(0, description="1이면 lookback을 그대로(최대 제한만) 적용"),
    db: Session = Depends(get_db),
):
    line_id = _validate_line(line_id)
    dlog("\n[/api/dashboard] params:", {"line_id": line_id, "lookback": lookback})

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
    window_sec = n0 * {"SECOND": 1, "MINUTE": 60, "HOUR": 3600, "DAY": 86400, "WEEK": 604800}.get(unit0, 3600)

    sql = f"""
        SELECT
          recorded_at,
          units_total,
          units_scrap,
          latency_s,
          queue_delay_s,
          wip_units,
          energy_kwh
        FROM {LINE_TABLE}
        WHERE line_id = :line_id
          AND recorded_at > NOW(6) - {interval_mysql}
        ORDER BY recorded_at DESC
        LIMIT 1
    """

    row = _fetch_one(db, sql, {"line_id": line_id})
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
            "energy_kwh": None,          # ✅ 추가 반환
            "_source": "no_data_recent",
        }

    ts = _parse_ts(row.get("recorded_at"))
    now_local = datetime.now()
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
            "energy_kwh": None,
            "_source": "too_old",
        }

    total = _safe_float(row.get("units_total"))
    scrap = _safe_float(row.get("units_scrap"))
    latency = _safe_float(row.get("latency_s"))
    queue_delay = _safe_float(row.get("queue_delay_s"))
    wip_units = _safe_float(row.get("wip_units"))
    energy_kwh = _safe_float(row.get("energy_kwh"))  # ✅ 추가 컬럼

    # ---- window 기준 run_time ----
    run_time_h = (window_sec / 3600.0) if window_sec else None

    # 단순 availability (데모): 값이 있으면 100
    availability_pct = 100.0 if total is not None else None

    # ---- Performance (latency 기반) ----
    performance_pct = None
    if total is not None and window_sec > 0 and latency:
        performance_pct = IDEAL_LATENCY_SEC * 100.0 / latency

    # ---- Quality ----
    quality_ratio_pct = None
    if total and total > 0 and scrap is not None:
        good = max(total - scrap, 0)
        quality_ratio_pct = (good / total) * 100.0

    # ---- Throughput ----
    throughput_uph = None
    if total and run_time_h:
        throughput_uph = total / run_time_h

    # ---- Takt Adherence ----
    takt_adherence_pct = None
    if latency and latency > 0:
        takt_adherence_pct = TARGET_STEP_SEC * 100.0 / latency

    # ---- OEE ----
    oee_pct = None
    if availability_pct and performance_pct and quality_ratio_pct:
        oee_pct = (
            (availability_pct / 100.0)
            * (performance_pct / 100.0)
            * (quality_ratio_pct / 100.0)
            * 100.0
        )

    return {
        "total_count": total,
        "yield_pct": round(quality_ratio_pct, 2) if quality_ratio_pct is not None else None,
        "cycle_time_s": round(latency, 2) if latency is not None else None,
        "takt_adherence_pct": round(takt_adherence_pct, 2) if takt_adherence_pct is not None else None,
        "throughput_uph": round(throughput_uph, 1) if throughput_uph is not None else None,
        "queue_time_s": round(queue_delay, 2) if queue_delay is not None else None,
        "wip_ct": round(wip_units, 1) if wip_units is not None else None,
        "run_time_h": round(run_time_h, 2) if run_time_h is not None else None,
        "performance_pct": round(performance_pct, 2) if performance_pct is not None else None,
        "quality_ratio_pct": round(quality_ratio_pct, 2) if quality_ratio_pct is not None else None,
        "availability_pct": round(availability_pct, 2) if availability_pct is not None else None,
        "oee_pct": round(oee_pct, 2) if oee_pct is not None else None,
        "energy_kwh": round(energy_kwh, 3) if energy_kwh is not None else None,  # ✅ 추가 반환
        "_source": "mysql+derived(study)",
        "_time": _to_iso(ts),
        "_age_sec": age_sec,
    }


# ---------------- Dashboard Equipment Status ----------------
@router.get("/dashboard_devices")
def dashboard_devices(
    line_id: str = Query(..., description="라인/워크셀 ID (예: A1)"),
    db: Session = Depends(get_db),
):
    line_id = _validate_line(line_id)

    # ---- Cache ----
    now_ts = time.time()
    c = _NODE_CACHE.get(line_id)
    if c and (now_ts - c.get("ts", 0) < _NODE_CACHE_TTL):
        return {
            "devices": c.get("names", []),
            "_source": "mysql(cache)",
            "status": c.get("status", {}),
            "_status_interval": c.get("status_interval", "1HOUR"),
        }

    sql_names = f"""
        SELECT DISTINCT node_name
        FROM {NODE_TABLE}
        WHERE node_name IS NOT NULL
          AND node_name <> ''
        ORDER BY node_name
    """
    names_rows = _fetch_all(db, sql_names, {})
    names = sorted({r["node_name"] for r in names_rows if r.get("node_name")})

    # ---- status (최근 1시간) ----
    status_interval_mysql = mysql_interval_expr(1, "HOUR")
    sql_status = f"""
        SELECT s.node_name, s.observed_at, s.health_code
        FROM {NODE_TABLE} AS s
        JOIN (
            SELECT node_name, MAX(observed_at) AS max_time
            FROM {NODE_TABLE}
            WHERE line_id = :line_id
              AND observed_at > NOW(6) - {status_interval_mysql}
            GROUP BY node_name
        ) AS latest
          ON latest.node_name = s.node_name
         AND latest.max_time  = s.observed_at
        WHERE s.line_id = :line_id
        ORDER BY s.node_name
    """
    status_rows = _fetch_all(db, sql_status, {"line_id": line_id})

    now_local = datetime.now()
    status_map: Dict[str, Any] = {}

    # health_code 예시 매핑(제출용):
    # 1=healthy, 2=warning, 3=down (기존 status 1/2/3 동일 컨셉)
    for r in status_rows:
        node = r.get("node_name")
        ts = _parse_ts(r.get("observed_at"))
        st = r.get("health_code")

        if not node or not ts:
            continue

        age_sec = (now_local - ts).total_seconds()
        try:
            st_int = int(st) if st is not None else None
        except Exception:
            st_int = None

        recent = (
            age_sec <= RECENT_THRESHOLD_SEC
            and st_int in (1, 2, 3)
        )

        status_map[str(node)] = {
            "status": st_int if recent else None,
            "time": ts.isoformat(),
            "age_sec": age_sec,
            "recent": recent,
        }

    # ---- Cache 저장 ----
    if names:
        _NODE_CACHE[line_id] = {
            "names": names,
            "ts": now_ts,
            "status": status_map,
            "status_interval": "1HOUR",
        }
        return {
            "devices": names,
            "_source": "mysql(all-distinct+status)",
            "status": status_map,
            "_status_interval": "1HOUR",
        }

    # ---- fallback: static nodes ----
    static_names = STATIC_NODES.get(line_id)
    if static_names:
        return {"devices": static_names, "_source": "static-default", "status": {}}

    return {"devices": [], "_source": "mysql(empty)", "status": {}}


# alias (기존 호환)
@router.get("/devices")
def devices_alias(line_id: str = Query(...), db: Session = Depends(get_db)):
    return dashboard_devices(line_id=line_id, db=db)


# ---------------- Analytics (study) ----------------
@router.get("/analytics")
def get_analytics(
    line_id: str = Query(...),
    time_range: str = Query("24h", regex="^(24h|7d|30d)$", alias="range"),
    db: Session = Depends(get_db),
):
    line_id = _validate_line(line_id)

    latest_ts, _ = _get_latest_line_time(db, line_id)
    anchor_iso = _to_iso(latest_ts)
    dlog("[/api/analytics] anchor:", anchor_iso)

    if time_range == "7d":
        n, unit = 7, "DAY"
    elif time_range == "30d":
        n, unit = 30, "DAY"
    else:
        n, unit = 24, "HOUR"

    interval_mysql = mysql_interval_expr(n, unit)

    sql_line = f"""
        SELECT recorded_at, units_total, units_scrap, latency_s, energy_kwh
        FROM {LINE_TABLE}
        WHERE line_id = :line_id
          AND recorded_at > NOW(6) - {interval_mysql}
        ORDER BY recorded_at
    """
    rows = _fetch_all(db, sql_line, {"line_id": line_id})

    series = {
        "time": [],
        "quality_pct": [],
        "performance_pct": [],
        "availability_pct": [],
        "oee_pct": [],
        "throughput_uph": [],
        "latency_s": [],
        "takt_adherence_pct": [],
        "energy_kwh": [],  # ✅ 추가
    }

    times: List[Optional[datetime]] = []
    totals: List[Optional[float]] = []
    scraps: List[Optional[float]] = []
    latencies: List[Optional[float]] = []

    for r in rows:
        ts = _parse_ts(r.get("recorded_at"))
        times.append(ts)
        series["time"].append(ts.isoformat() if ts else None)

        totals.append(_safe_float(r.get("units_total")))
        scraps.append(_safe_float(r.get("units_scrap")))
        latencies.append(_safe_float(r.get("latency_s")))
        series["energy_kwh"].append(_safe_float(r.get("energy_kwh")))

    real_first = next((t for t in times if t is not None), None)
    real_last = next((t for t in reversed(times) if t is not None), None)
    run_time_sec = (real_last - real_first).total_seconds() if (real_first and real_last) else None
    if run_time_sec is not None and run_time_sec < 0:
        run_time_sec = None

    for i, _ in enumerate(times):
        total = totals[i]
        scrap = scraps[i]
        latency = latencies[i]

        availability = 100.0 if total is not None else None

        performance = None
        if total is not None and latency and latency > 0:
            performance = IDEAL_LATENCY_SEC * 100.0 / latency

        quality = None
        if total and total > 0 and scrap is not None:
            quality = (max(total - scrap, 0) / total) * 100.0

        throughput = None
        if total and run_time_sec and run_time_sec > 0:
            throughput = total / (run_time_sec / 3600.0)

        takt = None
        if latency and latency > 0:
            takt = TARGET_STEP_SEC * 100.0 / latency

        oee = None
        if availability and performance and quality:
            oee = (availability / 100) * (performance / 100) * (quality / 100) * 100.0

        series["availability_pct"].append(round(availability, 2) if availability is not None else None)
        series["performance_pct"].append(round(performance, 2) if performance is not None else None)
        series["quality_pct"].append(round(quality, 2) if quality is not None else None)
        series["throughput_uph"].append(round(throughput, 2) if throughput is not None else None)
        series["oee_pct"].append(round(oee, 2) if oee is not None else None)
        series["latency_s"].append(round(latency, 2) if latency is not None else None)
        series["takt_adherence_pct"].append(round(takt, 2) if takt is not None else None)

    # node_names
    sql_nodes = f"""
        SELECT DISTINCT node_name
        FROM {NODE_TABLE}
        WHERE line_id = :line_id
          AND node_name IS NOT NULL
          AND node_name <> ''
    """
    node_rows = _fetch_all(db, sql_nodes, {"line_id": line_id})
    node_names = sorted({r["node_name"] for r in node_rows if r.get("node_name")})

    # latency distribution per node (기존 ct 분포 개념 유지)
    latency_dist: Dict[str, List[float]] = {}
    if node_names:
        placeholders = ", ".join(f":n{i}" for i in range(len(node_names)))
        sql_dist = f"""
            SELECT node_name, latency_s
            FROM {NODE_TABLE}
            WHERE line_id = :line_id
              AND node_name IN ({placeholders})
              AND latency_s IS NOT NULL
              AND observed_at > NOW(6) - {interval_mysql}
        """
        params = {"line_id": line_id}
        params.update({f"n{i}": name for i, name in enumerate(node_names)})
        dist_rows = _fetch_all(db, sql_dist, params)

        latency_dist = {n: [] for n in node_names}
        for r in dist_rows:
            n = r.get("node_name")
            v = _safe_float(r.get("latency_s"))
            if n and v is not None:
                latency_dist[str(n)].append(v)

    return {
        "line_ts": series,
        "latency_dist": latency_dist,
        "_meta": {
            "line_id": line_id,
            "range": time_range,
            "interval_expr": interval_mysql,
            "anchor_time": anchor_iso,
            "latest_time_raw": str(latest_ts) if latest_ts else None,
            "node_names": node_names,
            "ideal_latency_sec": IDEAL_LATENCY_SEC,
            "target_step_sec": TARGET_STEP_SEC,
            "run_time_sec": run_time_sec,
        },
    }
