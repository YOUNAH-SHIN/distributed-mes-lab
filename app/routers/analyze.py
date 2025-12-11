import os
import re
from typing import List, Dict, Any, Optional, Set, Tuple

from datetime import datetime, timedelta  # ✅ timezone 제거

from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import get_db  # dashboard.py 와 동일한 DB 세션 사용

print(">>> loaded routers.analyze (MySQL version):", __file__, flush=True)

# /api/analyze 로 분리
router = APIRouter(prefix="/api/analyze", tags=["analyze"])

# ---- Debug toggle ----
DEBUG = os.getenv("ANALYZE_DEBUG", "0").lower() in ("1", "true", "yes", "on")


def dlog(*args: Any) -> None:
    if DEBUG:
        print(*args, flush=True)


# ---- MySQL env ----
DEVICE_TABLE = os.getenv("DEVICE_KPI_TABLE", "device_kpi")


def _validate_workcell(workcell: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", workcell or ""):
        raise HTTPException(status_code=400, detail="Invalid workcell")
    return workcell


# ---------------- Range → window 매핑 ----------------
def _window_for_range(range_str: str) -> timedelta:
    """
    range 문자열을 실제 시간 window 로 변환.
    지원 값:
      - "30m"  → 최근 30분
      - "1day" → 최근 1일
      - "7day" → 최근 7일
    그 외 값은 기본 30분으로 처리
    """
    key = (range_str or "").strip().lower()

    if key == "30m":
        return timedelta(minutes=30)
    if key == "1day":
        return timedelta(days=1)
    if key == "7day":
        return timedelta(days=7)

    # 알 수 없는 값이면 안전하게 30분
    return timedelta(minutes=30)


# ---------------- DB helpers (SQLAlchemy + text) ----------------
def _fetch_all(db: Session, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    dlog("[ANALYZE SQL ALL]\n", sql, "\nparams:", params)
    rows = db.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]


# ------------------------------------------------------------------------
# GET /api/analyze/device_types  → {"types":[...]}
#   - MySQL device_kpi 에서 DISTINCT device_type
# ------------------------------------------------------------------------
@router.get("/device_types")
def list_device_types(
    workcell: str = Query(..., description="워크셀 ID (예: A1)"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    workcell = _validate_workcell(workcell)

    sql = f"""
        SELECT DISTINCT device_type
        FROM {DEVICE_TABLE}
        WHERE workcell = :workcell
          AND device_type IS NOT NULL
          AND device_type <> ''
        ORDER BY device_type
    """
    rows = _fetch_all(db, sql, {"workcell": workcell})
    types: List[str] = [str(r["device_type"]) for r in rows if r.get("device_type")]

    dlog("[/api/analyze/device_types] count:", len(types), "types:", types)
    return {
        "types": types,
        "_source": "mysql",
        "_table": DEVICE_TABLE,
        "_workcell": workcell,
    }


# ------------------------------------------------------------------------
# GET /api/analyze/device_names  → {"devices":[...]}
#   - 옵션: device_type 로 필터
# ------------------------------------------------------------------------
@router.get("/device_names")
def list_device_names(
    workcell: str = Query(..., description="워크셀 ID (예: A1)"),
    device_type: Optional[str] = Query(
        None,
        description="필터: 특정 device_type 만",
    ),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    workcell = _validate_workcell(workcell)

    base_sql = f"""
        SELECT DISTINCT device_name
        FROM {DEVICE_TABLE}
        WHERE workcell = :workcell
          AND device_name IS NOT NULL
          AND device_name <> ''
    """
    params: Dict[str, Any] = {"workcell": workcell}

    if device_type:
        base_sql += " AND device_type = :device_type"
        params["device_type"] = device_type

    base_sql += " ORDER BY device_name"

    rows = _fetch_all(db, base_sql, params)
    names: List[str] = [str(r["device_name"]) for r in rows if r.get("device_name")]

    dlog(
        "[/api/analyze/device_names] count:",
        len(names),
        "workcell:",
        workcell,
        "device_type:",
        device_type or "(none)",
        "names:",
        names,
    )

    return {
        "devices": names,
        "_source": "mysql",
        "_table": DEVICE_TABLE,
        "_workcell": workcell,
        "_device_type": device_type,
    }


# ------------------------------------------------------------------------
# GET /api/analyze/devices
#   → {"devices":[{"device_type": "...", "device_name": "..."}, ...]}
#   - type / name 둘 다 한 번에 받고 싶을 때 사용
# ------------------------------------------------------------------------
@router.get("/devices")
def list_devices(
    workcell: str = Query(..., description="워크셀 ID (예: A1)"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    workcell = _validate_workcell(workcell)

    sql = f"""
        SELECT DISTINCT device_type, device_name
        FROM {DEVICE_TABLE}
        WHERE workcell = :workcell
          AND device_name IS NOT NULL
          AND device_name <> ''
          AND device_type IS NOT NULL
          AND device_type <> ''
        ORDER BY device_type, device_name
    """
    rows = _fetch_all(db, sql, {"workcell": workcell})

    devices: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()

    for r in rows:
        t = str(r.get("device_type") or "")
        n = str(r.get("device_name") or "")
        if not t or not n:
            continue

        key = (t, n)
        if key in seen:
            continue
        seen.add(key)

        devices.append({"device_type": t, "device_name": n})

    dlog("[/api/analyze/devices] count:", len(devices), "devices:", devices)

    return {
        "devices": devices,
        "_source": "mysql",
        "_table": DEVICE_TABLE,
        "_workcell": workcell,
    }


# ------------------------------------------------------------------------
# GET /api/analyze/timeseries
# ------------------------------------------------------------------------
@router.get("/timeseries")
def timeseries(
    workcell: str = Query(..., description="워크셀 ID (예: A1)"),
    metric: str = Query(
        "defect_rate",
        description='metric: "defect_rate" 또는 "cycle" (cycle time)',
    ),
    range: str = Query(
        "30m",
        alias="range",
        description='"30m", "1day", "7day" 중 하나',
    ),
    device_types: Optional[str] = Query(
        None,
        description="쉼표로 구분된 device_type 목록 (예: Conveyor,Robot)",
    ),
    device_names: Optional[str] = Query(
        None,
        description="쉼표로 구분된 device_name 목록 (예: X_Robot,Y_Robot)",
    ),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    import traceback  # 내부 디버깅용

    try:
        workcell = _validate_workcell(workcell)

        metric_norm = metric.strip().lower()
        if metric_norm not in ("defect_rate", "cycle"):
            raise HTTPException(
                status_code=400,
                detail="metric must be 'defect_rate' or 'cycle'",
            )

        # ----- 1) 앵커 시간: 이 워크셀의 최신 time (DB 기준) -----
        anchor_sql = f"""
            SELECT MAX(time) AS max_time
            FROM {DEVICE_TABLE}
            WHERE workcell = :workcell
        """
        anchor_rows = _fetch_all(db, anchor_sql, {"workcell": workcell})
        anchor_time = (
            anchor_rows[0]["max_time"]
            if anchor_rows and anchor_rows[0].get("max_time") is not None
            else None
        )

        if anchor_time is None:
            dlog("[/api/analyze/timeseries] no data for workcell:", workcell)
            return {
                "workcell": workcell,
                "metric": metric_norm,
                "range": range,
                "series": [],
                "_source": "mysql",
                "_table": DEVICE_TABLE,
                "anchor_time": None,
                "t_from": None,
                "t_to": None,
            }

        if not isinstance(anchor_time, datetime):
            try:
                anchor_time = datetime.fromisoformat(str(anchor_time))
            except Exception:
                dlog("[/api/analyze/timeseries] anchor_time not datetime:", anchor_time)

        # ----- 2) 시간 구간: 앵커 기준, range 에 따라 동적 -----
        window = _window_for_range(range)
        t_to = anchor_time
        t_from = anchor_time - window

        dlog(
            "[/api/analyze/timeseries] anchor_time:",
            anchor_time,
            "window:",
            window,
            "t_from:",
            t_from,
            "t_to:",
            t_to,
            "range_param:",
            range,
        )

        # ----- 3) 필터 파라미터 파싱 (쉼표 구분) -----
        sel_types: Optional[List[str]] = None
        sel_names: Optional[List[str]] = None

        if device_types:
            sel_types = [s.strip() for s in device_types.split(",") if s.strip()]
        if device_names:
            sel_names = [s.strip() for s in device_names.split(",") if s.strip()]

        dlog(
            "[/api/analyze/timeseries] filters:",
            "types =", sel_types or [],
            "names =", sel_names or [],
        )

        # ✅ 타입이 하나라도 선택되면 type 시리즈는 항상 만든다
        want_type_series = bool(sel_types)
        # ✅ 디바이스는 name 선택 여부와 관계없이, sel_names가 없으면 전체
        want_device_series = True

        # ----- 4) SQL 구성 -----
        # 🔹 device_kpi 스키마 기준:
        #   ct           → cycle_time_s
        #   `count`      → total_count
        #   defect_count → defect_count
        sql = f"""
            SELECT
                time,
                device_type,
                device_name,
                ct AS cycle_time_s,
                `count` AS total_count,
                defect_count
            FROM {DEVICE_TABLE}
            WHERE workcell = :workcell
              AND time >= :t_from
              AND time <= :t_to
              AND device_type IS NOT NULL
              AND device_type <> ''
              AND device_name IS NOT NULL
              AND device_name <> ''
        """
        params: Dict[str, Any] = {
            "workcell": workcell,
            "t_from": t_from,
            "t_to": t_to,
        }

        # IN (...) placeholder 확장
        if sel_types:
            type_ph = []
            for i, tval in enumerate(sel_types):
                key = f"type_{i}"
                type_ph.append(f":{key}")
                params[key] = tval
            sql += f" AND device_type IN ({', '.join(type_ph)})"

        if sel_names:
            name_ph = []
            for i, nval in enumerate(sel_names):
                key = f"name_{i}"
                name_ph.append(f":{key}")
                params[key] = nval
            sql += f" AND device_name IN ({', '.join(name_ph)})"

        sql += " ORDER BY time ASC"

        rows = _fetch_all(db, sql, params)
        dlog("[/api/analyze/timeseries] raw row count:", len(rows))

        # 장비별 로우 카운트 로그
        per_key: Dict[str, int] = {}
        for r in rows:
            dtp = str(r.get("device_type") or "").strip()
            dnm = str(r.get("device_name") or "").strip()
            key = f"{dtp}::{dnm}"
            per_key[key] = per_key.get(key, 0) + 1
        dlog("[/api/analyze/timeseries] row count by device:", per_key)

        # ----- 5) 시리즈 집계 구조 -----
        series_map: Dict[str, Dict[str, Any]] = {}

        def _ensure_series(
            key: str,
            kind: str,
            device_type: str,
            device_name: Optional[str],
        ) -> Dict[str, Any]:
            if key not in series_map:
                series_map[key] = {
                    "key": key,
                    "kind": kind,  # "type" or "device"
                    "device_type": device_type,
                    "device_name": device_name,
                    "time": [],
                    "values": [],
                    "_agg": {},  # t_iso -> {sum, cnt, sum_count, sum_def}
                }
            return series_map[key]

        for r in rows:
            t_raw = r.get("time")
            if t_raw is None:
                continue

            if isinstance(t_raw, datetime):
                t_iso = t_raw.isoformat()
            else:
                t_iso = str(t_raw)

            dtp = str(r.get("device_type") or "").strip()
            dnm = str(r.get("device_name") or "").strip()
            if not dtp or not dnm:
                continue

            cycle_val = r.get("cycle_time_s")
            total_cnt = r.get("total_count")
            defect_cnt = r.get("defect_count")

            # ----- device_name 시리즈 -----
            if want_device_series:
                if (not sel_names) or (dnm in sel_names):
                    dev_key = f"device:{dnm}"
                    dev_ser = _ensure_series(dev_key, "device", dtp, dnm)
                    ag = dev_ser["_agg"].setdefault(
                        t_iso,
                        {"sum": 0.0, "cnt": 0, "sum_count": 0.0, "sum_def": 0.0},
                    )

                    if metric_norm == "cycle":
                        if cycle_val is not None:
                            ag["sum"] += float(cycle_val)
                            ag["cnt"] += 1

                    elif metric_norm == "defect_rate":
                        if total_cnt is not None and defect_cnt is not None:
                            try:
                                c = float(total_cnt)
                                d = float(defect_cnt)
                            except (TypeError, ValueError):
                                c = 0.0
                                d = 0.0
                            ag["sum_count"] += c
                            ag["sum_def"] += d

            # ----- device_type 시리즈 (상위 집계) -----
            if want_type_series:
                if (not sel_types) or (dtp in sel_types):
                    type_key = f"type:{dtp}"
                    type_ser = _ensure_series(type_key, "type", dtp, None)
                    ag_t = type_ser["_agg"].setdefault(
                        t_iso,
                        {"sum": 0.0, "cnt": 0, "sum_count": 0.0, "sum_def": 0.0},
                    )

                    if metric_norm == "cycle":
                        if cycle_val is not None:
                            ag_t["sum"] += float(cycle_val)
                            ag_t["cnt"] += 1

                    elif metric_norm == "defect_rate":
                        if total_cnt is not None and defect_cnt is not None:
                            try:
                                c = float(total_cnt)
                                d = float(defect_cnt)
                            except (TypeError, ValueError):
                                c = 0.0
                                d = 0.0
                            ag_t["sum_count"] += c
                            ag_t["sum_def"] += d

        # ----- 6) agg → time / values 배열로 변환 -----
        result_series: List[Dict[str, Any]] = []

        for key, ser in series_map.items():
            agg = ser.pop("_agg", {})
            keys_sorted = sorted(agg.keys())

            time_list: List[str] = []
            val_list: List[Optional[float]] = []

            for t_iso in keys_sorted:
                info = agg[t_iso]
                if metric_norm == "cycle":
                    if info["cnt"] > 0:
                        # 합을 그대로 사용 (원하면 평균 info["sum"]/info["cnt"] 로 변경 가능)
                        value = info["sum"]
                    else:
                        value = None
                elif metric_norm == "defect_rate":
                    if info["sum_count"] > 0:
                        # good ratio %
                        good_ratio_pct = (
                            (info["sum_count"] - info["sum_def"])
                            * 100.0
                            / info["sum_count"]
                        )
                        value = good_ratio_pct
                    else:
                        value = None
                else:
                    value = None

                time_list.append(t_iso)
                val_list.append(value)

            ser["time"] = time_list
            ser["values"] = val_list
            result_series.append(ser)

            dlog(
                "[/api/analyze/timeseries] series built:",
                key,
                "points:",
                len(time_list),
            )

        dlog(
            "[/api/analyze/timeseries] series count:",
            len(result_series),
            "workcell:",
            workcell,
            "metric:",
            metric_norm,
            "range(param):",
            range,
        )

        return {
            "workcell": workcell,
            "metric": metric_norm,
            "range": range,
            "series": result_series,
            "_source": "mysql",
            "_table": DEVICE_TABLE,
            "anchor_time": anchor_time.isoformat()
            if isinstance(anchor_time, datetime)
            else str(anchor_time),
            "t_from": t_from.isoformat()
            if isinstance(t_from, datetime)
            else str(t_from),
            "t_to": t_to.isoformat()
            if isinstance(t_to, datetime)
            else str(t_to),
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(
            status_code=500,
            detail=f"timeseries internal error: {e}",
        )
