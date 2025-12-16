# app/routers/analyze.py  (study / sanitized version)
import os
import re
from typing import List, Dict, Any, Optional, Set, Tuple
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db import get_db  # 기존과 동일 세션

print(">>> loaded routers.analyze (MySQL study version):", __file__, flush=True)

router = APIRouter(prefix="/api/analyze", tags=["analyze"])

# ---- Debug toggle ----
DEBUG = os.getenv("ANALYZE_DEBUG", "0").lower() in ("1", "true", "yes", "on")
def dlog(*args: Any) -> None:
    if DEBUG:
        print(*args, flush=True)

# ---- MySQL env ----
SIGNAL_TABLE = os.getenv("SIGNAL_TABLE", "signal_log")


def _validate_site(site: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", site or ""):
        raise HTTPException(status_code=400, detail="Invalid site")
    return site


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
    return timedelta(minutes=30)


# ---------------- DB helpers (SQLAlchemy + text) ----------------
def _fetch_all(db: Session, sql: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    dlog("[ANALYZE SQL ALL]\n", sql, "\nparams:", params)
    rows = db.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]

@router.get("/component_types")
def list_component_types(
    site: str = Query(..., description="사이트/라인 ID (예: A1)"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    site = _validate_site(site)

    sql = f"""
        SELECT DISTINCT component
        FROM {SIGNAL_TABLE}
        WHERE site_id = :site
          AND component IS NOT NULL
          AND component <> ''
        ORDER BY component
    """
    rows = _fetch_all(db, sql, {"site": site})
    types: List[str] = [str(r["component"]) for r in rows if r.get("component")]

    dlog("[/api/analyze/component_types] count:", len(types), "types:", types)
    return {"types": types, "_source": "mysql", "_table": SIGNAL_TABLE, "_site": site}


@router.get("/node_names")
def list_node_names(
    site: str = Query(..., description="사이트/라인 ID (예: A1)"),
    component: Optional[str] = Query(None, description="필터: 특정 component 만"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    site = _validate_site(site)

    base_sql = f"""
        SELECT DISTINCT node_name
        FROM {SIGNAL_TABLE}
        WHERE site_id = :site
          AND node_name IS NOT NULL
          AND node_name <> ''
    """
    params: Dict[str, Any] = {"site": site}

    if component:
        base_sql += " AND component = :component"
        params["component"] = component

    base_sql += " ORDER BY node_name"

    rows = _fetch_all(db, base_sql, params)
    names: List[str] = [str(r["node_name"]) for r in rows if r.get("node_name")]

    dlog(
        "[/api/analyze/node_names] count:", len(names),
        "site:", site, "component:", component or "(none)"
    )
    return {"nodes": names, "_source": "mysql", "_table": SIGNAL_TABLE, "_site": site, "_component": component}


@router.get("/nodes")
def list_nodes(
    site: str = Query(..., description="사이트/라인 ID (예: A1)"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    site = _validate_site(site)

    sql = f"""
        SELECT DISTINCT component, node_name
        FROM {SIGNAL_TABLE}
        WHERE site_id = :site
          AND node_name IS NOT NULL AND node_name <> ''
          AND component IS NOT NULL AND component <> ''
        ORDER BY component, node_name
    """
    rows = _fetch_all(db, sql, {"site": site})

    nodes: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()
    for r in rows:
        c = str(r.get("component") or "").strip()
        n = str(r.get("node_name") or "").strip()
        if not c or not n:
            continue
        key = (c, n)
        if key in seen:
            continue
        seen.add(key)
        nodes.append({"component": c, "node_name": n})

    dlog("[/api/analyze/nodes] count:", len(nodes))
    return {"nodes": nodes, "_source": "mysql", "_table": SIGNAL_TABLE, "_site": site}


@router.get("/timeseries")
def timeseries(
    site: str = Query(..., description="사이트/라인 ID (예: A1)"),
    metric: str = Query(
        "quality_pct",
        description='metric: "quality_pct"(정상품질%) 또는 "latency"(지연/사이클)',
    ),
    range: str = Query("30m", alias="range", description='"30m", "1day", "7day" 중 하나'),
    component_types: Optional[str] = Query(None, description="쉼표로 구분된 component 목록 (예: Conveyor,Robot)"),
    node_names: Optional[str] = Query(None, description="쉼표로 구분된 node_name 목록"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    import traceback

    try:
        site = _validate_site(site)

        metric_norm = (metric or "").strip().lower()
        if metric_norm not in ("quality_pct", "latency"):
            raise HTTPException(status_code=400, detail="metric must be 'quality_pct' or 'latency'")

        # ----- 1) 앵커 시간: 이 site의 최신 logged_at -----
        anchor_sql = f"""
            SELECT MAX(logged_at) AS max_time
            FROM {SIGNAL_TABLE}
            WHERE site_id = :site
        """
        anchor_rows = _fetch_all(db, anchor_sql, {"site": site})
        anchor_time = anchor_rows[0].get("max_time") if anchor_rows else None

        if anchor_time is None:
            dlog("[/api/analyze/timeseries] no data for site:", site)
            return {
                "site": site,
                "metric": metric_norm,
                "range": range,
                "series": [],
                "_source": "mysql",
                "_table": SIGNAL_TABLE,
                "anchor_time": None,
                "t_from": None,
                "t_to": None,
            }

        if not isinstance(anchor_time, datetime):
            try:
                anchor_time = datetime.fromisoformat(str(anchor_time))
            except Exception:
                dlog("[/api/analyze/timeseries] anchor_time not datetime:", anchor_time)

        # ----- 2) 시간 구간: 앵커 기준 -----
        window = _window_for_range(range)
        t_to = anchor_time
        t_from = anchor_time - window

        # ----- 3) 필터 파라미터 파싱 -----
        sel_components: Optional[List[str]] = None
        sel_nodes: Optional[List[str]] = None

        if component_types:
            sel_components = [s.strip() for s in component_types.split(",") if s.strip()]
        if node_names:
            sel_nodes = [s.strip() for s in node_names.split(",") if s.strip()]

        # 타입이 하나라도 선택되면 type 시리즈는 항상 만든다
        want_component_series = bool(sel_components)
        # 노드 시리즈는 항상 만든다(선택 없으면 전체)
        want_node_series = True

        # ----- 4) SQL 구성 (✅ 테이블/컬럼명 변경 + 컬럼 1개 추가(batch_id)) -----
        sql = f"""
            SELECT
                logged_at,
                component,
                node_name,
                latency_s,
                sample_total,
                sample_bad,
                batch_id
            FROM {SIGNAL_TABLE}
            WHERE site_id = :site
              AND logged_at >= :t_from
              AND logged_at <= :t_to
              AND component IS NOT NULL AND component <> ''
              AND node_name IS NOT NULL AND node_name <> ''
        """
        params: Dict[str, Any] = {"site": site, "t_from": t_from, "t_to": t_to}

        if sel_components:
            ph = []
            for i, v in enumerate(sel_components):
                k = f"comp_{i}"
                ph.append(f":{k}")
                params[k] = v
            sql += f" AND component IN ({', '.join(ph)})"

        if sel_nodes:
            ph = []
            for i, v in enumerate(sel_nodes):
                k = f"node_{i}"
                ph.append(f":{k}")
                params[k] = v
            sql += f" AND node_name IN ({', '.join(ph)})"

        sql += " ORDER BY logged_at ASC"

        rows = _fetch_all(db, sql, params)
        dlog("[/api/analyze/timeseries] raw row count:", len(rows))

        # ----- 5) 시리즈 집계 구조 -----
        series_map: Dict[str, Dict[str, Any]] = {}

        def _ensure_series(key: str, kind: str, component: str, node_name: Optional[str]) -> Dict[str, Any]:
            if key not in series_map:
                series_map[key] = {
                    "key": key,
                    "kind": kind,  # "component" or "node"
                    "component": component,
                    "node_name": node_name,
                    "time": [],
                    "values": [],
                    "_agg": {},  # t_iso -> {sum_lat, cnt_lat, sum_total, sum_bad}
                }
            return series_map[key]

        for r in rows:
            t_raw = r.get("logged_at")
            if t_raw is None:
                continue
            t_iso = t_raw.isoformat() if isinstance(t_raw, datetime) else str(t_raw)

            comp = str(r.get("component") or "").strip()
            node = str(r.get("node_name") or "").strip()
            if not comp or not node:
                continue

            latency_val = r.get("latency_s")
            total_cnt = r.get("sample_total")
            bad_cnt = r.get("sample_bad")

            # ----- node 시리즈 -----
            if want_node_series:
                if (not sel_nodes) or (node in sel_nodes):
                    dev_key = f"node:{node}"
                    ser = _ensure_series(dev_key, "node", comp, node)
                    ag = ser["_agg"].setdefault(t_iso, {"sum_lat": 0.0, "cnt_lat": 0, "sum_total": 0.0, "sum_bad": 0.0})

                    if metric_norm == "latency":
                        if latency_val is not None:
                            ag["sum_lat"] += float(latency_val)
                            ag["cnt_lat"] += 1
                    else:  # quality_pct
                        if total_cnt is not None and bad_cnt is not None:
                            try:
                                c = float(total_cnt)
                                b = float(bad_cnt)
                            except (TypeError, ValueError):
                                c, b = 0.0, 0.0
                            ag["sum_total"] += c
                            ag["sum_bad"] += b

            # ----- component 시리즈(상위 집계) -----
            if want_component_series:
                if (not sel_components) or (comp in sel_components):
                    type_key = f"component:{comp}"
                    ser = _ensure_series(type_key, "component", comp, None)
                    ag = ser["_agg"].setdefault(t_iso, {"sum_lat": 0.0, "cnt_lat": 0, "sum_total": 0.0, "sum_bad": 0.0})

                    if metric_norm == "latency":
                        if latency_val is not None:
                            ag["sum_lat"] += float(latency_val)
                            ag["cnt_lat"] += 1
                    else:
                        if total_cnt is not None and bad_cnt is not None:
                            try:
                                c = float(total_cnt)
                                b = float(bad_cnt)
                            except (TypeError, ValueError):
                                c, b = 0.0, 0.0
                            ag["sum_total"] += c
                            ag["sum_bad"] += b

        # ----- 6) agg → time / values 배열로 변환 -----
        result_series: List[Dict[str, Any]] = []

        for key, ser in series_map.items():
            agg = ser.pop("_agg", {})
            keys_sorted = sorted(agg.keys())

            t_list: List[str] = []
            v_list: List[Optional[float]] = []

            for t_iso in keys_sorted:
                info = agg[t_iso]
                if metric_norm == "latency":
                    value = (info["sum_lat"] / info["cnt_lat"]) if info["cnt_lat"] > 0 else None
                else:  # quality_pct
                    if info["sum_total"] > 0:
                        value = (info["sum_total"] - info["sum_bad"]) * 100.0 / info["sum_total"]
                    else:
                        value = None

                t_list.append(t_iso)
                v_list.append(value)

            ser["time"] = t_list
            ser["values"] = v_list
            result_series.append(ser)

            dlog("[/api/analyze/timeseries] series built:", key, "points:", len(t_list))

        return {
            "site": site,
            "metric": metric_norm,
            "range": range,
            "series": result_series,
            "_source": "mysql",
            "_table": SIGNAL_TABLE,
            "anchor_time": anchor_time.isoformat() if isinstance(anchor_time, datetime) else str(anchor_time),
            "t_from": t_from.isoformat() if isinstance(t_from, datetime) else str(t_from),
            "t_to": t_to.isoformat() if isinstance(t_to, datetime) else str(t_to),
        }

    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"timeseries internal error: {e}")
