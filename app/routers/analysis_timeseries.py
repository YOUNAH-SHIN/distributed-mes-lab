# app/routers/analysis_timeseries.py
import os
import re
import math
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List

from fastapi import APIRouter, HTTPException, Query
from influxdb_client_3 import InfluxDBClient3

router = APIRouter(prefix="/api/analysis", tags=["analysis-timeseries"])

# ---- Debug toggle (optional) ----
DEBUG = os.getenv("ANALYSIS_DEBUG", "0").lower() in ("1", "true", "yes", "on")
def dlog(*args):
    if DEBUG:
        print(*args, flush=True)

# ---- Influx env ----
INFLUX_URL = os.getenv("INFLUX_URL") or os.getenv("INFLUX_HOST")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG", "")
INFLUX_DB = os.getenv("INFLUX_BUCKET") or os.getenv("INFLUX_DATABASE")
MEAS = os.getenv("UNIFIED_MEASUREMENT", "unified_metrics")

METRICS = ("throughput_pcsph", "cycle_time_s")

def _client():
    if not (INFLUX_URL and INFLUX_TOKEN and INFLUX_DB):
        missing = [k for k, v in {
            "INFLUX_URL/INFLUX_HOST": INFLUX_URL,
            "INFLUX_TOKEN": INFLUX_TOKEN,
            "INFLUX_BUCKET/INFLUX_DATABASE": INFLUX_DB,
        }.items() if not v]
        raise HTTPException(500, detail=f"Influx env missing: {', '.join(missing)}")
    return InfluxDBClient3(host=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, database=INFLUX_DB)

# ---- Simulated fallback (막대/꺾은선 데모용) ----
def _simulate_series(now: datetime, lookback: str, interval: str) -> Dict[str, List[Dict[str, float]]]:
    # 간단히 5지점만 생성 (09,12,15,18,21시)
    base = now.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    hours = [ -36, -24, -12, -6, 0 ]  # 과거 시각들
    thr_vals = [420, 450, 480, 460, 440]
    cyc_vals = [9.2, 8.1, 7.6, 7.2, 7.6]

    throughput = [{"t": (base + timedelta(hours=h)).isoformat(), "v": v} for h, v in zip(hours, thr_vals)]
    cycle = [{"t": (base + timedelta(hours=h)).isoformat(), "v": v} for h, v in zip(hours, cyc_vals)]

    return {
        "workcell": "SIM",
        "lookback": lookback,
        "interval": interval,
        "_source": "simulated",
        "_fallback": "no_data",
        "throughput_pcsph": throughput,
        "cycle_time_s": cycle,
    }

@router.get("/timeseries")
def get_timeseries(
    workcell: str = Query(..., description="워크셀 ID (예: A1)"),
    lookback: str = Query("30d", description="조회 윈도우: 예) 24h, 7d, 30d (기본 30d)"),
    interval: str = Query("1h", description="버킷 간격: 예) 15m, 1h, 6h, 1d (기본 1h)"),
):
    """
    - throughput_pcsph, cycle_time_s 를 기간/간격으로 집계해서 반환
    - InfluxDB v3 SQL: DATE_BIN + GROUP BY (콘솔에서 검증된 형태)
    - 데이터가 없으면 시뮬레이트로 폴백
    """
    # 입력 밸리데이션(간단한 화이트리스트)
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", workcell):
        raise HTTPException(status_code=400, detail="Invalid workcell")
    if not re.fullmatch(r"\d+[smhdw]", lookback):  # 초/분/시간/일/주
        lookback = "30d"
    if not re.fullmatch(r"\d+[smhdw]", interval):
        interval = "1h"

    dlog("\n[/api/analysis/timeseries] params:", {"workcell": workcell, "lookback": lookback, "interval": interval})
    dlog("[/api/analysis/timeseries] influx:", {"host": INFLUX_URL, "db": INFLUX_DB, "org": INFLUX_ORG})

    metrics_sql_list = ", ".join(f"'{m}'" for m in METRICS)

    # 콘솔에서 그대로 실행 가능한 형태(CTE 없이)
    sql = f"""
    SELECT
      metric,
      DATE_BIN(INTERVAL '{interval}', time) AS time,
      AVG(value_num) AS value
    FROM {MEAS}
    WHERE workcell = '{workcell}'
      AND metric IN ({metrics_sql_list})
      AND time > now() - INTERVAL '{lookback}'
    GROUP BY metric, DATE_BIN(INTERVAL '{interval}', time)
    ORDER BY time
    """.strip()

    dlog("[/api/analysis/timeseries] SQL:\n", sql)

    try:
        client = _client()
        res = client.query(sql)
    except Exception as e:
        dlog("[/api/analysis/timeseries] query error:", repr(e))
        raise HTTPException(status_code=500, detail=f"InfluxDB query failed: {e}")

    # pyarrow.Table 또는 pandas.DataFrame 모두 지원
    rows: List[Dict] = []
    try:
        # pyarrow 우선
        if hasattr(res, "to_pydict"):
            pdict = res.to_pydict()  # {'metric': [...], 'time': [...], 'value': [...]}
            m_list = pdict.get("metric", [])
            t_list = pdict.get("time", [])
            v_list = pdict.get("value", [])
            for m, t, v in zip(m_list, t_list, v_list):
                # Arrow Timestamp -> ISO8601
                ts = t.isoformat() if hasattr(t, "isoformat") else str(t)
                rows.append({"metric": m, "time": ts, "value": float(v) if v is not None else None})
        else:
            # pandas 경로
            df = res
            if hasattr(res, "to_pandas"):
                df = res.to_pandas()
            if getattr(df, "empty", True):
                rows = []
            else:
                for _, r in df.iterrows():
                    ts = r["time"].isoformat() if hasattr(r["time"], "isoformat") else str(r["time"])
                    rows.append({"metric": r["metric"], "time": ts, "value": float(r["value"]) if r["value"] is not None else None})
    except Exception as e:
        dlog("[/api/analysis/timeseries] parse warn:", repr(e))

    if not rows:
        # 폴백: 시뮬레이트
        return _simulate_series(datetime.now(timezone.utc), lookback, interval)

    # metric 별로 배열 구성
    series: Dict[str, List[Dict[str, float]]] = {m: [] for m in METRICS}
    for r in rows:
        m = r["metric"]
        if m in series:
            series[m].append({"t": r["time"], "v": r["value"]})

    # 시간 정렬(안전)
    for arr in series.values():
        arr.sort(key=lambda x: x["t"])

    payload = {
        "workcell": workcell,
        "lookback": lookback,
        "interval": interval,
        "_source": "influx",
        **series
    }
    dlog("[/api/analysis/timeseries] return payload keys:", list(payload.keys()))
    return payload
