# app/routers/analysis_timeseries.py
import os
import re
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

MEAS = os.getenv("STUDY_MEASUREMENT", "kpi_timeseries")   # (기존: unified_metrics)
# 기존 metric, workcell, value_num -> 비식별 컬럼명으로 변경
COL_SITE   = os.getenv("TS_COL_SITE", "site_id")         # (기존: workcell)
COL_KIND   = os.getenv("TS_COL_KIND", "signal")          # (기존: metric)
COL_VALUE  = os.getenv("TS_COL_VALUE", "value")          # (기존: value_num)

METRICS = ("output_rate", "latency_s")


def _client():
    if not (INFLUX_URL and INFLUX_TOKEN and INFLUX_DB):
        missing = [k for k, v in {
            "INFLUX_URL/INFLUX_HOST": INFLUX_URL,
            "INFLUX_TOKEN": INFLUX_TOKEN,
            "INFLUX_BUCKET/INFLUX_DATABASE": INFLUX_DB,
        }.items() if not v]
        raise HTTPException(500, detail=f"Influx env missing: {', '.join(missing)}")
    return InfluxDBClient3(
        host=INFLUX_URL,
        token=INFLUX_TOKEN,
        org=INFLUX_ORG,
        database=INFLUX_DB
    )


# ---- Simulated fallback (막대/꺾은선 데모용) ----
def _simulate_series(now: datetime, lookback: str, interval: str) -> Dict[str, List[Dict[str, float]]]:
    # 간단히 5지점만 생성
    base = now.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    hours = [-36, -24, -12, -6, 0]
    out_vals = [420, 450, 480, 460, 440]
    lat_vals = [9.2, 8.1, 7.6, 7.2, 7.6]

    output_rate = [{"t": (base + timedelta(hours=h)).isoformat(), "v": v} for h, v in zip(hours, out_vals)]
    latency_s   = [{"t": (base + timedelta(hours=h)).isoformat(), "v": v} for h, v in zip(hours, lat_vals)]

    return {
        "site": "SIM",
        "lookback": lookback,
        "interval": interval,
        "_source": "simulated",
        "_fallback": "no_data",
        "output_rate": output_rate,
        "latency_s": latency_s,
    }


@router.get("/timeseries")
def get_timeseries(
    site: str = Query(..., description="사이트/라인 ID (예: A1)"),  # (기존: workcell)
    lookback: str = Query("30d", description="조회 윈도우: 예) 24h, 7d, 30d (기본 30d)"),
    interval: str = Query("1h", description="버킷 간격: 예) 15m, 1h, 6h, 1d (기본 1h)"),
):
    """
    - output_rate, latency_s 를 기간/간격으로 집계해서 반환
    - InfluxDB v3 SQL: DATE_BIN + GROUP BY
    - 데이터가 없으면 시뮬레이트로 폴백
    """
    # 입력 밸리데이션(간단한 화이트리스트)
    if not re.fullmatch(r"[A-Za-z0-9_\-]+", site):
        raise HTTPException(status_code=400, detail="Invalid site")
    if not re.fullmatch(r"\d+[smhdw]", lookback):
        lookback = "30d"
    if not re.fullmatch(r"\d+[smhdw]", interval):
        interval = "1h"

    dlog("\n[/api/analysis/timeseries] params:", {"site": site, "lookback": lookback, "interval": interval})
    dlog("[/api/analysis/timeseries] influx:", {"host": INFLUX_URL, "db": INFLUX_DB, "org": INFLUX_ORG})

    metrics_sql_list = ", ".join(f"'{m}'" for m in METRICS)

    sql = f"""
    SELECT
      {COL_KIND} AS signal,
      DATE_BIN(INTERVAL '{interval}', time) AS time,
      AVG({COL_VALUE}) AS value
    FROM {MEAS}
    WHERE {COL_SITE} = '{site}'
      AND {COL_KIND} IN ({metrics_sql_list})
      AND time > now() - INTERVAL '{lookback}'
    GROUP BY {COL_KIND}, DATE_BIN(INTERVAL '{interval}', time)
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
        if hasattr(res, "to_pydict"):
            pdict = res.to_pydict()
            s_list = pdict.get("signal", []) or pdict.get(COL_KIND, [])
            t_list = pdict.get("time", [])
            v_list = pdict.get("value", [])
            for s, t, v in zip(s_list, t_list, v_list):
                ts = t.isoformat() if hasattr(t, "isoformat") else str(t)
                rows.append({"signal": s, "time": ts, "value": float(v) if v is not None else None})
        else:
            df = res.to_pandas() if hasattr(res, "to_pandas") else res
            if getattr(df, "empty", True):
                rows = []
            else:
                for _, r in df.iterrows():
                    ts = r["time"].isoformat() if hasattr(r["time"], "isoformat") else str(r["time"])
                    rows.append({"signal": r.get("signal") or r.get(COL_KIND), "time": ts, "value": float(r["value"]) if r["value"] is not None else None})
    except Exception as e:
        dlog("[/api/analysis/timeseries] parse warn:", repr(e))

    if not rows:
        return _simulate_series(datetime.now(timezone.utc), lookback, interval)

    # signal 별로 배열 구성
    series: Dict[str, List[Dict[str, float]]] = {m: [] for m in METRICS}
    for r in rows:
        s = r.get("signal")
        if s in series:
            series[s].append({"t": r["time"], "v": r["value"]})

    # 시간 정렬(안전)
    for arr in series.values():
        arr.sort(key=lambda x: x["t"])

    payload = {
        "site": site,
        "lookback": lookback,
        "interval": interval,
        "_source": "influx",
        **series
    }
    dlog("[/api/analysis/timeseries] return payload keys:", list(payload.keys()))
    return payload
