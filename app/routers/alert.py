# app/routers/alert.py

from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends, Response, Query, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..db import get_db
from ..auth import get_current_user  # 세션/로그인 재사용

# 👇 여기서 prefix를 "/api/alerts" 로 고정
router = APIRouter(prefix="/api/alerts", tags=["alerts"])


# =========================
# helpers
# =========================

def _iso(v: Optional[datetime]) -> Optional[str]:
  if v is None:
    return None
  if isinstance(v, datetime):
    try:
      return v.replace(tzinfo=None).isoformat(timespec="seconds")
    except Exception:
      return v.isoformat()
  return str(v)


def _display_name(
  name: Optional[str],
  login_id: Optional[str],
  email: Optional[str],
) -> Optional[str]:
  """
  표시용 이름:
  1) name 컬럼이 있으면 제일 먼저 사용
  2) 없으면 login_id
  3) 그것도 없으면 email
  """
  return name or login_id or email or None


# =========================
# Pydantic Schemas
# =========================

class AlertItem(BaseModel):
  id: int

  workcell_code: str
  device_name: Optional[str] = None

  category: str
  param_code: str
  severity: str
  status: str

  description: str

  threshold_value: float
  actual_value: float
  unit: Optional[str] = None

  event_time: str
  acknowledged_at: Optional[str] = None
  resolved_at: Optional[str] = None

  acknowledged_by: Optional[str] = None
  resolved_by: Optional[str] = None

  created_at: Optional[str] = None
  updated_at: Optional[str] = None


class AlertsListResp(BaseModel):
  ok: bool = True
  items: List[AlertItem]


# =========================
# GET /api/alerts
# =========================
#
# - 기본: status in ('active','acknowledged')  (open)
# - ?status=all       -> 전체
# - ?status=resolved  -> resolved 만
# - ?status=inactive  -> inactive 만
# =========================

@router.get("", response_model=AlertsListResp)  # 👈 "" → prefix 뒤에 그대로 붙음
def list_alerts(
  request: Request,
  response: Response,
  db: Session = Depends(get_db),
  current_user: Dict[str, Any] = Depends(get_current_user),
  status: Optional[str] = Query(
    default="open",
    description="open(기본: active+acknowledged) | all | active | acknowledged | resolved | inactive",
  ),
  limit: int = Query(default=100, ge=1, le=500),
):
  """
  Logs & Alerts 화면용 알람 목록 조회.
  GET /api/alerts
  """
  response.headers["Cache-Control"] = "no-store"

  print(
    f"[/api/alerts] user_id={current_user.get('id')} "
    f"role={current_user.get('role')} status={status!r} limit={limit}",
    flush=True,
  )
  print(f"[/api/alerts] raw query params = {dict(request.query_params)}", flush=True)

  # ---- 상태 필터 구성 ----
  where_clauses = []
  params: Dict[str, Any] = {"limit": limit}

  if status == "open" or status is None:
    where_clauses.append("a.status IN ('active', 'acknowledged')")
  elif status == "all":
    # no filter
    pass
  else:
    where_clauses.append("a.status = :status")
    params["status"] = status

  where_sql = ""
  if where_clauses:
    where_sql = "WHERE " + " AND ".join(where_clauses)

  # ---- 쿼리 ----
  q = text(f"""
    SELECT
      a.id,
      a.workcell_code,
      a.device_name,
      a.category,
      a.param_code,
      a.severity,
      a.status,
      a.description,
      a.threshold_value,
      a.actual_value,
      a.unit,
      a.event_time,
      a.acknowledged_at,
      a.resolved_at,
      a.created_at,
      a.updated_at,

      -- Acknowledged by
      ma.name     AS ack_name,
      ma.login_id AS ack_login_id,
      ma.email    AS ack_email,

      -- Resolved by
      mr.name     AS res_name,
      mr.login_id AS res_login_id,
      mr.email    AS res_email

    FROM alerts a
    LEFT JOIN members ma ON ma.id = a.acknowledged_by
    LEFT JOIN members mr ON mr.id = a.resolved_by
    {where_sql}
    ORDER BY a.event_time DESC
    LIMIT :limit
  """)

  try:
    rows = db.execute(q, params).mappings().all() or []
  except Exception as e:
    print(f"[/api/alerts] query error: {e}", flush=True)
    raise HTTPException(status_code=500, detail="Failed to fetch alerts")

  items: List[AlertItem] = []
  for row in rows:
    items.append(
      AlertItem(
        id=row["id"],
        workcell_code=row["workcell_code"],
        device_name=row.get("device_name"),

        category=row["category"],
        param_code=row["param_code"],
        severity=row["severity"],
        status=row["status"],

        description=row["description"],

        threshold_value=float(row["threshold_value"]),
        actual_value=float(row["actual_value"]),
        unit=row.get("unit"),

        event_time=_iso(row.get("event_time")) or "",
        acknowledged_at=_iso(row.get("acknowledged_at")),
        resolved_at=_iso(row.get("resolved_at")),

        acknowledged_by=_display_name(
          row.get("ack_name"),
          row.get("ack_login_id"),
          row.get("ack_email"),
        ),
        resolved_by=_display_name(
          row.get("res_name"),
          row.get("res_login_id"),
          row.get("res_email"),
        ),

        created_at=_iso(row.get("created_at")),
        updated_at=_iso(row.get("updated_at")),
      )
    )

  print(f"[/api/alerts] returned {len(items)} items", flush=True)
  return AlertsListResp(ok=True, items=items)
