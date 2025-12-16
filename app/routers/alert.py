# app/routers/alert.py

from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, Depends, Response, Query, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..db import get_db
from ..auth import get_current_user  # 세션/로그인 재사용

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
  display_name: Optional[str],
  username: Optional[str],
  email: Optional[str],
) -> Optional[str]:
  """
  표시용 이름:
  1) display_name 컬럼이 있으면 제일 먼저 사용
  2) 없으면 username
  3) 그것도 없으면 email
  """
  return display_name or username or email or None


def _to_float(v: Any) -> float:
  try:
    if v is None:
      return 0.0
    return float(v)
  except Exception:
    return 0.0


class AlertItem(BaseModel):
  id: int

  cell_id: str
  asset_name: Optional[str] = None

  alert_type: str
  metric_key: str
  level: str
  state: str

  message: str

  threshold: float
  observed: float
  uom: Optional[str] = None

  occurred_at: str
  acked_at: Optional[str] = None
  closed_at: Optional[str] = None

  acked_by: Optional[str] = None
  closed_by: Optional[str] = None

  created_at: Optional[str] = None
  updated_at: Optional[str] = None


class AlertsListResp(BaseModel):
  ok: bool = True
  items: List[AlertItem]


# =========================
# GET /api/alerts
# =========================
#
# - 기본: state in ('open','ack')  (open)
# - ?status=all     -> 전체
# - ?status=closed  -> closed 만
# - ?status=muted   -> muted 만
# - ?status=open    -> open+ack (기본)
# =========================

@router.get("", response_model=AlertsListResp)
def list_alerts(
  request: Request,
  response: Response,
  db: Session = Depends(get_db),
  current_user: Dict[str, Any] = Depends(get_current_user),
  status: Optional[str] = Query(
    default="open",
    description="open(기본: open+ack) | all | open | ack | closed | muted",
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
    where_clauses.append("e.state IN ('open', 'ack')")
  elif status == "all":
    pass
  else:
    where_clauses.append("e.state = :status")
    params["status"] = status

  where_sql = ""
  if where_clauses:
    where_sql = "WHERE " + " AND ".join(where_clauses)

  q = text(f"""
    SELECT
      e.id,
      e.cell_id,
      e.asset_name,
      e.alert_type,
      e.metric_key,
      e.level,
      e.state,
      e.message,
      e.threshold,
      e.observed,
      e.uom,
      e.occurred_at,
      e.acked_at,
      e.closed_at,
      e.created_at,
      e.updated_at,

      -- Acknowledged by
      ua.display_name AS ack_display_name,
      ua.username     AS ack_username,
      ua.email        AS ack_email,

      -- Closed by
      uc.display_name AS close_display_name,
      uc.username     AS close_username,
      uc.email        AS close_email

    FROM event_alerts e
    LEFT JOIN users ua ON ua.id = e.ack_user_id
    LEFT JOIN users uc ON uc.id = e.close_user_id
    {where_sql}
    ORDER BY e.occurred_at DESC
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
        cell_id=row["cell_id"],
        asset_name=row.get("asset_name"),

        alert_type=row["alert_type"],
        metric_key=row["metric_key"],
        level=row["level"],
        state=row["state"],

        message=row["message"],

        threshold=_to_float(row.get("threshold")),
        observed=_to_float(row.get("observed")),
        uom=row.get("uom"),

        occurred_at=_iso(row.get("occurred_at")) or "",
        acked_at=_iso(row.get("acked_at")),
        closed_at=_iso(row.get("closed_at")),

        acked_by=_display_name(
          row.get("ack_display_name"),
          row.get("ack_username"),
          row.get("ack_email"),
        ),
        closed_by=_display_name(
          row.get("close_display_name"),
          row.get("close_username"),
          row.get("close_email"),
        ),

        created_at=_iso(row.get("created_at")),
        updated_at=_iso(row.get("updated_at")),
      )
    )

  print(f"[/api/alerts] returned {len(items)} items", flush=True)
  return AlertsListResp(ok=True, items=items)
