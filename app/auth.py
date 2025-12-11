# app/auth.py

from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timedelta
import secrets

from fastapi import (
    APIRouter, Depends, HTTPException, status,
    Path, Query, Header, Response, Cookie
)
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from sqlalchemy import text
from passlib.context import CryptContext

from .db import get_db
from pydantic import BaseModel

pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")
router = APIRouter(prefix="/api", tags=["auth"])

# ============================================================
# 세션/쿠키 설정 (sessions 테이블 필요)
#
# 예시 스키마:
# CREATE TABLE sessions (
#   id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
#   session_id VARCHAR(128) NOT NULL UNIQUE,
#   member_id BIGINT UNSIGNED NOT NULL,
#   created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
#   expires_at DATETIME NULL,
#   INDEX idx_member_id (member_id),
#   CONSTRAINT fk_sessions_member
#     FOREIGN KEY (member_id) REFERENCES members(id)
#     ON DELETE CASCADE
# );
# ============================================================

SESSION_COOKIE_NAME = "imp_session"
SESSION_TTL_SECONDS = 60 * 60 * 8  # 8시간
SESSION_COOKIE_SECURE = True       # HTTPS 환경에서만 사용(로컬 개발 시 False로 수정해도 됨)


# =========================
# Pydantic Schemas
# =========================

class LoginBody(BaseModel):
    mode: str               # "admin" | "member" (or "operator")
    user: str               # admin: email, member/operator: login_id
    password: str

class LoginResp(BaseModel):
    ok: bool = True
    user_id: str            # email or login_id (프론트와 호환)
    role: str               # "admin" | "operator" | ...
    token: str              # 데모 토큰 (이제는 실제 사용 X, 호환용)

class CreateMemberBody(BaseModel):
    # 일반(오퍼레이터) 계정 생성: login_id + password
    login_id: str = Field(..., min_length=3, max_length=120)
    password: str = Field(..., min_length=6, max_length=128)

class CreateMemberResp(BaseModel):
    ok: bool = True
    user_id: str
    role: str = "operator"

class MemberOut(BaseModel):
    id: int
    login_id: Optional[str] = None
    email: Optional[str] = None
    role: str

class ListMembersResp(BaseModel):
    ok: bool = True
    items: List[MemberOut]

class SimpleOkResp(BaseModel):
    ok: bool = True


# ===== 프로필 스키마 (my.html 용) =====
class ProfileOut(BaseModel):
    id: int
    role: str
    login_id: Optional[str] = None
    email: Optional[str] = None
    site: Optional[str] = None
    workcell: Optional[str] = None
    name: Optional[str] = None
    phone: Optional[str] = None
    status: Optional[str] = None
    last_login_at: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

class ProfilePatchBody(BaseModel):
    # 읽기 전용: role / login_id / email 은 받더라도 무시
    site: Optional[str] = Field(default=None, max_length=120)
    workcell: Optional[str] = Field(default=None, max_length=120)
    name: Optional[str] = Field(default=None, max_length=120)
    phone: Optional[str] = Field(default=None, max_length=50)


# =========================
# helpers
# =========================

def _iso(v: Union[datetime, str, None]) -> Optional[str]:
    """datetime이면 ISO 문자열로, 그 외는 그대로/None."""
    if isinstance(v, datetime):
        try:
            return v.replace(tzinfo=None).isoformat(timespec="seconds")
        except Exception:
            return v.isoformat()
    return v if v is None or isinstance(v, str) else str(v)


def _set_session_cookie(response: Response, session_id: str) -> None:
    """클라이언트에 세션 쿠키 심기."""
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_id,
        httponly=True,
        secure=SESSION_COOKIE_SECURE,
        samesite="lax",
        path="/",
        max_age=SESSION_TTL_SECONDS,
    )


def _create_session(db: Session, member_id: int) -> str:
    """sessions 테이블에 세션 레코드 생성 후 session_id 반환."""
    session_id = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(seconds=SESSION_TTL_SECONDS)

    ins = text("""
        INSERT INTO sessions (session_id, member_id, expires_at)
        VALUES (:sid, :mid, :exp)
    """)
    try:
        db.execute(ins, {"sid": session_id, "mid": member_id, "exp": expires_at})
        db.commit()
        print(f"[_create_session] created session for member_id={member_id}", flush=True)
    except Exception as e:
        db.rollback()
        print(f"[_create_session] error: {e}", flush=True)
        raise

    return session_id


def _clear_session(db: Session, session_id: str) -> None:
    """sessions 테이블에서 해당 세션 삭제."""
    try:
        db.execute(text("DELETE FROM sessions WHERE session_id=:sid"), {"sid": session_id})
        db.commit()
        print(f"[_clear_session] cleared session_id={session_id!r}", flush=True)
    except Exception as e:
        db.rollback()
        print(f"[_clear_session] error: {e}", flush=True)


def get_current_user(
    db: Session = Depends(get_db),
    session_id: Optional[str] = Cookie(None, alias=SESSION_COOKIE_NAME),
) -> Dict[str, Any]:
    """
    쿠키 기반 세션 검증.
    - 세션이 없거나 만료/잘못된 경우 401
    - 유효하면 members 조인 결과를 dict로 반환
    """
    print(f"[get_current_user] session_id cookie={session_id!r}", flush=True)

    if not session_id:
        print("[get_current_user] missing session cookie", flush=True)
        raise HTTPException(status_code=401, detail="Unauthorized")

    now = datetime.utcnow()
    sel = text("""
        SELECT
            m.id, m.role, m.email, m.login_id,
            m.site, m.workcell, m.name, m.phone,
            m.status, m.last_login_at, m.created_at, m.updated_at
        FROM sessions s
        JOIN members m ON m.id = s.member_id
        WHERE s.session_id = :sid
          AND (s.expires_at IS NULL OR s.expires_at > :now)
        LIMIT 1
    """)

    row = db.execute(sel, {"sid": session_id, "now": now}).mappings().first()
    if not row:
        print("[get_current_user] session invalid or expired", flush=True)
        # 만료된 세션이라면 정리
        try:
            _clear_session(db, session_id)
        except Exception:
            pass
        raise HTTPException(status_code=401, detail="Unauthorized")

    user_dict = dict(row)
    print(f"[get_current_user] resolved user id={user_dict.get('id')}, role={user_dict.get('role')}", flush=True)
    return user_dict


def require_admin(current_user: Dict[str, Any] = Depends(get_current_user)) -> Dict[str, Any]:
    """관리자 전용 의존성."""
    if current_user.get("role") != "admin":
        print(f"[require_admin] forbidden role={current_user.get('role')}", flush=True)
        raise HTTPException(status_code=403, detail="Admin only")
    return current_user


# =========================
# /api/login
# =========================

@router.post("/login", response_model=LoginResp)
def login(
    body: LoginBody,
    response: Response,
    db: Session = Depends(get_db),
):
    # 민감 응답 캐시 방지
    response.headers["Cache-Control"] = "no-store"

    # print 로그인 시도 (식별자만)
    try:
        print(f"[/api/login] mode={body.mode!r}, user={body.user!r}", flush=True)
    except Exception:
        pass

    # 1) bcrypt 72바이트 한도 가드 (UTF-8 기준)
    if len(body.password.encode("utf-8")) > 72:
        print("[/api/login] password too long (>72 bytes)", flush=True)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid credentials"
        )

    # 2) 사용자 조회 (관리자=이메일, 일반=login_id)
    if body.mode == "admin":
        q = text("SELECT id, email, password_hash, role FROM members WHERE email=:u LIMIT 1")
    else:
        q = text("SELECT id, login_id, password_hash, role FROM members WHERE login_id=:u LIMIT 1")

    row = db.execute(q, {"u": body.user}).mappings().first()
    if not row:
        print("[/api/login] user not found", flush=True)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    # 3) 비밀번호 검증
    try:
        ok = pwd.verify(body.password, row["password_hash"])
    except Exception as e:
        print(f"[/api/login] bcrypt verify error: {e}", flush=True)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    if not ok:
        print("[/api/login] password mismatch", flush=True)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    # 4) last_login_at 갱신 (선택)
    try:
        if body.mode == "admin":
            db.execute(
                text("UPDATE members SET last_login_at = NOW() WHERE email=:u LIMIT 1"),
                {"u": body.user},
            )
        else:
            db.execute(
                text("UPDATE members SET last_login_at = NOW() WHERE login_id=:u LIMIT 1"),
                {"u": body.user},
            )
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[/api/login] last_login_at update failed: {e}", flush=True)

    # 5) 세션 생성 + 쿠키 발급
    user_id = row.get("login_id") or row.get("email")
    role = row.get("role", "member")
    member_db_id = row["id"]

    try:
        session_id = _create_session(db, member_db_id)
        _set_session_cookie(response, session_id)
    except Exception as e:
        print(f"[/api/login] failed to create session: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to create session")

    print(f"[/api/login] success user_id={user_id!r}, role={role!r}", flush=True)
    return LoginResp(
        ok=True,
        user_id=user_id,
        role=role,
        # 이제 쿠키 기반 세션 사용. token은 데모/호환용 값만 유지.
        token="demo-token-not-for-production",
    )


# =========================
# /api/logout
# =========================

@router.post("/logout", response_model=SimpleOkResp)
def logout(
    response: Response,
    db: Session = Depends(get_db),
    session_id: Optional[str] = Cookie(None, alias=SESSION_COOKIE_NAME),
):
    """
    세션 삭제 + 쿠키 만료.
    """
    response.headers["Cache-Control"] = "no-store"
    print(f"[/api/logout] session_id={session_id!r}", flush=True)

    if session_id:
        try:
            _clear_session(db, session_id)
        except Exception as e:
            print(f"[/api/logout] clear session error: {e}", flush=True)

    # 브라우저 쿠키 제거
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        samesite="lax",
    )

    return SimpleOkResp(ok=True)


# =========================
# /api/me (GET) - 프로필 조회
# =========================
@router.get("/me", response_model=ProfileOut)
def get_me(
    response: Response,
    current_user: Dict[str, Any] = Depends(get_current_user),
):
    """
    현재 로그인 사용자 프로필 조회.
    - 세션 쿠키 기반
    """
    response.headers["Cache-Control"] = "no-store"

    print(f"[/api/me] current_user id={current_user.get('id')}, role={current_user.get('role')}", flush=True)

    payload = {
        "id": current_user["id"],
        "role": current_user.get("role"),
        "email": current_user.get("email"),
        "login_id": current_user.get("login_id"),
        "site": current_user.get("site"),
        "workcell": current_user.get("workcell"),
        "name": current_user.get("name"),
        "phone": current_user.get("phone"),
        "status": current_user.get("status"),
        "last_login_at": _iso(current_user.get("last_login_at")),
        "created_at": _iso(current_user.get("created_at")),
        "updated_at": _iso(current_user.get("updated_at")),
    }
    print(f"[/api/me] response payload = {payload}", flush=True)
    return payload  # Pydantic이 ProfileOut으로 변환


# =========================
# /api/me (PATCH) - 프로필 수정(허용 필드만)
# =========================
@router.patch("/me", response_model=SimpleOkResp)
def patch_me(
    body: ProfilePatchBody,
    response: Response,
    current_user: Dict[str, Any] = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    허용 필드: site, workcell, name, phone
    role/login_id/email 은 무시(읽기 전용).
    세션 기반으로 현재 로그인 사용자만 수정.
    """
    response.headers["Cache-Control"] = "no-store"

    member_id = current_user["id"]
    print(f"[/api/me PATCH] member_id={member_id}, body={body.dict()}", flush=True)

    # 업데이트 셋 구성
    fields: Dict[str, Any] = {}
    if body.site is not None:
        fields["site"] = body.site.strip() if (body.site or body.site == "") else None
    if body.workcell is not None:
        fields["workcell"] = body.workcell.strip() if (body.workcell or body.workcell == "") else None
    if body.name is not None:
        fields["name"] = body.name.strip() if (body.name or body.name == "") else None
    if body.phone is not None:
        fields["phone"] = body.phone.strip() if (body.phone or body.phone == "") else None

    print(f"[/api/me PATCH] update fields={fields}", flush=True)

    if not fields:
        print("[/api/me PATCH] no-op (empty fields)", flush=True)
        return SimpleOkResp(ok=True)  # 변경 없음

    sets = ", ".join([f"{k}=:{k}" for k in fields.keys()])
    q = text(f"UPDATE members SET {sets} WHERE id=:id LIMIT 1")

    try:
        result = db.execute(q, {**fields, "id": member_id})
        db.commit()
        print(f"[/api/me PATCH] updated rowcount={getattr(result, 'rowcount', None)}", flush=True)
    except Exception as e:
        db.rollback()
        print(f"[/api/me PATCH] update error: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to update profile")

    return SimpleOkResp(ok=True)


# =========================
# /api/admin/members (CREATE)
# =========================
@router.post("/admin/members", response_model=CreateMemberResp)
def create_member(
    body: CreateMemberBody,
    response: Response,
    db: Session = Depends(get_db),
    current_admin: Dict[str, Any] = Depends(require_admin),
):
    response.headers["Cache-Control"] = "no-store"
    print(f"[/api/admin/members POST] admin={current_admin.get('login_id') or current_admin.get('email')} create login_id={body.login_id!r}", flush=True)

    # 1) bcrypt 72바이트 한도 체크
    if len(body.password.encode("utf-8")) > 72:
        print("[/api/admin/members POST] password too long", flush=True)
        raise HTTPException(status_code=400, detail="Password too long")

    # 2) 아이디(login_id) 중복 체크
    exists_q = text("SELECT 1 FROM members WHERE login_id=:u LIMIT 1")
    exists = db.execute(exists_q, {"u": body.login_id}).first()
    if exists:
        print("[/api/admin/members POST] duplicate login_id", flush=True)
        raise HTTPException(status_code=409, detail="login_id already exists")

    # 3) 비번 해시
    try:
        pw_hash = pwd.hash(body.password)
    except Exception as e:
        print(f"[/api/admin/members POST] hash error: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to hash password")

    # 4) INSERT (role='operator'로 고정)
    ins_q = text("""
        INSERT INTO members (login_id, password_hash, role)
        VALUES (:u, :ph, 'operator')
    """)
    try:
        db.execute(ins_q, {"u": body.login_id, "ph": pw_hash})
        db.commit()
        print("[/api/admin/members POST] created", flush=True)
    except Exception as e:
        db.rollback()
        print(f"[/api/admin/members POST] insert error: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to create member")

    return CreateMemberResp(ok=True, user_id=body.login_id, role="operator")


# =========================
# /api/admin/members (LIST)
# =========================
@router.get("/admin/members", response_model=ListMembersResp)
def list_members(
    response: Response,
    db: Session = Depends(get_db),
    current_admin: Dict[str, Any] = Depends(require_admin),
):
    response.headers["Cache-Control"] = "no-store"
    print(f"[/api/admin/members GET] admin={current_admin.get('login_id') or current_admin.get('email')} list", flush=True)

    q = text("""
        SELECT id, login_id, email, role
        FROM members
        ORDER BY id ASC
    """)
    rows = db.execute(q).mappings().all() or []
    items = [
        MemberOut(
            id=row["id"],
            login_id=row.get("login_id"),
            email=row.get("email"),
            role=row.get("role", "operator"),
        )
        for row in rows
    ]
    return ListMembersResp(ok=True, items=items)


# =========================
# /api/admin/members/{login_id} (DELETE) - 오퍼레이터만 삭제 허용
# =========================
@router.delete("/admin/members/{login_id}", response_model=SimpleOkResp)
def delete_member(
    response: Response,
    login_id: str = Path(..., min_length=1),
    db: Session = Depends(get_db),
    current_admin: Dict[str, Any] = Depends(require_admin),
):
    response.headers["Cache-Control"] = "no-store"
    print(f"[/api/admin/members DELETE] admin={current_admin.get('login_id') or current_admin.get('email')} login_id={login_id!r}", flush=True)

    # 1) 존재/역할 확인
    row = db.execute(
        text("SELECT id, role FROM members WHERE login_id=:u LIMIT 1"),
        {"u": login_id}
    ).mappings().first()

    if not row:
        print("[/api/admin/members DELETE] not found", flush=True)
        raise HTTPException(status_code=404, detail="Member not found")

    if row.get("role") != "operator":
        print("[/api/admin/members DELETE] forbidden: not operator", flush=True)
        raise HTTPException(status_code=403, detail="Only operator accounts can be deleted")

    # 2) 삭제
    try:
        db.execute(text("DELETE FROM members WHERE id=:id"), {"id": row["id"]})
        db.commit()
        print("[/api/admin/members DELETE] deleted", flush=True)
    except Exception as e:
        db.rollback()
        print(f"[/api/admin/members DELETE] delete error: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to delete member")

    return SimpleOkResp(ok=True)


# =========================
# /api/me/password - 비밀번호 변경
# =========================

class PasswordChangeBody(BaseModel):
    current_password: str
    new_password: str

@router.post("/me/password", response_model=SimpleOkResp)
def change_password(
    body: PasswordChangeBody,
    response: Response,
    current_user: Dict[str, Any] = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    현재 비밀번호 확인 후 새 비밀번호로 변경.
    - 세션 쿠키 기반 사용자 식별
    - 제약: bcrypt 72바이트, 최소 6자
    """
    response.headers["Cache-Control"] = "no-store"

    member_id = current_user["id"]
    print(f"[/api/me/password] member_id={member_id}", flush=True)

    if len(body.new_password.encode("utf-8")) > 72:
        raise HTTPException(status_code=400, detail="Password too long")
    if len(body.new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be >= 6 chars")

    # 사용자 조회
    sel = text("SELECT id, password_hash FROM members WHERE id=:id LIMIT 1")
    row = db.execute(sel, {"id": member_id}).mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="User not found")

    # 현재 비번 검증
    try:
        ok = pwd.verify(body.current_password, row["password_hash"])
    except Exception:
        ok = False
    if not ok:
        raise HTTPException(status_code=401, detail="Invalid current password")

    # 새 비번이 기존과 동일한지(선택)
    try:
        same = pwd.verify(body.new_password, row["password_hash"])
    except Exception:
        same = False
    if same:
        raise HTTPException(status_code=400, detail="New password must differ from current")

    # 해시 & 업데이트
    try:
        new_hash = pwd.hash(body.new_password)
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to hash password")

    upd = text("UPDATE members SET password_hash=:ph WHERE id=:id LIMIT 1")
    try:
        db.execute(upd, {"ph": new_hash, "id": row["id"]})
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[/api/me/password] update error: {e}", flush=True)
        raise HTTPException(status_code=500, detail="Failed to update password")

    print("[/api/me/password] password updated", flush=True)
    return SimpleOkResp(ok=True)
