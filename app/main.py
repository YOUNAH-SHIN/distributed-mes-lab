import os
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from .db import get_db, ping
from .auth import router as auth_router
from .routers.dashboard import router as dashboard_router
from .routers.analysis_timeseries import router as analysis_router
from .routers.analyze import router as analyze_router
from .routers.alert import router as alert_router

# .env 로드
load_dotenv()

app = FastAPI(
    title="Workcell Backend",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
)

# ---------- CORS ----------
# 예: CORS_ORIGINS="https://twinworks.app,https://imp.twinworks.app"
raw_origins = os.getenv("CORS_ORIGINS", "")
allow_origins = [o.strip() for o in raw_origins.split(",") if o.strip()]

# 체크리스트:
# - allow_credentials=True
# - 정확한 origin만 명시 (와일드카드 금지)
#   👉 allow_origins 에 "*" 절대 안 넣음
app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,   # 반드시 .env에서 설정
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- 공통 Cache-Control ----------
# 모든 /api/* 응답에 기본적으로 no-store 적용
# (개별 핸들러에서 이미 설정한 경우는 그대로 둠)
@app.middleware("http")
async def add_cache_control_no_store(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path

    if path.startswith("/api/"):
        # 이미 명시된 경우는 덮어쓰지 않음
        if "Cache-Control" not in response.headers:
            response.headers["Cache-Control"] = "no-store"
    return response


# ---------- 라우터 ----------
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(analysis_router)
app.include_router(analyze_router)
app.include_router(alert_router)

# ---------- 헬스체크 ----------
@app.get("/api/health")
def health(db=Depends(get_db)):
    # add_cache_control_no_store 미들웨어에서 no-store 헤더 자동 부여
    return {"db": bool(ping(db)), "status": "ok"}
