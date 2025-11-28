from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import uuid
import datetime
import mysql.connector
from mysql.connector import errorcode
from decimal import Decimal, ROUND_HALF_UP
import certifi  # ★ TLS용 루트 CA 경로 제공
import os

# ===== TiDB Cloud 접속 정보 =====
DB_CONFIG = {
    "host": "gateway01.ap-northeast-1.prod.aws.tidbcloud.com",
    "port": 4000,  # ★ TiDB는 3306 아님
    "user": "4H5i9y91oiu7qZU.root",
    "password": "JJS23jK0cQotoe1w",
    "database": "test",
    # TLS 필수
    "ssl_ca": certifi.where(),
    # 행당 즉시 타임아웃(초). 없으면 플랫폼 30초 대기 후 502 날 수 있음
    "connection_timeout": 5,
    "autocommit": True,
}

# 샘플 간격(초) — 기존 5초/15초 등과 일치시켜야 KWh 누적 계산에 정확
DEFAULT_SAMPLE_SEC = 15

# ===== FastAPI 기본 =====
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== 데이터 모델 =====
class PowerIn(BaseModel):
    agent_id: str
    device_alias: str
    device_logical_id: int
    power_w: float
    timestamp: str
    sample_sec: int | None = None  # 에이전트가 보내면 그 값 우선 사용

class CommandIn(BaseModel):
    agent_id: str
    target_alias: str
    action: str  # "on"/"off"/"toggle" 등

class DeviceControlIn(BaseModel):
    status: str  # "on" or "off"

# ===== 요금 계산 =====
RATES_OTHER = [
    (Decimal("910"), Decimal("200"), Decimal("120.0")),
    (Decimal("1600"), Decimal("400"), Decimal("214.6")),
    (Decimal("7300"), Decimal("Infinity"), Decimal("307.3")),
]
RATES_SUMMER = [
    (Decimal("910"), Decimal("300"), Decimal("120.0")),
    (Decimal("1600"), Decimal("450"), Decimal("214.6")),
    (Decimal("7300"), Decimal("Infinity"), Decimal("307.3")),
]

def calc_bill_from_kwh(monthly_kwh: float) -> int:
    now = datetime.datetime.now()
    rates = RATES_SUMMER if now.month in (7, 8) else RATES_OTHER
    remaining = Decimal(str(monthly_kwh))
    total = Decimal("0")
    last_limit = Decimal("0")
    base = Decimal("0")
    for base_fee, limit, rate in rates:
        if remaining <= 0:
            break
        if limit == Decimal("Infinity"):
            kwh_in_tier = remaining
        else:
            kwh_in_tier = min(remaining, limit - last_limit)
        total += kwh_in_tier * rate
        remaining -= kwh_in_tier
        base = base_fee
        if limit != Decimal("Infinity"):
            last_limit = limit
    total += base
    return int(total.quantize(Decimal("1"), rounding=ROUND_HALF_UP))

# ===== DB 연결 유틸 =====
def get_conn():
    # 각 요청마다 새 커넥션. Pool을 쓸 수도 있지만 TiDB 무료티어면 단순화 권장
    return mysql.connector.connect(**DB_CONFIG)

def ensure_schema():
    """최소 스키마 생성(없으면). 배포 직후 한 번 호출."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS devices (
          id INT PRIMARY KEY,
          agent_id VARCHAR(64) NOT NULL,
          alias VARCHAR(128) NOT NULL,
          last_power_w DOUBLE DEFAULT 0,
          last_seen DATETIME,
          UNIQUE KEY idx_agent_alias (agent_id, alias)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS power_logs (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          agent_id VARCHAR(64) NOT NULL,
          device_id INT NOT NULL,
          power_w DOUBLE NOT NULL,
          ts DATETIME NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS commands (
          id VARCHAR(64) PRIMARY KEY,
          agent_id VARCHAR(64) NOT NULL,
          target_alias VARCHAR(128) NOT NULL,
          action VARCHAR(32) NOT NULL,
          status VARCHAR(32) NOT NULL,
          created_at DATETIME NOT NULL,
          acked_at DATETIME NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS notifications (
          id BIGINT PRIMARY KEY AUTO_INCREMENT,
          agent_id VARCHAR(64) NOT NULL,
          device_id INT NULL,
          level VARCHAR(16) NOT NULL,
          title VARCHAR(255) NOT NULL,
          message TEXT NOT NULL,
          created_at DATETIME NOT NULL,
          is_read TINYINT(1) NOT NULL DEFAULT 0
        );
        """)
        conn.commit()
    except Exception as e:
        print(f"[ensure_schema] error: {e}")
    finally:
        try:
            cur.close()
            conn.close()
        except:
            pass

@app.on_event("startup")
def _on_startup():
    ensure_schema()

# ===== 헬스체크 =====
@app.get("/health")
def health():
    return {"ok": True, "uptime": datetime.datetime.utcnow().isoformat() + "Z"}

@app.get("/health/db")
def health_db():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        conn.close()
        return {"ok": True}
    except Exception as e:
        # 여기서 503을 내보내면 프록시가 502 대신 정확한 원인이 보임
        raise HTTPException(status_code=503, detail=f"DB error: {e}")

# ===== 데이터 수집 =====
@app.post("/power")
def ingest_power(data: PowerIn):
    # 타임스탬프 파싱/정규화
    ts = data.timestamp.replace("Z", "").replace("T", " ")
    sample_sec = data.sample_sec or DEFAULT_SAMPLE_SEC

    try:
        conn = get_conn()
        cur = conn.cursor()

        # devices upsert
        cur.execute(
            """
            INSERT INTO devices (id, agent_id, alias, last_power_w, last_seen)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              alias=VALUES(alias),
              last_power_w=VALUES(last_power_w),
              last_seen=VALUES(last_seen);
            """,
            (data.device_logical_id, data.agent_id, data.device_alias, data.power_w, ts),
        )

        # power_logs insert
        cur.execute(
            """
            INSERT INTO power_logs (agent_id, device_id, power_w, ts)
            VALUES (%s, %s, %s, %s);
            """,
            (data.agent_id, data.device_logical_id, data.power_w, ts),
        )

        # ---- 알림 생성 예시(대기전력/과소비) ----
        # 필요시 임계치 바꿔도 됨
        STANDBY_W = 5.0
        HIGH_W = 800.0

        if 0 < data.power_w < STANDBY_W:
            cur.execute(
                """
                INSERT INTO notifications
                  (agent_id, device_id, level, title, message, created_at, is_read)
                VALUES (%s, %s, %s, %s, %s, %s, 0);
                """,
                (
                    data.agent_id, data.device_logical_id, "info",
                    "대기전력 감지",
                    f"{data.device_alias} 대기전력 {data.power_w:.1f}W",
                    ts
                )
            )
        elif data.power_w >= HIGH_W:
            cur.execute(
                """
                INSERT INTO notifications
                  (agent_id, device_id, level, title, message, created_at, is_read)
                VALUES (%s, %s, %s, %s, %s, %s, 0);
                """,
                (
                    data.agent_id, data.device_logical_id, "warn",
                    "전력 과소비 감지",
                    f"{data.device_alias} 순간 {data.power_w:.0f}W",
                    ts
                )
            )

        conn.commit()
        cur.close()
        conn.close()
        return {"ok": True}
    except mysql.connector.Error as e:
        # DB 연결/쿼리 실패는 503으로 반환 → 프록시 502 방지
        raise HTTPException(status_code=503, detail=f"DB error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ===== 조회 API들 =====
@app.get("/power/latest")
def latest_power():
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT agent_id, device_id, power_w, ts
            FROM power_logs
            ORDER BY ts DESC
            LIMIT 50;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")

@app.post("/command")
def create_command(cmd: CommandIn):
    cmd_id = str(uuid.uuid4())
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO commands (id, agent_id, target_alias, action, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (cmd_id, cmd.agent_id, cmd.target_alias, cmd.action, "pending", now),
        )
        conn.commit()
        cur.close()
        conn.close()
        return {"ok": True, "id": cmd_id}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")

@app.put("/api/devices/{device_id}/power")
def control_device_power(device_id: int, control: DeviceControlIn):
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT agent_id, alias FROM devices WHERE id = %s", (device_id,))
        device = cur.fetchone()
        if not device:
            cur.close(); conn.close()
            raise HTTPException(status_code=404, detail="Device not found")

        cmd_id = str(uuid.uuid4())
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        cur.execute(
            """
            INSERT INTO commands (id, agent_id, target_alias, action, status, created_at)
            VALUES (%s, %s, %s, %s, %s, %s);
            """,
            (cmd_id, device['agent_id'], device['alias'], control.status, "pending", now),
        )
        conn.commit()
        cur.close(); conn.close()
        return {"success": True, "message": f"{device['alias']} -> {control.status}"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")

@app.get("/commands")
def get_commands(agent_id: str = Query(...)):
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, target_alias, action
            FROM commands
            WHERE agent_id = %s AND status = 'pending'
            ORDER BY created_at ASC;
            """,
            (agent_id,),
        )
        rows = cur.fetchall()
        cur.close(); conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")

@app.post("/commands/{cmd_id}/ack")
def ack_command(cmd_id: str):
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE commands
            SET status = 'acked', acked_at = %s
            WHERE id = %s;
            """,
            (now, cmd_id),
        )
        conn.commit()
        cur.close(); conn.close()
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")

@app.get("/api/notifications")
def get_notifications(agent_id: str | None = Query(None)):
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)
        query = "SELECT * FROM notifications"
        params = []
        if agent_id:
            query += " WHERE agent_id = %s"
            params.append(agent_id)
        query += " ORDER BY created_at DESC LIMIT 50"
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close(); conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")

@app.put("/api/notifications/{noti_id}/read")
def read_notification(noti_id: int):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("UPDATE notifications SET is_read = 1 WHERE id = %s", (noti_id,))
        conn.commit()
        cur.close(); conn.close()
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")

@app.get("/api/devices")
def get_devices_list(agent_id: str = Query(...)):
    try:
        conn = get_conn()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM devices WHERE agent_id = %s", (agent_id,))
        rows = cur.fetchall()
        # 안전하게 필드 보강
        for row in rows:
            row.setdefault("status", "off")
            row.setdefault("device_name", row.get("alias"))
        cur.close(); conn.close()
        return rows
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")

# ===== 로컬 실행 =====
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
