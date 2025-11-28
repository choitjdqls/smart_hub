from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import uuid
import datetime
import mysql.connector
from decimal import Decimal, ROUND_HALF_UP
import certifi  # ✅ TiDB Cloud TLS 검증용 CA 번들

# =========================
# 1) DB 설정 (TiDB Cloud)
# =========================
DB_CONFIG = {
    "host": "gateway01.ap-northeast-1.prod.aws.tidbcloud.com",
    "port": 4000,  # ✅ 주의: 3306 아니라 4000
    "user": "4H5i9y91oiu7qZU.root",
    "password": "JJS23jK0cQotoe1w",
    "database": "test",
    # TLS 활성화 (TiDB Cloud는 TLS 필수)
    "ssl_ca": certifi.where(),   # ✅ 시스템 CA 번들로 인증
    "ssl_disabled": False,
    "use_pure": True,
}

# =========================
# 2) FastAPI 기본
# =========================
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# 3) 데이터 모델
# =========================
class PowerIn(BaseModel):
    agent_id: str
    device_alias: str
    device_logical_id: int
    power_w: float
    timestamp: str

class CommandIn(BaseModel):
    agent_id: str
    target_alias: str
    action: str  # "on" | "off" | "toggle" 등

class DeviceControlIn(BaseModel):
    status: str  # "on" | "off"

# 알림 읽음 처리 모형(필요시 사용)
class NotificationReadIn(BaseModel):
    read: bool

# =========================
# 4) 요금 계산 함수
# =========================
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

# =========================
# 5) DB 유틸/스키마 생성
# =========================
def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

def init_db():
    conn = get_conn()
    cur = conn.cursor()
    # devices
    cur.execute("""
    CREATE TABLE IF NOT EXISTS devices (
      id INT PRIMARY KEY,
      agent_id VARCHAR(64) NOT NULL,
      alias VARCHAR(64) NOT NULL,
      last_power_w FLOAT NULL,
      last_seen DATETIME NULL,
      status VARCHAR(16) NULL,
      KEY idx_devices_agent (agent_id)
    );
    """)
    # power_logs
    cur.execute("""
    CREATE TABLE IF NOT EXISTS power_logs (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      agent_id VARCHAR(64) NOT NULL,
      device_id INT NOT NULL,
      power_w FLOAT NOT NULL,
      ts DATETIME NOT NULL,
      KEY idx_pl_agent (agent_id),
      KEY idx_pl_ts (ts),
      KEY idx_pl_device (device_id)
    );
    """)
    # commands
    cur.execute("""
    CREATE TABLE IF NOT EXISTS commands (
      id VARCHAR(64) PRIMARY KEY,
      agent_id VARCHAR(64) NOT NULL,
      target_alias VARCHAR(64) NOT NULL,
      action VARCHAR(16) NOT NULL,
      status VARCHAR(16) NOT NULL,
      created_at DATETIME NOT NULL,
      acked_at DATETIME NULL,
      KEY idx_cmd_agent (agent_id),
      KEY idx_cmd_status (status)
    );
    """)
    # notifications (알림센터)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS notifications (
      id BIGINT AUTO_INCREMENT PRIMARY KEY,
      agent_id VARCHAR(64) NOT NULL,
      type VARCHAR(32) NOT NULL,
      title VARCHAR(128) NOT NULL,
      message VARCHAR(512) NOT NULL,
      device_id INT NULL,
      amount_won INT NULL,
      created_at DATETIME NOT NULL,
      is_read TINYINT(1) NOT NULL DEFAULT 0,
      KEY idx_noti_agent (agent_id),
      KEY idx_noti_created (created_at)
    );
    """)
    conn.commit()
    cur.close()
    conn.close()

@app.on_event("startup")
def _startup():
    init_db()

# =========================
# 6) /power : 데이터 수집
# =========================
@app.post("/power")
def ingest_power(data: PowerIn):
    conn = get_conn()
    cur = conn.cursor()
    ts = data.timestamp.replace("Z", "").replace("T", " ")

    # devices upsert
    cur.execute(
        """
        INSERT INTO devices (id, agent_id, alias, last_power_w, last_seen)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          alias = VALUES(alias),
          last_power_w = VALUES(last_power_w),
          last_seen = VALUES(last_seen);
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

    # (옵션) 간단 알림 샘플: 대기전력 감지 시 알림 생성
    STANDBY_W = 5.0
    if 0 < data.power_w < STANDBY_W:
        cur.execute(
            """
            INSERT INTO notifications
              (agent_id, type, title, message, device_id, created_at, is_read)
            VALUES
              (%s, %s, %s, %s, %s, %s, 0);
            """,
            (
                data.agent_id,
                "standby",
                f"[대기전력] {data.device_alias}",
                f"{data.device_alias}가 {data.power_w:.1f}W로 대기 상태로 보입니다.",
                data.device_logical_id,
                ts,
            ),
        )

    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True}

# =========================
# 7) 최근 로그
# =========================
@app.get("/power/latest")
def latest_power():
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

# =========================
# 8) 원격 명령
# =========================
@app.post("/command")
def create_command(cmd: CommandIn):
    cmd_id = str(uuid.uuid4())
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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

# 프론트엔드용 PUT 제어 래퍼
@app.put("/api/devices/{device_id}/power")
def control_device_power(device_id: int, control: DeviceControlIn):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT agent_id, alias FROM devices WHERE id = %s", (device_id,))
    device = cur.fetchone()
    if not device:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Device not found")

    cmd_id = str(uuid.uuid4())
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        """
        INSERT INTO commands (id, agent_id, target_alias, action, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        (cmd_id, device["agent_id"], device["alias"], control.status, "pending", now),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"success": True, "message": f"{device['alias']} -> {control.status}"}

# =========================
# 9) 에이전트 명령 조회/ACK
# =========================
@app.get("/commands")
def get_commands(agent_id: str = Query(...)):
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
    cur.close()
    conn.close()
    return rows

@app.post("/commands/{cmd_id}/ack")
def ack_command(cmd_id: str):
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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
    cur.close()
    conn.close()
    return {"ok": True}

# =========================
# 10) 알림 API
# =========================
@app.get("/api/notifications")
def get_notifications(agent_id: str = Query(None)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        if agent_id:
            cur.execute(
                """
                SELECT * FROM notifications
                WHERE agent_id = %s
                ORDER BY created_at DESC
                LIMIT 50;
                """,
                (agent_id,),
            )
        else:
            cur.execute(
                """
                SELECT * FROM notifications
                ORDER BY created_at DESC
                LIMIT 50;
                """
            )
        rows = cur.fetchall()
        return rows
    finally:
        cur.close()
        conn.close()

@app.put("/api/notifications/{noti_id}/read")
def read_notification(noti_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE notifications SET is_read = 1 WHERE id = %s", (noti_id,))
    conn.commit()
    cur.close()
    conn.close()
    return {"success": True}

# =========================
# 11) 프론트: 장치 목록
# =========================
@app.get("/api/devices")
def get_devices_list(agent_id: str = Query(...)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM devices WHERE agent_id = %s", (agent_id,))
    rows = cur.fetchall()
    for row in rows:
        row.setdefault("status", "off")
        row.setdefault("device_name", row.get("alias"))
    cur.close()
    conn.close()
    return rows

# =========================
# 12~14) 사용량 API (단순 5초 샘플 가정)
#   - 실제로는 에이전트에서 sample_sec을 같이 보내고 누적하는 편이 정확
# =========================
DEFAULT_SAMPLE_SEC = 5  # power_w * (sec/3600) 로 Wh→kWh 환산

@app.get("/usage/today")
def usage_today(agent_id: str = Query(...)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT d.alias, pl.device_id,
               SUM(pl.power_w / 1000 * %s / 3600) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= CURDATE()
        GROUP BY pl.device_id, d.alias;
    """, (DEFAULT_SAMPLE_SEC, agent_id))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    total_kwh = sum(r["kwh"] or 0 for r in rows)
    estimated_bill = calc_bill_from_kwh(float(total_kwh))
    return {
        "agent_id": agent_id,
        "total_kwh": float(total_kwh),
        "estimated_bill": estimated_bill,
        "devices": rows
    }

@app.get("/usage/daily")
def usage_daily(agent_id: str = Query(...), target_date: str | None = Query(None)):
    if target_date:
        day = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
    else:
        day = datetime.date.today()
    start_dt = day.strftime("%Y-%m-%d 00:00:00")
    end_dt = (day + datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT d.alias, pl.device_id,
               SUM(pl.power_w / 1000 * %s / 3600) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= %s AND pl.ts < %s
        GROUP BY pl.device_id, d.alias;
    """, (DEFAULT_SAMPLE_SEC, agent_id, start_dt, end_dt))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    total_kwh = sum(r["kwh"] or 0 for r in rows)
    estimated_bill = calc_bill_from_kwh(float(total_kwh))
    return {
        "agent_id": agent_id,
        "date": day.isoformat(),
        "total_kwh": float(total_kwh),
        "estimated_bill": estimated_bill,
        "devices": rows
    }

@app.get("/usage/monthly")
def usage_monthly(agent_id: str = Query(...)):
    month_start = datetime.datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_str = month_start.strftime("%Y-%m-%d %H:%M:%S")

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT d.alias, pl.device_id,
               SUM(pl.power_w / 1000 * %s / 3600) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= %s
        GROUP BY pl.device_id, d.alias;
    """, (DEFAULT_SAMPLE_SEC, agent_id, start_str))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    total_kwh = sum(r["kwh"] or 0 for r in rows)
    estimated_bill = calc_bill_from_kwh(float(total_kwh))
    return {
        "agent_id": agent_id,
        "month": month_start.strftime("%Y-%m"),
        "total_kwh": float(total_kwh),
        "estimated_bill": estimated_bill,
        "devices": rows
    }

# 헬스 체크
@app.get("/health")
def health():
    return {"ok": True}

# =========================
# 15) 로컬 실행
# =========================
if __name__ == "__main__":
    # 로컬: uvicorn server:app --reload
    uvicorn.run(app, host="0.0.0.0", port=8000)
