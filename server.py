from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import uuid
import datetime
import mysql.connector
from decimal import Decimal, ROUND_HALF_UP

# ===== 1) DB 설정 =====
DB_CONFIG = {
    "host": "gateway01.ap-northeast-1.prod.aws.tidbcloud.com",
    "port": 4000,
    "user": "4H5i9y91oiu7qZU.root",
    "password": "JJS23jK0cQotoe1w",
    "database": "test",
    "ssl_ca": certifi.where(),
    "ssl_disabled": False,
    "ssl_verify_identity": True,   # ← 추가
    "use_pure": True,
}

# ===== 2) FastAPI 기본 =====
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== 3) 들어오는 데이터 모델 =====
class PowerIn(BaseModel):
    agent_id: str
    device_alias: str
    device_logical_id: int
    power_w: float
    timestamp: str

class CommandIn(BaseModel):
    agent_id: str
    target_alias: str
    action: str  # "on" or "off"

# ===== 4) 요금 계산 함수 =====
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

# ===== 5) DB 커넥션 =====
def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

# ===== 6) /power : 에이전트가 전송하는 곳 =====
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
        (
            data.device_logical_id,
            data.agent_id,
            data.device_alias,
            data.power_w,
            ts,
        ),
    )

    # power_logs insert
    cur.execute(
        """
        INSERT INTO power_logs (agent_id, device_id, power_w, ts)
        VALUES (%s, %s, %s, %s);
        """,
        (
            data.agent_id,
            data.device_logical_id,
            data.power_w,
            ts,
        ),
    )

    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True}

# ===== 7) 최근 로그 보기 =====
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

# ===== 8) 원격 명령 만들기 =====
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

# ===== 9) 에이전트가 가져갈 명령 =====
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

# ===== 10) 명령 ACK =====
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

# ===== 11) 오늘(지금까지) 사용량 =====
@app.get("/usage/today")
def usage_today(agent_id: str = Query(...)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    cur.execute("""
        SELECT
            d.alias,
            pl.device_id,
            SUM(pl.power_w / 1000 * 5 / 3600) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= CURDATE()
        GROUP BY pl.device_id, d.alias;
    """, (agent_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    total_kwh = sum(r["kwh"] or 0 for r in rows)
    estimated_bill = calc_bill_from_kwh(total_kwh)

    return {
        "agent_id": agent_id,
        "total_kwh": float(total_kwh),
        "estimated_bill": estimated_bill,
        "devices": rows
    }

# ===== 12) 특정 일자 일간 사용량 =====
@app.get("/usage/daily")
def usage_daily(
    agent_id: str = Query(...),
    target_date: str | None = Query(None)
):
    if target_date:
        day = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
    else:
        day = datetime.date.today()

    start_dt = day.strftime("%Y-%m-%d 00:00:00")
    end_dt = (day + datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
            d.alias,
            pl.device_id,
            SUM(pl.power_w / 1000 * 5 / 3600) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= %s
          AND pl.ts < %s
        GROUP BY pl.device_id, d.alias;
    """, (agent_id, start_dt, end_dt))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    total_kwh = sum(r["kwh"] or 0 for r in rows)
    estimated_bill = calc_bill_from_kwh(total_kwh)

    return {
        "agent_id": agent_id,
        "date": day.isoformat(),
        "total_kwh": float(total_kwh),
        "estimated_bill": estimated_bill,
        "devices": rows,
    }

# ===== 13) 월간 사용량 =====
@app.get("/usage/monthly")
def usage_monthly(agent_id: str = Query(...)):
    month_start = datetime.datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_str = month_start.strftime("%Y-%m-%d %H:%M:%S")

    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
            d.alias,
            pl.device_id,
            SUM(pl.power_w / 1000 * 5 / 3600) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= %s
        GROUP BY pl.device_id, d.alias;
    """, (agent_id, start_str))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    total_kwh = sum(r["kwh"] or 0 for r in rows)
    estimated_bill = calc_bill_from_kwh(total_kwh)

    return {
        "agent_id": agent_id,
        "month": month_start.strftime("%Y-%m"),
        "total_kwh": float(total_kwh),
        "estimated_bill": estimated_bill,
        "devices": rows,
    }

# ===== 14) 서버 실행 =====
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
