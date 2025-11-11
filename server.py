from fastapi import FastAPI, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
import os
import datetime
import mysql.connector

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== DB 설정: Railway Variables에 넣었으면 os.getenv로 읽힘 =====
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mainline.proxy.rlwy.net"),
    "port": int(os.getenv("DB_PORT", "31299")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "wZxTvdwprKhKAkkyKzbeQJbqQxHxeXCf"),
    "database": os.getenv("DB_NAME", "railway"),
}

# ===== Pydantic 모델 =====
class PowerIn(BaseModel):
    agent_id: str
    device_alias: str
    device_logical_id: int
    power_w: float
    timestamp: str  # ISO 문자열

class CommandIn(BaseModel):
    agent_id: str
    target_alias: str
    action: str  # "on" | "off"


def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


# ===== 1) 전력 수신 =====
@app.post("/power")
def ingest_power(data: PowerIn):
    """에이전트가 보내는 전력을 DB에 저장"""
    ts = datetime.datetime.fromisoformat(data.timestamp.replace("Z", "+00:00"))

    conn = get_conn()
    cur = conn.cursor()

    # 1) devices 테이블 upsert 비슷하게
    #   id는 에이전트가 준 device_logical_id를 그대로 쓴다 (우리가 설계한대로)
    cur.execute(
        """
        INSERT INTO devices (id, agent_id, alias, last_power_w, last_seen)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            alias = VALUES(alias),
            last_power_w = VALUES(last_power_w),
            last_seen = VALUES(last_seen)
        """,
        (
            data.device_logical_id,
            data.agent_id,
            data.device_alias,
            data.power_w,
            ts,
        ),
    )

    # 2) power_logs 에 한 줄 저장
    cur.execute(
        """
        INSERT INTO power_logs (agent_id, device_id, power_w, ts)
        VALUES (%s, %s, %s, %s)
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


# ===== 2) 최근 전력 보기 =====
@app.get("/power/latest")
def latest_power(agent_id: Optional[str] = None):
    """
    최근에 들어온 전력 로그 몇 개 보기
    """
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    if agent_id:
        cur.execute(
            """
            SELECT pl.id, pl.agent_id, pl.device_id, d.alias, pl.power_w, pl.ts
            FROM power_logs pl
            LEFT JOIN devices d ON pl.device_id = d.id
            WHERE pl.agent_id = %s
            ORDER BY pl.ts DESC
            LIMIT 50
            """,
            (agent_id,),
        )
    else:
        cur.execute(
            """
            SELECT pl.id, pl.agent_id, pl.device_id, d.alias, pl.power_w, pl.ts
            FROM power_logs pl
            LEFT JOIN devices d ON pl.device_id = d.id
            ORDER BY pl.ts DESC
            LIMIT 50
            """
        )

    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ===== 3) 명령 등록 =====
@app.post("/command")
def create_command(cmd: CommandIn):
    import uuid

    cmd_id = str(uuid.uuid4())
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO commands (id, agent_id, target_alias, action, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            cmd_id,
            cmd.agent_id,
            cmd.target_alias,
            cmd.action,
            "pending",
            datetime.datetime.utcnow(),
        ),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True, "id": cmd_id}


# ===== 4) 에이전트가 가져갈 명령 =====
@app.get("/commands")
def get_commands(agent_id: str = Query(...)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT id, target_alias, action
        FROM commands
        WHERE agent_id = %s AND status = 'pending'
        """,
        (agent_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


# ===== 5) 명령 처리했다고 알림 =====
@app.post("/commands/{cmd_id}/ack")
def ack_command(cmd_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE commands
        SET status = 'done', acked_at = %s
        WHERE id = %s
        """,
        (datetime.datetime.utcnow(), cmd_id),
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True}


# ===== 요금 계산용 유틸 =====
from decimal import Decimal, ROUND_HALF_UP

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

def calc_bill(kwh: Decimal) -> int:
    now = datetime.datetime.now()
    rates = RATES_SUMMER if now.month in (7, 8) else RATES_OTHER
    remaining = kwh
    total = Decimal("0")
    last_limit = Decimal("0")
    base_fee = Decimal("0")
    for base, limit, rate in rates:
        if remaining <= 0:
            break
        if limit == Decimal("Infinity"):
            use = remaining
        else:
            use = min(remaining, limit - last_limit)
        total += use * rate
        remaining -= use
        base_fee = base
        last_limit = limit
    total += base_fee
    return int(total.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


# ===== 6) 오늘 사용량 =====
@app.get("/usage/today")
def usage_today(agent_id: str = Query(...)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT pl.device_id, d.alias,
               SUM(pl.power_w / 1000 * 5 / 3600) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= CURDATE()
        GROUP BY pl.device_id, d.alias
        """,
        (agent_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    total_kwh = sum(Decimal(str(r["kwh"])) for r in rows)
    bill = calc_bill(total_kwh)

    return {
        "agent_id": agent_id,
        "total_kwh": float(total_kwh),
        "estimated_bill": bill,
        "devices": rows,
    }


# ===== 7) 월간 사용량 =====
@app.get("/usage/monthly")
def usage_monthly(agent_id: str = Query(...)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute(
        """
        SELECT pl.device_id, d.alias,
               SUM(pl.power_w / 1000 * 5 / 3600) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= DATE_FORMAT(CURRENT_DATE(), '%%Y-%%m-01')
        GROUP BY pl.device_id, d.alias
        """,
        (agent_id,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    total_kwh = sum(Decimal(str(r["kwh"])) for r in rows)
    bill = calc_bill(total_kwh)

    return {
        "agent_id": agent_id,
        "total_kwh": float(total_kwh),
        "estimated_bill": bill,
        "devices": rows,
    }
