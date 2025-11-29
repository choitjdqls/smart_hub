from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import uuid
import datetime as dt
import mysql.connector
import certifi
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

# ===== 3) 데이터 모델 =====
class PowerIn(BaseModel):
    agent_id: str
    device_alias: str
    device_logical_id: int
    power_w: float
    timestamp: str
    sample_sec: int | None = None  # 에이전트가 보내면 사용(없으면 60초로 처리)

class CommandIn(BaseModel):
    agent_id: str
    target_alias: str
    action: str  # "on" / "off" / "toggle" / "__ROUTINE__" 등

class DeviceControlIn(BaseModel):
    status: str  # "on" or "off"

class NotificationReadIn(BaseModel):
    read: bool

# ===== 4) 요금 계산 =====
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
    now = dt.datetime.now()
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
    # TiDB TLS가 필요한 경우, mysql-connector-python은 기본 TLS를 사용하므로
    # 추가 설정 없이도 동작합니다. (필요시 client_flags/ssl_args 추가)
    return mysql.connector.connect(**DB_CONFIG)

# ===== 6) /power : 데이터 수집 =====
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

    # power_logs insert (sample_sec 없으면 60초로 기록)
    sample_sec = data.sample_sec if data.sample_sec and data.sample_sec > 0 else 60
    try:
        cur.execute(
            """
            INSERT INTO power_logs (agent_id, device_id, power_w, ts, sample_sec)
            VALUES (%s, %s, %s, %s, %s);
            """,
            (data.agent_id, data.device_logical_id, data.power_w, ts, sample_sec),
        )
    except mysql.connector.Error as e:
        # sample_sec 컬럼이 없는 경우의 백업 쿼리
        if "Unknown column 'sample_sec'" in str(e):
            cur.execute(
                """
                INSERT INTO power_logs (agent_id, device_id, power_w, ts)
                VALUES (%s, %s, %s, %s);
                """,
                (data.agent_id, data.device_logical_id, data.power_w, ts),
            )
        else:
            raise

    conn.commit()
    cur.close(); conn.close()
    return {"ok": True}

# ===== 7) 최근 로그 =====
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
    cur.close(); conn.close()
    return rows

# ===== 8) 원격 명령 생성 =====
@app.post("/command")
def create_command(cmd: CommandIn):
    cmd_id = str(uuid.uuid4())
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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
    cur.close(); conn.close()
    return {"ok": True, "id": cmd_id}

# ===== 프론트 호환 제어(옵션) =====
@app.put("/api/devices/{device_id}/power")
def control_device_power(device_id: int, control: DeviceControlIn):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT agent_id, alias FROM devices WHERE id = %s", (device_id,))
    device = cur.fetchone()
    if not device:
        cur.close(); conn.close()
        raise HTTPException(status_code=404, detail="Device not found")

    cmd_id = str(uuid.uuid4())
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        """
        INSERT INTO commands (id, agent_id, target_alias, action, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        (cmd_id, device['agent_id'], device['alias'], control.status, "pending", now),
    )
    conn.commit()
    cur.close(); conn.close()
    return {"success": True, "message": f"Device {device['alias']} turned {control.status}"}

# ===== 9) 에이전트 명령 조회/ACK =====
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
    cur.close(); conn.close()
    return rows

@app.post("/commands/{cmd_id}/ack")
def ack_command(cmd_id: str):
    now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
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

# ===== 10) 알림(선택 구현) =====
@app.get("/api/notifications")
def get_notifications(agent_id: str = Query(None)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        q = "SELECT * FROM notifications"
        params = []
        if agent_id:
            q += " WHERE agent_id = %s"
            params.append(agent_id)
        q += " ORDER BY created_at DESC LIMIT 20"
        cur.execute(q, tuple(params))
        rows = cur.fetchall()
        return rows
    except mysql.connector.Error:
        return []
    finally:
        cur.close(); conn.close()

@app.put("/api/notifications/{noti_id}/read")
def read_notification(noti_id: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE notifications SET is_read = 1 WHERE id = %s", (noti_id,))
        conn.commit()
        return {"success": True}
    except mysql.connector.Error as err:
        return {"success": False, "error": str(err)}
    finally:
        cur.close(); conn.close()

# ===== 11) 사용량 요약 (today/daily/monthly) =====
@app.get("/usage/today")
def usage_today(agent_id: str = Query(...)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    # sample_sec 있으면 그 값, 없으면 60초 사용
    cur.execute("""
        SELECT
          d.alias,
          pl.device_id,
          SUM( (pl.power_w/1000.0) * (COALESCE(pl.sample_sec, 60)/3600.0) ) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= CURDATE()
        GROUP BY pl.device_id, d.alias;
    """, (agent_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
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
        day = dt.datetime.strptime(target_date, "%Y-%m-%d").date()
    else:
        day = dt.date.today()
    start_dt = day.strftime("%Y-%m-%d 00:00:00")
    end_dt = (day + dt.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          d.alias,
          pl.device_id,
          SUM( (pl.power_w/1000.0) * (COALESCE(pl.sample_sec, 60)/3600.0) ) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= %s
          AND pl.ts < %s
        GROUP BY pl.device_id, d.alias;
    """, (agent_id, start_dt, end_dt))
    rows = cur.fetchall()
    cur.close(); conn.close()
    total_kwh = sum(r["kwh"] or 0 for r in rows)
    estimated_bill = calc_bill_from_kwh(float(total_kwh))
    return {
        "agent_id": agent_id,
        "date": day.isoformat(),
        "total_kwh": float(total_kwh),
        "estimated_bill": estimated_bill,
        "devices": rows,
    }

@app.get("/usage/monthly")
def usage_monthly(agent_id: str = Query(...)):
    month_start = dt.datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_str = month_start.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          d.alias,
          pl.device_id,
          SUM( (pl.power_w/1000.0) * (COALESCE(pl.sample_sec, 60)/3600.0) ) AS kwh
        FROM power_logs pl
        JOIN devices d ON pl.device_id = d.id
        WHERE pl.agent_id = %s
          AND pl.ts >= %s
        GROUP BY pl.device_id, d.alias;
    """, (agent_id, start_str))
    rows = cur.fetchall()
    cur.close(); conn.close()
    total_kwh = sum(r["kwh"] or 0 for r in rows)
    estimated_bill = calc_bill_from_kwh(float(total_kwh))
    return {
        "agent_id": agent_id,
        "month": month_start.strftime("%Y-%m"),
        "total_kwh": float(total_kwh),
        "estimated_bill": estimated_bill,
        "devices": rows,
    }

# ===== 12) 대기전력 분석 (/api/analysis/waste) =====
def month_kwh_total(agent_id: str) -> float:
    month_start = dt.datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_str = month_start.strftime("%Y-%m-%d %H:%M:%S")
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT
          SUM( (pl.power_w/1000.0) * (COALESCE(pl.sample_sec, 60)/3600.0) ) AS kwh
        FROM power_logs pl
        WHERE pl.agent_id = %s
          AND pl.ts >= %s
    """, (agent_id, start_str))
    row = cur.fetchone()
    cur.close(); conn.close()
    return float(row["kwh"] or 0.0)

@app.get("/api/analysis/waste")
def analysis_waste(
    agent_id: str = Query(...),
    threshold_w: float = Query(5.0, description="대기전력 기준 W(기본 5W)"),
    fresh_sec: int = Query(180, description="최근 N초 이내 보고만 현재 켜짐으로 간주"),
    assume_hours_per_day: float = Query(24.0, description="대기 상태 하루 시간(기본 24h)")
):
    """
    켜져 있으나 소비가 작아(대기전력) 보이는 기기 목록과 '끄면 월 절약액' 추정
    """
    now = dt.datetime.utcnow()
    fresh_after = (now - dt.timedelta(seconds=fresh_sec)).strftime("%Y-%m-%d %H:%M:%S")

    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    # device_settings가 있으면 사용, 없으면 NULL => 기본값으로 처리
    cur.execute("""
        SELECT
          d.id AS device_id,
          d.agent_id,
          d.alias,
          d.last_power_w,
          d.last_seen,
          s.standby_threshold_w,
          s.standby_exempt,
          s.standby_hours_per_day
        FROM devices d
        LEFT JOIN device_settings s ON s.device_id = d.id
        WHERE d.agent_id = %s
          AND d.last_seen >= %s
          AND d.last_power_w > 0
    """, (agent_id, fresh_after))
    rows = cur.fetchall()

    base_kwh = month_kwh_total(agent_id)
    base_bill = calc_bill_from_kwh(base_kwh)

    items = []
    total_savings_krw = 0
    total_savings_kwh = 0.0

    for r in rows:
        if (r.get("standby_exempt") or 0) == 1:
            continue
        thr = float(r.get("standby_threshold_w") or threshold_w)
        standby_w = float(r["last_power_w"] or 0.0)
        if standby_w <= 0 or standby_w > thr:
            continue

        hours_per_day = float(r.get("standby_hours_per_day") or assume_hours_per_day)
        delta_kwh = (standby_w/1000.0) * hours_per_day * 30.0

        new_bill = calc_bill_from_kwh(max(base_kwh - delta_kwh, 0.0))
        saving = base_bill - new_bill

        items.append({
            "device_id": r["device_id"],
            "alias": r["alias"],
            "standby_w": round(standby_w, 2),
            "threshold_w": thr,
            "assumed_hours_per_day": hours_per_day,
            "delta_kwh_month": round(delta_kwh, 3),
            "saving_krw_month": int(saving),
        })
        total_savings_krw += saving
        total_savings_kwh += delta_kwh

    cur.close(); conn.close()

    return {
        "agent_id": agent_id,
        "as_of": now.isoformat() + "Z",
        "fresh_within_sec": fresh_sec,
        "default_threshold_w": threshold_w,
        "default_hours_per_day": assume_hours_per_day,
        "base_month_kwh": round(base_kwh, 3),
        "estimated_total_saving_kwh": round(total_savings_kwh, 3),
        "estimated_total_saving_krw": int(total_savings_krw),
        "items": items
    }

# ===== 13) 서버 실행 =====
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
