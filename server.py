from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import uuid
import datetime
import mysql.connector
from decimal import Decimal, ROUND_HALF_UP

# ===== 1) DB 설정 (기존 유지) =====
DB_CONFIG = {
    "host": "mainline.proxy.rlwy.net",
    "port": 31299,
    "user": "root",
    "password": "wZxTvdwprKhKAkkyKzbeQJbqQxHxeXCf",
    "database": "railway",
}

# ===== 2) FastAPI 기본 (기존 유지) =====
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== 3) 데이터 모델 (수정 및 추가) =====
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

# 🚨 [추가] 프론트엔드 제어 요청용 모델
class DeviceControlIn(BaseModel):
    status: str # "on" or "off"

# 🚨 [추가] 알림 읽음 처리용 모델
class NotificationReadIn(BaseModel):
    read: bool

# ===== 4) 요금 계산 함수 (기존 유지) =====
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

# ===== 5) DB 커넥션 (기존 유지) =====
def get_conn():
    return mysql.connector.connect(**DB_CONFIG)

# ===== 6) /power : 데이터 수집 (기존 유지) =====
@app.post("/power") # 원래 /api/power 였으나 팀장님 코드 유지
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
    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True}

# ===== 7) 최근 로그 (기존 유지) =====
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

# ===== 8) 원격 명령 (기존 유지 + 프론트엔드용 래퍼 추가 예정) =====
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

# ===== 🚨 [추가] 프론트엔드 호환용 기기 제어 API (PUT) =====
# 프론트엔드에서 PUT /api/devices/{id}/power 요청을 보내면, 내부적으로 POST /command 로직을 수행합니다.
@app.put("/api/devices/{device_id}/power")
def control_device_power(device_id: int, control: DeviceControlIn):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    # 1. device_id로 기기 정보(agent_id, alias) 조회
    cur.execute("SELECT agent_id, alias FROM devices WHERE id = %s", (device_id,))
    device = cur.fetchone()
    
    if not device:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Device not found")
    
    # 2. POST /command 로직 수행 (명령 생성)
    cmd_id = str(uuid.uuid4())
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    
    cur.execute(
        """
        INSERT INTO commands (id, agent_id, target_alias, action, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        (cmd_id, device['agent_id'], device['alias'], control.status, "pending", now),
    )
    
    # 3. 기기 상태 업데이트 (낙관적 업데이트 지원)
    # 실제로는 플러그가 응답해야 하지만, UI 반응성을 위해 DB 상태도 업데이트해줌
    # (devices 테이블에 status 컬럼이 있다고 가정하거나, 없으면 생략 가능)
    # cur.execute("UPDATE devices SET status = %s WHERE id = %s", (control.status, device_id))

    conn.commit()
    cur.close()
    conn.close()
    
    return {"success": True, "message": f"Device {device['alias']} turned {control.status}"}

# ===== 9) 에이전트 명령 조회 (기존 유지) =====
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

# ===== 10) 명령 ACK (기존 유지) =====
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

# ===== 🚨 [추가] 알림 목록 조회 API (GET /api/notifications) =====
# DB에 'notifications' 테이블이 없으면 오류가 날 수 있으므로, 
# 테이블이 없으면 빈 리스트를 반환하거나 가짜 데이터를 반환하도록 처리해야 함.
# 여기서는 notifications 테이블이 있다고 가정하고 작성합니다.
@app.get("/api/notifications")
def get_notifications(agent_id: str = Query(None)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    try:
        query = "SELECT * FROM notifications"
        params = []
        
        if agent_id:
            query += " WHERE agent_id = %s"
            params.append(agent_id)
            
        query += " ORDER BY created_at DESC LIMIT 20"
        
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        return rows
    except mysql.connector.Error as err:
        # 테이블이 없는 경우 등을 대비해 빈 리스트 반환 (서버 죽는 것 방지)
        print(f"Error fetching notifications: {err}")
        return [] 
    finally:
        cur.close()
        conn.close()

# ===== 🚨 [추가] 알림 읽음 처리 API (PUT /api/notifications/{id}/read) =====
@app.put("/api/notifications/{noti_id}/read")
def read_notification(noti_id: int):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE notifications SET is_read = 1 WHERE id = %s", (noti_id,))
        conn.commit()
        return {"success": True}
    except mysql.connector.Error as err:
        print(f"Error updating notification: {err}")
        return {"success": False, "error": str(err)}
    finally:
        cur.close()
        conn.close()

# ===== 🚨 [추가] 기기 목록 조회 API (GET /api/devices) =====
# 프론트엔드가 /api/devices를 호출하므로, /usage/today 로직을 재활용하여 구현
@app.get("/api/devices")
def get_devices_list(agent_id: str = Query(...)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    
    # devices 테이블에서 직접 조회 (가장 정확함)
    cur.execute("SELECT * FROM devices WHERE agent_id = %s", (agent_id,))
    rows = cur.fetchall()
    
    # status 필드가 없다면 임의로 추가 (프론트엔드 오류 방지)
    for row in rows:
        if 'status' not in row:
            row['status'] = 'off' # 기본값 off
            
        # 프론트엔드가 device_name을 원하면 alias를 복사해줌
        if 'device_name' not in row:
            row['device_name'] = row['alias']

    cur.close()
    conn.close()
    return rows

# ===== 11, 12, 13) 사용량 API (기존 유지) =====
@app.get("/usage/today")
def usage_today(agent_id: str = Query(...)):
    # ... (기존 코드 내용 유지) ...
    # (위의 팀장님 코드 11번 섹션 복사해서 넣으시면 됩니다. 여기서는 생략)
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

@app.get("/usage/daily")
def usage_daily(agent_id: str = Query(...), target_date: str | None = Query(None)):
    # ... (기존 코드 내용 유지 - 12번 섹션) ...
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

@app.get("/usage/monthly")
def usage_monthly(agent_id: str = Query(...)):
    # ... (기존 코드 내용 유지 - 13번 섹션) ...
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

# ===== 14) 서버 실행 (기존 유지) =====
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
