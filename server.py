from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import uuid
import datetime
import json
import mysql.connector
from decimal import Decimal, ROUND_HALF_UP

# ===== 1) DB 설정 =====
DB_CONFIG = {
    "host": "mainline.proxy.rlwy.net",
    "port": 31299,
    "user": "root",
    "password": "wZxTvdwprKhKAkkyKzbeQJbqQxHxeXCf",
    "database": "railway",
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

class CommandIn(BaseModel):
    agent_id: str
    target_alias: str
    action: str  # "on" or "off" or "toggle"

# 프론트엔드 제어 요청용
class DeviceControlIn(BaseModel):
    status: str  # "on" / "off" / "toggle"

# 알림 읽음 처리용
class NotificationReadIn(BaseModel):
    read: bool

# 임의 알림 생성용(선택): 외부/관리자 호출
class NotifyIn(BaseModel):
    agent_id: str
    level: str           # info / warning / critical
    title: str
    message: str
    device_alias: str | None = None
    category: str | None = None
    meta: dict | None = None

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

# ===== 6) 공용: 알림 생성/디바운스 도우미 =====
def create_notification(cur, agent_id: str, level: str, title: str, message: str,
                        device_alias: str | None = None, meta: dict | None = None,
                        category: str | None = None):
    """
    notifications 테이블에 INSERT.
    테이블에 device_alias/category 컬럼이 없으면 자동으로 제외하고 넣습니다.
    """
    extra_cols = []
    extra_vals = []
    params = [agent_id, level, title, message, json.dumps(meta) if meta else None]

    if device_alias is not None:
        extra_cols.append("device_alias")
        extra_vals.append(device_alias)
    if category is not None:
        extra_cols.append("category")
        extra_vals.append(category)

    sql = f"""
        INSERT INTO notifications (agent_id, level, title, message, meta, is_read, created_at
            {',' + ','.join(extra_cols) if extra_cols else ''}
        )
        VALUES (%s, %s, %s, %s, %s, 0, NOW()
            {',' + ','.join(['%s'] * len(extra_vals)) if extra_vals else ''}
        )
    """
    cur.execute(sql, tuple(params + extra_vals))

def should_emit(cur, agent_id: str, category: str, device_alias: str | None, gap_minutes: int = 30) -> bool:
    """
    최근 gap_minutes 내 같은 카테고리(+같은 기기)의 알림이 있었는지 확인.
    notifications 테이블에 category/device_alias 컬럼이 없으면 True 반환(우회).
    """
    try:
        if device_alias:
            cur.execute("""
                SELECT id FROM notifications
                WHERE agent_id=%s AND category=%s AND device_alias=%s
                  AND created_at >= NOW() - INTERVAL %s MINUTE
                ORDER BY id DESC LIMIT 1
            """, (agent_id, category, device_alias, gap_minutes))
        else:
            cur.execute("""
                SELECT id FROM notifications
                WHERE agent_id=%s AND category=%s
                  AND created_at >= NOW() - INTERVAL %s MINUTE
                ORDER BY id DESC LIMIT 1
            """, (agent_id, category, gap_minutes))
        return cur.fetchone() is None
    except mysql.connector.Error:
        # 컬럼이 없을 때는 중복 방지 기능을 끄고 항상 알림 허용
        return True

# ===== 7) 전력 수집 API + 알림 생성 =====
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

    # ---- 알림 생성 로직 ----
    STANDBY_W = 5.0     # 대기전력 기준(W)
    STANDBY_MIN = 5     # 최근 5분 평균
    HIGH_W = 1200.0     # 순간 고전력 경고 기준(W)
    GAP_MIN = 30        # 동일 알림 디바운스(분)

    # (A) 최근 5분 평균이 대기전력보다 작으면 알림
    cur_avg = conn.cursor()
    cur_avg.execute("""
        SELECT AVG(power_w), COUNT(*)
        FROM power_logs
        WHERE agent_id=%s AND device_id=%s
          AND ts >= NOW() - INTERVAL %s MINUTE
    """, (data.agent_id, data.device_logical_id, STANDBY_MIN))
    avg_w, cnt = cur_avg.fetchone()
    cur_avg.close()

    if avg_w is not None and cnt and cnt >= 3 and avg_w < STANDBY_W:
        if should_emit(cur, data.agent_id, category="standby",
                       device_alias=data.device_alias, gap_minutes=GAP_MIN):
            create_notification(
                cur,
                agent_id=data.agent_id,
                level="warning",
                title="대기전력 감지",
                message=f"{data.device_alias} 최근 {STANDBY_MIN}분 평균 {avg_w:.1f}W",
                device_alias=data.device_alias,
                meta={"device_id": data.device_logical_id, "avg_w": avg_w, "window_min": STANDBY_MIN},
                category="standby",
            )

    # (B) 순간 고전력
    if float(data.power_w) >= HIGH_W:
        if should_emit(cur, data.agent_id, category="overuse",
                       device_alias=data.device_alias, gap_minutes=GAP_MIN):
            create_notification(
                cur,
                agent_id=data.agent_id,
                level="critical",
                title="전력 과소비 경고",
                message=f"{data.device_alias} 순간 {float(data.power_w):.0f}W",
                device_alias=data.device_alias,
                meta={"device_id": data.device_logical_id, "power_w": float(data.power_w)},
                category="overuse",
            )

    conn.commit()
    cur.close()
    conn.close()
    return {"ok": True}

# ===== 8) 최근 로그 =====
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

# ===== 9) 원격 명령 생성 =====
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

# ===== 10) 프론트 호환: 기기 전원 제어(명령 생성 래퍼) =====
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
    cur2 = conn.cursor()
    cur2.execute(
        """
        INSERT INTO commands (id, agent_id, target_alias, action, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        (cmd_id, device["agent_id"], device["alias"], control.status, "pending", now),
    )
    conn.commit()
    cur2.close()
    cur.close()
    conn.close()
    return {"success": True, "message": f"Device {device['alias']} -> {control.status}"}

# ===== 11) 에이전트 명령 조회 =====
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

# ===== 12) 명령 ACK =====
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

# ===== 13) 알림 API =====
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
        query += " ORDER BY created_at DESC LIMIT 50"
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        return rows
    except mysql.connector.Error as err:
        print(f"Error fetching notifications: {err}")
        return []
    finally:
        cur.close()
        conn.close()

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

# (선택) 임의 알림 생성(관리자/테스트용)
@app.post("/notify")
def post_notify(n: NotifyIn):
    conn = get_conn()
    cur = conn.cursor()
    try:
        create_notification(
            cur,
            agent_id=n.agent_id,
            level=n.level,
            title=n.title,
            message=n.message,
            device_alias=n.device_alias,
            meta=n.meta,
            category=n.category,
        )
        conn.commit()
        return {"ok": True}
    finally:
        cur.close()
        conn.close()

# ===== 14) 기기 목록 =====
@app.get("/api/devices")
def get_devices_list(agent_id: str = Query(...)):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM devices WHERE agent_id = %s", (agent_id,))
    rows = cur.fetchall()
    for row in rows:
        if "status" not in row:
            row["status"] = "off"
        if "device_name" not in row:
            row["device_name"] = row["alias"]
    cur.close()
    conn.close()
    return rows

# ===== 15) 사용량 API =====
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

# ===== 16) 서버 실행 =====
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
