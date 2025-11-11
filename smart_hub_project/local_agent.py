import asyncio
import datetime
from decimal import Decimal, ROUND_HALF_UP

import requests
from tapo import ApiClient

# ===== 1) 설정 =====
TAPO_USERNAME = "starcrong@gmail.com"
TAPO_PASSWORD = "tjdqls@0418"

AGENT_ID = "house1"

# 네 서버 IP로 바꿔서 사용
REMOTE_BASE = "http://192.168.0.3:8000"

PLUG_MAPPING = {
    "세탁기": ("192.168.0.2", 1),
    "냉장고": ("192.168.0.4", 2),
    "컴퓨터": ("192.168.0.5", 3),
}

COLLECT_INTERVAL_SEC = 5
STANDBY_POWER_THRESHOLD = Decimal("5.0")
STANDBY_HOLD_SEC = 300  # 5분

# ===== 2) 요금 계산 (로컬 표시용) =====
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


def calculate_electricity_bill(monthly_kwh: Decimal) -> Decimal:
    now = datetime.datetime.now()
    rates = RATES_SUMMER if now.month in [7, 8] else RATES_OTHER
    total_bill = Decimal("0")
    remaining_kwh = Decimal(monthly_kwh)
    last_limit = Decimal("0")
    base_fee_applied = Decimal("0")

    for base_fee, limit, rate_per_kwh in rates:
        if remaining_kwh <= 0:
            break
        if limit == Decimal("Infinity"):
            kwh_in_this_tier = remaining_kwh
        else:
            kwh_in_this_tier = min(remaining_kwh, limit - last_limit)
        total_bill += kwh_in_this_tier * rate_per_kwh
        remaining_kwh -= kwh_in_this_tier
        base_fee_applied = base_fee
        if limit == Decimal("Infinity") or remaining_kwh <= 0:
            break
        last_limit = limit

    total_bill += base_fee_applied
    return total_bill.quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def estimate_monthly_kwh_from_snapshot(total_w: Decimal) -> Decimal:
    total_kw = total_w / Decimal("1000")
    return total_kw * Decimal("24") * Decimal("30")


# ===== 3) 플러그 클래스 =====
class TapoPlug:
    def __init__(self, alias: str, ip: str, logical_id: int):
        self.alias = alias
        self.ip = ip
        self.logical_id = logical_id
        self.plug = None
        self.is_connected = False
        self.current_power = Decimal("0")
        self._low_start = None

    async def connect(self):
        try:
            client = ApiClient(TAPO_USERNAME, TAPO_PASSWORD)
            self.plug = await client.p110(self.ip)
            await self.plug.get_device_info()
            self.is_connected = True
            print(f"🔌 [{self.alias}] 연결 성공 ({self.ip})")
        except Exception as e:
            print(f"❌ [{self.alias}] 연결 실패: {e}")
            self.is_connected = False

    async def get_power(self):
        if not self.is_connected:
            await self.connect()
            if not self.is_connected:
                return
        try:
            res = await self.plug.get_current_power()
            val = getattr(res, "current_power", 0)
            self.current_power = Decimal(str(val))
            print(f"⚡ [{self.alias}] {self.current_power} W")
        except Exception as e:
            print(f"❌ [{self.alias}] 전력 읽기 실패: {e}")
            self.current_power = Decimal("0")

    async def turn_on(self):
        if self.is_connected:
            await self.plug.turn_on()
            print(f"💡 [{self.alias}] ON")

    async def turn_off(self):
        if self.is_connected:
            await self.plug.turn_off()
            print(f"🚫 [{self.alias}] OFF")

    async def check_standby(self):
        if not self.is_connected:
            return
        now = datetime.datetime.now()
        if 0 < self.current_power < STANDBY_POWER_THRESHOLD:
            if self._low_start is None:
                self._low_start = now
            else:
                if (now - self._low_start).total_seconds() >= STANDBY_HOLD_SEC:
                    print(f"🚨 [{self.alias}] 대기전력 지속 → OFF")
                    await self.turn_off()
                    self._low_start = None
        else:
            self._low_start = None


# ===== 4) 서버 통신 =====
def send_power_to_server(agent_id: str, plug: TapoPlug):
    url = f"{REMOTE_BASE}/power"
    payload = {
        "agent_id": agent_id,
        "device_alias": plug.alias,
        "device_logical_id": plug.logical_id,
        "power_w": float(plug.current_power),
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            print(f"⚠️ 원격 전송 응답 오류: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"⚠️ 원격 전송 실패: {e}")


def fetch_commands_from_server(agent_id: str):
    url = f"{REMOTE_BASE}/commands?agent_id={agent_id}"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except Exception as e:
        print(f"⚠️ 명령 조회 실패: {e}")
    return []


def ack_command(command_id: str):
    try:
        requests.post(f"{REMOTE_BASE}/commands/{command_id}/ack", timeout=5)
    except Exception:
        pass


# ===== 5) 메인 루프 =====
async def main():
    plugs: list[TapoPlug] = []
    for alias, (ip, lid) in PLUG_MAPPING.items():
        plugs.append(TapoPlug(alias, ip, lid))

    for p in plugs:
        await p.connect()

    while True:
        print(f"\n--- 수집 사이클 {datetime.datetime.now()} ---")
        total_w = Decimal("0")

        for p in plugs:
            await p.get_power()
            total_w += p.current_power
            await p.check_standby()
            send_power_to_server(AGENT_ID, p)

        est_kwh = estimate_monthly_kwh_from_snapshot(total_w)
        est_bill = calculate_electricity_bill(est_kwh)
        print(f"💰 월 예상요금(대충): {est_bill:,}원 / 추정 사용량: {est_kwh:.1f} kWh")

        commands = fetch_commands_from_server(AGENT_ID)
        for cmd in commands:
            target_alias = cmd.get("target_alias")
            action = cmd.get("action")
            for p in plugs:
                if p.alias == target_alias:
                    if action == "on":
                        await p.turn_on()
                    elif action == "off":
                        await p.turn_off()
            if "id" in cmd:
                ack_command(cmd["id"])

        await asyncio.sleep(COLLECT_INTERVAL_SEC)


if __name__ == "__main__":
    asyncio.run(main())
