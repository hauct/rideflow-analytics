"""
simulator.py — RideFlow Incremental Trip Simulator
---------------------------------------------------
Mỗi lần chạy = 1 window 15 phút. Giữ state giữa các lần chạy.
Driver/rider pool được tái sử dụng → data mới overlap với data cũ (realistic).

Usage:
    python data/generators/simulator.py --init            # lần đầu: tạo pool
    python data/generators/simulator.py --backfill        # fill 1 ngày (96 windows)
    python data/generators/simulator.py --backfill --windows 192   # fill 2 ngày
    python data/generators/simulator.py                   # 1 window (Airflow gọi)

Output structure (Hive-style partitions):
    data/raw/trips/date=2026-03-21/hour=14/batch_1430.jsonl
    data/raw/payments/date=2026-03-21/hour=14/batch_1430.jsonl
    data/raw/ratings/date=2026-03-21/hour=14/batch_1430.jsonl
    data/simulator_state.json
"""

import argparse
import json
import math
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
from faker import Faker

fake = Faker("vi_VN")

# ─── Constants ────────────────────────────────────────────────────────────────
STATE_FILE   = "data/simulator_state.json"
RAW_ROOT     = "data/raw"
WINDOW_MIN   = 15
LOG_VERSION  = "1.0"

# Trips trung bình mỗi 15 phút theo giờ trong ngày (index = giờ 0-23)
TRIPS_PER_WINDOW = [
     4,  3,  2,  2,  2,  5,    # 0-5h   đêm khuya
    18, 38, 42, 28, 20, 22,    # 6-11h  sáng cao điểm
    25, 22, 20, 20, 22, 40,    # 12-17h trưa
    48, 44, 35, 26, 18, 10,    # 18-23h tối cao điểm
]

# ─── Zones ────────────────────────────────────────────────────────────────────
HCMC_ZONES = [
    ("Quận 1",        10.7769, 106.7009, 1.2, 0.20),
    ("Quận 3",        10.7831, 106.6887, 1.0, 0.10),
    ("Bình Thạnh",    10.8121, 106.7094, 1.8, 0.10),
    ("Gò Vấp",        10.8384, 106.6652, 2.0, 0.08),
    ("Tân Bình",      10.8015, 106.6519, 2.0, 0.10),
    ("Phú Nhuận",     10.7992, 106.6793, 1.0, 0.07),
    ("Quận 7",        10.7344, 106.7214, 2.5, 0.10),
    ("Thủ Đức",       10.8527, 106.7718, 3.5, 0.12),
    ("Sân bay TSN",   10.8184, 106.6520, 0.8, 0.08),
    ("Bình Dương",    10.9804, 106.6519, 4.0, 0.05),
]
HANOI_ZONES = [
    ("Hoàn Kiếm",       21.0285, 105.8542, 1.0, 0.18),
    ("Đống Đa",         21.0245, 105.8412, 1.5, 0.12),
    ("Hai Bà Trưng",    21.0063, 105.8631, 1.8, 0.10),
    ("Cầu Giấy",        21.0331, 105.7938, 2.0, 0.12),
    ("Thanh Xuân",      20.9950, 105.8131, 2.0, 0.10),
    ("Hoàng Mai",       20.9781, 105.8632, 2.5, 0.10),
    ("Long Biên",       21.0468, 105.8875, 2.5, 0.08),
    ("Tây Hồ",          21.0683, 105.8261, 2.0, 0.08),
    ("Sân bay Nội Bài", 21.2187, 105.8047, 1.0, 0.07),
    ("Hà Đông",         20.9711, 105.7756, 3.0, 0.05),
]

VEHICLE_TYPES    = ["bike", "car_4", "car_7", "electric"]
VEHICLE_W        = [0.45, 0.35, 0.12, 0.08]
PAYMENT_METHODS  = ["cash", "momo", "zalopay", "credit_card", "vnpay"]
PAYMENT_W        = [0.35, 0.28, 0.18, 0.12, 0.07]
DRIVER_TIERS     = ["bronze", "silver", "gold", "platinum"]
DRIVER_TIER_W    = [0.40, 0.35, 0.18, 0.07]
RIDER_SEGMENTS   = ["casual", "regular", "vip"]
RIDER_SEGMENT_W  = [0.50, 0.35, 0.15]
POSITIVE_TAGS    = ["Lái xe an toàn", "Đúng giờ", "Thân thiện",
                    "Xe sạch sẽ", "Biết đường", "Lịch sự", "Đi nhanh"]
NEGATIVE_TAGS    = ["Lái nhanh", "Đến muộn", "Thái độ không tốt",
                    "Xe bẩn", "Đi sai đường"]
PROMO_CODES      = ["RIDE10", "GRAB20", "NEWUSER30", None, None, None, None]
PROMO_DISC       = {"RIDE10": 0.10, "GRAB20": 0.20, "NEWUSER30": 0.30}
SURGE_HOURS      = {7, 8, 17, 18, 19}

# ─── Geo helpers ──────────────────────────────────────────────────────────────

def rand_point(lat: float, lng: float, radius_km: float):
    r = radius_km / 111.0
    while True:
        dlat = random.uniform(-r, r)
        dlng = random.uniform(-r, r)
        if dlat**2 + dlng**2 <= r**2:
            return round(lat + dlat, 6), round(lng + dlng, 6)


def haversine(lat1, lng1, lat2, lng2) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a  = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

# ─── Utils ────────────────────────────────────────────────────────────────────

def pick(pool, weights):
    return random.choices(pool, weights=weights, k=1)[0]


def write_jsonl(records: list, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str) + "\n")


def partition_path(entity: str, dt: datetime) -> Path:
    """
    Hive-style partition path:
    data/raw/trips/date=2026-03-21/data.jsonl
    """
    return (
        Path(RAW_ROOT) / entity
        / f"date={dt.strftime('%Y-%m-%d')}"
        / "data.jsonl"
    )

# ─── State management ─────────────────────────────────────────────────────────

def _online_prob(hour: int) -> float:
    """Xác suất driver online theo giờ — mô phỏng shift sáng/chiều/tối."""
    if 6 <= hour <= 22:  return 0.55
    if hour in (5, 23):  return 0.25
    return 0.10


def init_state(n_drivers: int = 500, n_riders: int = 2000) -> dict:
    """
    Tạo pool driver và rider một lần duy nhất.
    Lưu vào STATE_FILE để các lần chạy sau tái dùng.
    """
    print(f"  Init: {n_drivers} drivers, {n_riders} riders...")
    drivers, riders = [], []

    for i in range(1, n_drivers + 1):
        city = pick(["HCMC", "HANOI"], [0.65, 0.35])
        drivers.append({
            "driver_id":    f"DRV{str(i).zfill(5)}",
            "city":         city,
            "vehicle_type": pick(VEHICLE_TYPES, VEHICLE_W),
            "tier":         pick(DRIVER_TIERS, DRIVER_TIER_W),
            "avg_rating":   round(random.uniform(3.5, 5.0), 2),
            "is_active":    random.random() < 0.90,
            # 1 = online giờ đó, 0 = offline — mỗi driver có pattern riêng
            "online_hours": [1 if random.random() < _online_prob(h) else 0
                             for h in range(24)],
        })

    for i in range(1, n_riders + 1):
        city = pick(["HCMC", "HANOI"], [0.65, 0.35])
        riders.append({
            "rider_id":          f"RDR{str(i).zfill(6)}",
            "city":              city,
            "segment":           pick(RIDER_SEGMENTS, RIDER_SEGMENT_W),
            "preferred_payment": pick(PAYMENT_METHODS, PAYMENT_W),
            "is_active":         random.random() < 0.80,
        })

    return {
        "version":        LOG_VERSION,
        "created_at":     datetime.now().isoformat(),
        "last_run_at":    None,
        "current_sim_date": datetime.now().strftime("%Y-%m-%d"),
        "trip_counter":   0,
        "pay_counter":    0,
        "rating_counter": 0,
        "total_trips":    0,
        "drivers":        drivers,
        "riders":         riders,
    }


def load_state() -> dict:
    p = Path(STATE_FILE)
    if not p.exists():
        raise FileNotFoundError(
            f"Không tìm thấy state: {STATE_FILE}\n"
            f"Chạy lần đầu: python simulator.py --init"
        )
    with open(p) as f:
        return json.load(f)


def save_state(state: dict):
    p = Path(STATE_FILE)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2, default=str)

# ─── Window generator ─────────────────────────────────────────────────────────

def generate_window(state: dict, window_start: datetime) -> tuple:
    """Sinh trips/payments/ratings cho 1 window 15 phút."""
    hour       = window_start.hour
    city_zones = {"HCMC": HCMC_ZONES, "HANOI": HANOI_ZONES}

    # Số trips có noise ±20%
    n_trips = max(1, int(TRIPS_PER_WINDOW[hour] * random.uniform(0.80, 1.20)))
    trips, payments, ratings = [], [], []

    for _ in range(n_trips):
        city  = pick(["HCMC", "HANOI"], [0.65, 0.35])
        zones = city_zones[city]

        # ── Rider: 97% pool cũ, 3% rider mới ────────────────────────────────
        city_riders = [r for r in state["riders"]
                       if r["city"] == city and r["is_active"]]
        if not city_riders or random.random() < 0.03:
            new_rider = {
                "rider_id":          f"RDR{str(len(state['riders']) + 1).zfill(6)}",
                "city":              city,
                "segment":           pick(RIDER_SEGMENTS, RIDER_SEGMENT_W),
                "preferred_payment": pick(PAYMENT_METHODS, PAYMENT_W),
                "is_active":         True,
            }
            state["riders"].append(new_rider)
            rider = new_rider
        else:
            rider = random.choice(city_riders)

        # ── Driver: online drivers trong city + giờ hiện tại ─────────────────
        active_drivers = [d for d in state["drivers"]
                          if d["city"] == city
                          and d["is_active"]
                          and d["online_hours"][hour] == 1]

        # Supply/demand ratio ảnh hưởng no_driver rate
        ratio        = len(active_drivers) / max(len(city_riders), 1) * 10
        no_drv_prob  = max(0.03, min(0.20, 0.10 / max(ratio, 0.1)))
        status       = pick(
            ["completed", "cancelled", "no_driver"],
            [0.78 - no_drv_prob / 2, 0.22 - no_drv_prob / 2, no_drv_prob],
        )

        if status == "no_driver" or not active_drivers:
            driver, driver_id, status = None, None, "no_driver"
        else:
            driver    = random.choice(active_drivers)
            driver_id = driver["driver_id"]

        # ── Location ─────────────────────────────────────────────────────────
        zw             = [z[4] for z in zones]
        pickup_zone    = random.choices(zones, weights=zw)[0]
        pu_lat, pu_lng = rand_point(pickup_zone[1], pickup_zone[2], pickup_zone[3])

        dropoff_zone   = pickup_zone if random.random() < 0.65 \
                         else random.choices(zones, weights=zw)[0]
        do_lat, do_lng = rand_point(dropoff_zone[1], dropoff_zone[2], dropoff_zone[3])

        # ── Distance / duration ───────────────────────────────────────────────
        dist_km  = round(haversine(pu_lat, pu_lng, do_lat, do_lng)
                        * random.uniform(1.3, 1.5), 2)
        dist_km  = max(0.5, dist_km)
        dur_min  = max(3, round(dist_km / random.uniform(15, 28) * 60))

        # ── Timing ───────────────────────────────────────────────────────────
        offset       = random.randint(0, WINDOW_MIN * 60 - 1)
        request_time = window_start + timedelta(seconds=offset)
        wait_sec     = random.randint(90, 480)

        if status == "completed":
            pickup_time  = request_time + timedelta(seconds=wait_sec)
            dropoff_time = pickup_time  + timedelta(minutes=dur_min)
        elif status == "cancelled":
            pickup_time  = request_time + timedelta(seconds=random.randint(30, 180))
            dropoff_time, dur_min = None, None
        else:
            pickup_time = dropoff_time = None
            dur_min     = None

        # ── Fare ─────────────────────────────────────────────────────────────
        if status == "completed":
            surge    = 1.3 if hour in SURGE_HOURS else 1.0
            fare_vnd = max(15_000, int(
                round((12_000 + dist_km * 5_500) * surge
                      * random.uniform(0.9, 1.1) / 1000) * 1000
            ))
            # INJECT ERROR: Negative Fare (2%)
            if random.random() < 0.02:
                fare_vnd = -fare_vnd
        else:
            fare_vnd = 0

        # ── Trip record ───────────────────────────────────────────────────────
        state["trip_counter"] += 1
        trip_id = f"TRP{str(state['trip_counter']).zfill(8)}"
        batch_id = window_start.strftime("%Y%m%d_%H%M")

        rider_id_val = rider["rider_id"]
        # INJECT ERROR: Null rider_id (1%)
        if random.random() < 0.01:
            rider_id_val = None

        trips.append({
            "_meta": {
                "schema_version": LOG_VERSION,
                "ingested_at":    datetime.now().isoformat(),
                "window_start":   window_start.isoformat(),
                "batch_id":       batch_id,
            },
            "trip_id":      trip_id,
            "driver_id":    driver_id,
            "rider_id":     rider_id_val,
            "request_time": request_time.isoformat(),
            "pickup_time":  pickup_time.isoformat() if pickup_time else None,
            "dropoff_time": dropoff_time.isoformat() if dropoff_time else None,
            "status":       status,
            "city":         city,
            "pickup_zone":  pickup_zone[0],
            "pickup_lat":   pu_lat,
            "pickup_lng":   pu_lng,
            "dropoff_zone": dropoff_zone[0],
            "dropoff_lat":  do_lat,
            "dropoff_lng":  do_lng,
            "distance_km":  dist_km,
            "duration_min": dur_min,
            "fare_vnd":     fare_vnd,
        })

        # ── Payment ───────────────────────────────────────────────────────────
        if status == "completed":
            promo        = random.choice(PROMO_CODES)
            disc         = PROMO_DISC.get(promo, 0)
            disc_vnd     = int(fare_vnd * disc)
            final        = max(10_000, fare_vnd - disc_vnd)
            method       = rider.get("preferred_payment", pick(PAYMENT_METHODS, PAYMENT_W))
            pay_status   = "success" if method == "cash" else \
                           pick(["success", "refunded", "failed"], [0.965, 0.025, 0.010])
            if pay_status != "success":
                final = 0

            # INJECT ERROR: Unknown Payment Method (2%), Negative Final Amount (2%)
            if random.random() < 0.02:
                method = "unknown_method"
            if random.random() < 0.02:
                final = -1 * abs(final) if final != 0 else -50000

            state["pay_counter"] += 1
            pay_time = dropoff_time + timedelta(seconds=random.randint(2, 30))
            payments.append({
                "_meta": {"schema_version": LOG_VERSION, "batch_id": batch_id},
                "payment_id":         f"PAY{str(state['pay_counter']).zfill(8)}",
                "trip_id":            trip_id,
                "rider_id":           rider["rider_id"],
                "payment_time":       pay_time.isoformat(),
                "payment_method":     method,
                "fare_vnd":           fare_vnd,
                "promo_code":         promo,
                "discount_vnd":       disc_vnd,
                "final_amount_vnd":   final,
                "payment_status":     pay_status,
                "platform_fee_vnd":   int(final * 0.20),
                "driver_earning_vnd": int(final * 0.80),
            })

        # ── Ratings ───────────────────────────────────────────────────────────
        if status == "completed":
            for rater_type, rater_id, ratee_id, star_w, rate_prob in [
                ("rider",  rider["rider_id"], driver_id,
                 [0.02, 0.03, 0.07, 0.18, 0.70], 0.75),
                ("driver", driver_id, rider["rider_id"],
                 [0.01, 0.01, 0.05, 0.18, 0.75], 0.60),
            ]:
                if ratee_id and random.random() < rate_prob:
                    stars = random.choices([1, 2, 3, 4, 5], weights=star_w)[0]
                    
                    # INJECT ERROR: Stars out of bounds (2%)
                    if random.random() < 0.02:
                        stars = random.choice([0, 6, 9])

                    tags  = "|".join(random.sample(
                        POSITIVE_TAGS if stars >= 4 else NEGATIVE_TAGS,
                        k=random.randint(1, 2)
                    )) if rater_type == "rider" else None

                    state["rating_counter"] += 1
                    rated_at = dropoff_time + timedelta(
                        minutes=random.randint(1, 30 if rater_type == "rider" else 60)
                    )
                    ratings.append({
                        "_meta": {"batch_id": batch_id},
                        "rating_id":  f"RTG{str(state['rating_counter']).zfill(8)}",
                        "trip_id":    trip_id,
                        "rater_id":   rater_id,
                        "ratee_id":   ratee_id,
                        "rater_type": rater_type,
                        "ratee_type": "driver" if rater_type == "rider" else "rider",
                        "stars":      stars,
                        "tags":       tags,
                        "rated_at":   rated_at.isoformat(),
                    })

    state["total_trips"] += len(trips)
    return trips, payments, ratings

# ─── Run modes ────────────────────────────────────────────────────────────────

def run_daily_batch(target_date: str = None):
    """Airflow gọi cái này. Sinh 1 ngày data (96 windows) và update state tiến 1 ngày."""
    state = load_state()

    if target_date is None:
        target_date = state.get("current_sim_date")
        if not target_date:
            target_date = datetime.now().strftime("%Y-%m-%d")

    current_dt = datetime.strptime(target_date, "%Y-%m-%d")

    trips_all, payments_all, ratings_all = [], [], []
    for m in range(0, 24 * 60, 15):
        window_dt = current_dt + timedelta(minutes=m)
        t, p, r = generate_window(state, window_dt)
        trips_all.extend(t)
        payments_all.extend(p)
        ratings_all.extend(r)

    # Overwrite the jsonl data for the target_date
    write_jsonl(trips_all,    partition_path("trips",    current_dt))
    write_jsonl(payments_all, partition_path("payments", current_dt))
    write_jsonl(ratings_all,  partition_path("ratings",  current_dt))

    # Advance state for the next run
    next_date = (current_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    state["current_sim_date"] = next_date
    state["last_run_at"] = datetime.now().isoformat()
    save_state(state)

    print(f"  [Daily Batch: {target_date}] "
          f"{len(trips_all)} trips | {len(payments_all)} payments | "
          f"{len(ratings_all)} ratings | total: {state['total_trips']:,}")
    return {"trips": len(trips_all), "payments": len(payments_all), "ratings": len(ratings_all), "target_date": target_date}


def run_backfill(n_days: int = 1, start_date: str = None):
    """Backfill nhiều ngày — dùng lần đầu để có historical data."""
    state = load_state()

    if start_date is None:
        start_date = state.get("current_sim_date")
        if not start_date:
            start_date = datetime.now().strftime("%Y-%m-%d")

    print(f"  Backfill {n_days} days starting from {start_date}")
    print(f"  {'─' * 50}")

    current_date = start_date
    for _ in range(n_days):
        result = run_daily_batch(target_date=current_date)
        current_dt = datetime.strptime(current_date, "%Y-%m-%d")
        current_date = (current_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n  Done backfill {n_days} days!")
    print(f"  Files: {RAW_ROOT}/trips/, payments/, ratings/\n")


def main():
    parser = argparse.ArgumentParser(description="RideFlow Trip Simulator")
    parser.add_argument("--init",     action="store_true", help="Khởi tạo state (chỉ chạy 1 lần)")
    parser.add_argument("--backfill", action="store_true", help="Backfill historical data")
    parser.add_argument("--days",     type=int, default=1, help="Số ngày khi backfill (default: 1 ngày)")
    parser.add_argument("--drivers",  type=int, default=500)
    parser.add_argument("--riders",   type=int, default=2000)
    parser.add_argument("--seed",     type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)
    np.random.seed(args.seed)

    print(f"\nRideFlow Simulator  v{LOG_VERSION}")
    print(f"{'─' * 40}")

    if args.init:
        if Path(STATE_FILE).exists():
            print(f"  State đã tồn tại ({STATE_FILE}) — xóa nếu muốn reset.")
            return
        state = init_state(n_drivers=args.drivers, n_riders=args.riders)
        save_state(state)
        print(f"  Saved → {STATE_FILE}")
        print(f"  Bước tiếp: python simulator.py --backfill --days 7\n")
        return

    if args.backfill:
        run_backfill(n_days=args.days)
        return

    # Default: chạy 1 batch hàng ngày — Airflow gọi
    run_daily_batch()


if __name__ == "__main__":
    main()
