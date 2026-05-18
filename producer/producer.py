# import os
# import time
# import random
# import json
# import logging
# import signal
# import sys
# from faker import Faker
# from confluent_kafka import Producer

# fake = Faker()

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

# # Kafka configuration
# kafka_broker = os.getenv("KAFKA_BROKER")
# if not kafka_broker:
#     raise ValueError("KAFKA_BROKER environment variable is not set.")

# kafka_config = {
#     'bootstrap.servers': kafka_broker
# }
# producer = Producer(kafka_config)

# topic = 'clickstream'

# def generate_clickstream_event():
#     return {
#         "event_id": fake.uuid4(),
#         "user_id": fake.uuid4(),
#         "event_type": fake.random_element(elements=("page_view", "add_to_cart", "purchase", "logout")),
#         "url": fake.uri_path(),
#         "session_id": fake.uuid4(),
#         "device": fake.random_element(elements=("mobile", "desktop", "tablet")),
#         "geo_location": {
#             "lat": float(fake.latitude()),
#             "lon": float(fake.longitude())
#         },
#         "purchase_amount": float(random.uniform(0.0, 500.0)) if fake.boolean(chance_of_getting_true=30) else None
#     }

# def delivery_report(err, msg):
#     if err is not None:
#         logger.error(f"Message delivery failed: {err}")
#     else:
#         logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# def signal_handler(sig, frame):
#     logger.info("Data generation stopped.")
#     producer.flush()
#     sys.exit(0)

# signal.signal(signal.SIGINT, signal_handler)

# if __name__ == "__main__":
#     try:
#         while True:
#             event = generate_clickstream_event()
#             try:
#                 producer.produce(topic, key=event["session_id"], value=json.dumps(event), callback=delivery_report)
#             except BufferError as e:
#                 logger.error(f"Buffer error: {e}")
#             except Exception as e:
#                 logger.error(f"Unexpected error: {e}")
#             logger.info(json.dumps(event, indent=2))
#             time.sleep(1)
#             producer.poll(1)
#     except KeyboardInterrupt:
#         logger.info("Data generation stopped.")
#     finally:
#         producer.flush()

import os
import time
import random
import json
import logging
import signal
import sys
import datetime
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
kafka_broker = os.getenv("KAFKA_BROKER")
if not kafka_broker:
    raise ValueError("KAFKA_BROKER environment variable is not set.")

kafka_config = {
    'bootstrap.servers': kafka_broker
}
producer = Producer(kafka_config)

topic = 'clickstream'

# Giả lập bộ nhớ tạm để theo dõi hành vi người dùng (cho kịch bản Velocity)
user_actions_count = {}
user_last_action_time = {}

def get_current_utc_timestamp():
    """Trả về timestamp hiện tại theo chuẩn ISO 8601."""
    return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

def is_night_time():
    """Kiểm tra xem hiện tại có phải ban đêm không (0h-5h sáng)."""
    now = datetime.datetime.now()
    return 0 <= now.hour < 5

def generate_clickstream_event():
    """
    Tạo sự kiện clickstream với khả năng mô phỏng hành vi gian lận.
    """
    user_id = fake.uuid4()
    event_type = fake.random_element(elements=("page_view", "add_to_cart", "purchase", "logout"))
    device = fake.random_element(elements=("mobile", "desktop", "tablet"))
    url = fake.uri_path()
    geo_lat = float(fake.latitude())
    geo_lon = float(fake.longitude())
    purchase_amount = None
    is_fraud = False
    fraud_reason = ""

    # --- Logic mô phỏng các kịch bản GIAN LẬN (5-10% tổng sự kiện) ---
    if random.random() < 0.08:  # 8% cơ hội là sự kiện gian lận
        scenario = random.choice(["velocity", "night_purchase", "admin_url"])

        if scenario == "velocity":
            # Kịch bản 1: Tốc độ cao - một user thực hiện >10 hành động trong 10 giây
            if user_id in user_actions_count:
                user_actions_count[user_id] += 1
                time_diff = (datetime.datetime.now() - user_last_action_time[user_id]).total_seconds()
                if user_actions_count[user_id] > 10 and time_diff < 10:
                    is_fraud = True
                    fraud_reason = f"High velocity: {user_actions_count[user_id]} actions in {time_diff:.2f}s"
            else:
                user_actions_count[user_id] = 1
            user_last_action_time[user_id] = datetime.datetime.now()

        elif scenario == "night_purchase":
            # Kịch bản 2: Mua hàng giá trị cao vào ban đêm trên thiết bị di động
            event_type = "purchase"
            device = "mobile"
            purchase_amount = round(random.uniform(800.0, 1500.0), 2) # Giá trị cao
            if is_night_time():
                is_fraud = True
                fraud_reason = "Night purchase on mobile"

        elif scenario == "admin_url":
            # Kịch bản 3: Truy cập URL quản trị
            url = random.choice(["/admin/login", "/wp-admin", "/config.php", "/.env", "/api/admin/users"])
            is_fraud = True
            fraud_reason = f"Suspicious URL: {url}"

    # --- Logic mô phỏng hành vi BÌNH THƯỜNG ---
    if not is_fraud:
        # Reset bộ đếm nếu user quay về trạng thái bình thường
        if user_id in user_actions_count:
            del user_actions_count[user_id]
            del user_last_action_time[user_id]

        if event_type == "purchase":
            purchase_amount = round(random.uniform(5.0, 200.0), 2) # Giá trị thấp hơn

        # Xác suất nhỏ để một event bình thường bị gán nhầm là không gian lận (thực tế là có) - noise
        # Nhưng ở đây ta giữ sạch để làm mẫu dương.

    # Tạo cấu trúc sự kiện hoàn chỉnh
    event = {
        "event_id": fake.uuid4(),
        "user_id": user_id,
        "event_type": event_type,
        "url": url,
        "session_id": fake.uuid4(),
        "device": device,
        "timestamp": get_current_utc_timestamp(),  # Đã thêm timestamp
        "geo_location": {
            "lat": geo_lat,
            "lon": geo_lon
        },
        "purchase_amount": purchase_amount,
        "is_fraud": is_fraud,        # Nhãn cho ML
        "fraud_reason": fraud_reason  # Giải thích (tùy chọn, để debug)
    }

    # Loại bỏ trường fraud_reason nếu không có gian lận để dữ liệu gọn hơn
    if not is_fraud:
        event.pop("fraud_reason")

    return event

def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def signal_handler(sig, frame):
    logger.info("Data generation stopped.")
    producer.flush()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    logger.info("Starting enhanced clickstream producer with fraud labels...")
    try:
        while True:
            event = generate_clickstream_event()
            try:
                # Sử dụng user_id làm key để đảm bảo các sự kiện của cùng user về cùng partition
                producer.produce(topic, key=event["user_id"], value=json.dumps(event), callback=delivery_report)
            except BufferError as e:
                logger.error(f"Buffer error: {e}")
            except Exception as e:
                logger.error(f"Unexpected error: {e}")
            
            if event.get("is_fraud"):
                logger.warning(f"FRAUD EVENT: {json.dumps(event, indent=2)}")
            else:
                logger.info(f"NORMAL EVENT: {event['event_type']} by {event['user_id']}")
            
            time.sleep(5)  # Giảm delay để tạo dữ liệu nhanh hơn
            producer.poll(1)
    except KeyboardInterrupt:
        logger.info("Data generation stopped.")
    finally:
        producer.flush()