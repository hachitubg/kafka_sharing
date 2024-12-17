import logging
from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime
import socket
import threading

# Cấu hình logging mức độ INFO
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LogGenerator:
    def __init__(self, bootstrap_servers='localhost:29092'):
        # Khởi tạo đối tượng Producer để gửi dữ liệu đến Kafka.
        # 'bootstrap.servers': địa chỉ (host:port) của các broker Kafka.
        # 'client.id': định danh cho client này, giúp việc theo dõi, chẩn đoán dễ hơn.
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'log_generator'
        })

        # Danh sách tên service giả định
        self.service_names = ['user-service', 'order-service', 'payment-service', 'inventory-service']
        # Danh sách mức độ log
        self.log_levels = ['INFO', 'WARNING', 'ERROR', 'DEBUG']
        # Lấy hostname của máy chạy
        self.hostname = socket.gethostname()

    def delivery_report(self, err, msg):
        # Đây là callback sẽ được gọi tự động sau khi Producer gửi message đến Kafka broker.
        # Tham số `err` là lỗi (nếu có), `msg` là message đã được gửi.
        # Nếu `err` là None, nghĩa là message đã gửi thành công.
        if err is not None:
            logger.error(f'Message delivery failed: {err}')
        else:
            logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}]')

    def generate_log_message(self):
        # Sinh ra một log message giả lập ngẫu nhiên
        service = random.choice(self.service_names)
        level = random.choice(self.log_levels)
        
        messages = {
            'INFO': [
                f"Request processed successfully for {service}",
                f"Database connection established in {service}",
                f"Cache updated for {service}",
                f"API endpoint called: GET /api/{service}/status"
            ],
            'WARNING': [
                f"High memory usage detected in {service}",
                f"Slow query performance in {service}",
                f"Rate limit threshold approaching for {service}",
                f"Cache miss rate increasing in {service}"
            ],
            'ERROR': [
                f"Database connection failed in {service}",
                f"API request timeout in {service}",
                f"Internal server error in {service}",
                f"Service dependency unavailable: {service}"
            ],
            'DEBUG': [
                f"Processing request parameters in {service}",
                f"Executing database query in {service}",
                f"Cache lookup performed in {service}",
                f"Request headers received in {service}"
            ]
        }

        message = random.choice(messages[level])
        
        # Trả về dữ liệu log giả định dạng dictionary
        return {
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'level': level,
            'service': service,
            'host': self.hostname,
            'message': message,
            'thread': threading.current_thread().name,
            'additional_info': {
                'memory_usage': random.randint(100, 1000),
                'cpu_usage': random.randint(0, 100),
                'request_id': f'req_{random.randint(1000, 9999)}',
                'user_id': f'user_{random.randint(1, 100)}'
            }
        }

    def send_log(self, topic='system_logs'):
        try:
            # Tạo một log message dưới dạng JSON
            log_data = self.generate_log_message()
            
            # Sử dụng Producer để gửi log message này đến Kafka.
            # - `topic`: Tên topic trong Kafka nơi message sẽ được gửi.
            # - `value`: Nội dung message (dạng bytes). Ở đây, chúng ta dùng json.dumps để chuyển dict -> JSON, sau đó encode sang bytes.
            # - `callback=self.delivery_report`: Sau khi gửi xong, Kafka Producer sẽ gọi callback này để báo trạng thái gửi.
            self.producer.produce(
                topic,
                value=json.dumps(log_data).encode('utf-8'),
                callback=self.delivery_report
            )

            # `producer.poll(0)` để kích hoạt quá trình gửi và xử lý callback.
            # poll cho Producer cơ hội xử lý các sự kiện nội bộ (như gửi message, nhận ack,...).
            # poll(0) nghĩa là không chờ đợi, chỉ xử lý tức thì các event sẵn có.
            self.producer.poll(0)

            # Log ra thông tin message vừa gửi, chỉ để quan sát.
            logger.info(f"Sent log: {log_data['level']} - {log_data['message']}")
        except Exception as e:
            # Nếu có lỗi trong quá trình gửi message, log ra thông tin lỗi
            logger.error(f"Error sending message: {e}")

    def run(self, interval=1):
        # Hàm chạy vòng lặp gửi log đều đặn theo khoảng thời gian `interval` (giây)
        logger.info("Starting log generator...")
        try:
            while True:
                self.send_log()  # Gửi một log đến Kafka
                time.sleep(interval)  # Chờ một khoảng interval trước khi gửi log tiếp theo
        except KeyboardInterrupt:
            # Khi người dùng nhấn Ctrl+C, ta dừng quá trình gửi log.
            logger.info("Stopping log generator...")
            
            # `self.producer.flush()` yêu cầu Producer gửi hết các message còn trong buffer đến Kafka
            # và chờ cho đến khi việc này hoàn tất (hoặc thời gian chờ kết thúc).
            # Điều này đảm bảo không mất log còn trong queue nội bộ của Producer.
            self.producer.flush()

            logger.info("Log generator stopped")

if __name__ == "__main__":
    log_generator = LogGenerator()
    log_generator.run()
