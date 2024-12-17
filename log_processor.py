from confluent_kafka import Consumer, KafkaError
from elasticsearch import Elasticsearch
import json
import logging
from datetime import datetime

# Cấu hình logging ở mức độ INFO
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LogProcessor:
    def __init__(self, kafka_servers='localhost:29092', es_host='localhost'):
        # Khởi tạo Kafka Consumer để tiêu thụ message từ Kafka.
        # 'bootstrap.servers': địa chỉ Kafka broker.
        # 'group.id': tên group consumer, giúp quản lý offset và phân chia partition.
        # 'auto.offset.reset': hành vi đọc offset khi không tìm thấy offset đã commit (vd: earliest).
        self.consumer = Consumer({
            'bootstrap.servers': kafka_servers,
            'group.id': 'log_processor_group',
            'auto.offset.reset': 'earliest'
        })
        
        # Kết nối đến Elasticsearch.
        # Ở đây dùng kết nối đơn giản, giả định ES chạy trên localhost:9200.
        self.es = Elasticsearch(['http://localhost:9200'])
        
        # Kiểm tra kết nối ES. Nếu ping thành công mới thiết lập template.
        if self.es.ping():
            logger.info("Connected to Elasticsearch")
            self.setup_elasticsearch()
        else:
            raise Exception("Could not connect to Elasticsearch")

    def setup_elasticsearch(self):
        # Thiết lập template cho các chỉ mục trong Elasticsearch.
        # Template này quy định mapping (kiểu dữ liệu) cho các trường log.
        template_name = 'system_logs'
        template_body = {
            "index_patterns": ["system_logs-*"],
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
            },
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "level": {"type": "keyword"},
                    "service": {"type": "keyword"},
                    "host": {"type": "keyword"},
                    "message": {"type": "text"},
                    "thread": {"type": "keyword"},
                    "additional_info": {
                        "properties": {
                            "memory_usage": {"type": "integer"},
                            "cpu_usage": {"type": "integer"},
                            "request_id": {"type": "keyword"},
                            "user_id": {"type": "keyword"}
                        }
                    }
                }
            }
        }
        
        try:
            # Xóa template cũ nếu tồn tại
            if self.es.indices.exists_template(name=template_name):
                self.es.indices.delete_template(name=template_name)
            
            # Tạo template mới
            self.es.indices.put_template(name=template_name, body=template_body)
            logger.info(f"Created Elasticsearch template: {template_name}")
            
        except Exception as e:
            logger.error(f"Error creating template: {e}")
            raise

    def process_log(self, log_data):
        # Xử lý log message sau khi nhận từ Kafka.
        
        # Ví dụ: nếu level là ERROR, ta ghi cảnh báo riêng.
        if log_data['level'] == 'ERROR':
            logger.warning(f"Error detected in service {log_data['service']}: {log_data['message']}")
        
        # Tạo tên index dựa trên ngày trong timestamp (ví dụ: system_logs-2024-12-17)
        index_name = f"system_logs-{log_data['timestamp'][:10]}"
        
        try:
            # Lưu log vào Elasticsearch
            self.es.index(index=index_name, body=log_data)
            logger.info(f"Processed and stored log: {log_data['level']} - {log_data['message']}")
        except Exception as e:
            logger.error(f"Error processing log: {e}")

    def run(self):
        # Consumer đăng ký (subscribe) topic 'system_logs'.
        # Consumer sẽ lắng nghe các message mới trên topic này.
        self.consumer.subscribe(['system_logs'])
        logger.info("Starting log processor...")
        
        try:
            while True:
                # Poll để lấy message từ Kafka.
                # poll(1.0) nghĩa là chờ tối đa 1 giây để nhận 1 message.
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    # Không có message mới, tiếp tục vòng lặp.
                    continue
                
                if msg.error():
                    # Nếu có lỗi từ Kafka, kiểm tra loại lỗi.
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Báo hiệu rằng đã đọc đến cuối partition.
                        logger.debug('Reached end of partition')
                    else:
                        # Lỗi khác
                        logger.error(f'Error: {msg.error()}')
                    continue
                
                # Nếu không có lỗi, ta xử lý message.
                try:
                    # Giải mã nội dung message từ bytes sang chuỗi JSON, sau đó parse thành dict.
                    log_data = json.loads(msg.value().decode('utf-8'))
                    
                    # Gọi hàm xử lý log.
                    self.process_log(log_data)
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding message: {e}")
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    
        except KeyboardInterrupt:
            # Nhấn Ctrl+C để dừng chương trình.
            logger.info("Stopping log processor...")
        finally:
            # Đóng consumer để giải phóng tài nguyên.
            self.consumer.close()
            logger.info("Log processor stopped")

if __name__ == "__main__":
    try:
        # Khởi tạo và chạy log processor
        log_processor = LogProcessor()
        log_processor.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
