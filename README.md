
docker-compose up -d

pip install elasticsearch==7.13.4
pip install confluent-kafka


Liệt kê các topic:
kafka-topics --list --bootstrap-server localhost:9092

Kiểm tra Partition, Leader và các thông tin chi tiết:
kafka-topics --describe \
    --topic system_logs \
    --bootstrap-server localhost:9092
	
PartitionCount: Số lượng Partition.
Leader: Broker đang chịu trách nhiệm chính cho Partition.
Replicas: Các bản sao dữ liệu của Partition.
Isr: In-Sync Replicas (các replica đang đồng bộ).


Tăng số Partition
kafka-topics --alter \
    --topic system_logs \
    --partitions 3 \
    --bootstrap-server localhost:9092


Consumer Group:
kafka-consumer-groups --list --bootstrap-server localhost:9092

Mô tả chi tiết một Consumer Group:
kafka-consumer-groups --describe \
    --group log_processor_group \
    --bootstrap-server localhost:9092
	
GROUP: Tên của Consumer Group (ở đây là log_processor_group).
TOPIC: Topic mà Consumer đang đọc dữ liệu.
PARTITION: Partition mà Consumer đang đọc.
CURRENT-OFFSET: Offset hiện tại của Consumer trong Partition.
LOG-END-OFFSET: Offset cuối cùng trong Partition (điểm dữ liệu mới nhất).
LAG: Số lượng message chưa được Consumer đọc (độ trễ).
CONSUMER-ID: ID của Consumer trong Group.
HOST: Địa chỉ IP của máy chạy Consumer.
CLIENT-ID: Tên Client (Consumer) được chỉ định.