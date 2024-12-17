Hướng dẫn sử dụng Kafka, Elasticsearch và Log Processor
=======================================================

1. Khởi chạy môi trường bằng Docker Compose
-------------------------------------------
Chạy lệnh dưới đây để khởi động các dịch vụ Kafka, Elasticsearch, và các thành phần liên quan 
(theo cấu hình trong `docker-compose.yml`):

    docker-compose up -d

2. Cài đặt các thư viện Python cần thiết
----------------------------------------
Cài đặt các phiên bản tương thích của Elasticsearch và confluent-kafka:

    pip install elasticsearch==7.13.4
    pip install confluent-kafka

Run 2 file python bằng 2 terminal khác nhau:

    py log_generator.py
    py log_processor.py

3. Quản lý Topic Kafka
----------------------
- Liệt kê tất cả các Topic:

      kafka-topics --list --bootstrap-server localhost:9092

- Tạo một Topic mới (ví dụ: `my_topic`) với 3 partitions và replication-factor = 1:

      kafka-topics --create \
          --topic my_topic \
          --partitions 3 \
          --replication-factor 1 \
          --bootstrap-server localhost:9092

- Kiểm tra thông tin chi tiết của Topic (ví dụ: `system_logs`):

      kafka-topics --describe \
          --topic system_logs \
          --bootstrap-server localhost:9092

  Các thông tin hiển thị:
  - PartitionCount: Số lượng Partition.
  - Leader: Broker chịu trách nhiệm chính cho Partition.
  - Replicas: Danh sách các Broker chứa bản sao dữ liệu của Partition.
  - Isr (In-Sync Replicas): Các replica đang được đồng bộ hóa.

- Tăng số Partition cho một Topic (ví dụ: tăng `system_logs` lên 3 partition):

      kafka-topics --alter \
          --topic system_logs \
          --partitions 3 \
          --bootstrap-server localhost:9092

- Xóa một Topic (ví dụ: `my_topic`):

      kafka-topics --delete \
          --topic my_topic \
          --bootstrap-server localhost:9092

4. Gửi và Nhận Message từ Kafka
-------------------------------
- Gửi message vào một Topic bằng console producer (ví dụ: gửi vào `my_topic`):

      kafka-console-producer \
          --broker-list localhost:9092 \
          --topic my_topic

  (Sau khi chạy lệnh, gõ nội dung tin nhắn và nhấn Enter để gửi.)

- Nhận message từ một Topic bằng console consumer (ví dụ: đọc `my_topic` từ đầu):

      kafka-console-consumer \
          --bootstrap-server localhost:9092 \
          --topic my_topic \
          --from-beginning

5. Quản lý Consumer Groups
--------------------------
- Liệt kê tất cả các Consumer Group:

      kafka-consumer-groups --list --bootstrap-server localhost:9092

- Mô tả chi tiết một Consumer Group (ví dụ: `log_processor_group`):

      kafka-consumer-groups --describe \
          --group log_processor_group \
          --bootstrap-server localhost:9092

  Các thông tin hiển thị:
  - GROUP: Tên Consumer Group (vd: log_processor_group).
  - TOPIC: Tên Topic mà Consumer đang đọc dữ liệu.
  - PARTITION: Partition mà Consumer đang đọc.
  - CURRENT-OFFSET: Offset hiện tại mà Consumer đã đọc đến.
  - LOG-END-OFFSET: Offset cuối cùng trong Partition (dữ liệu mới nhất).
  - LAG: Số lượng message mà Consumer chưa đọc (khoảng trễ).
  - CONSUMER-ID: ID định danh Consumer trong Group.
  - HOST: Địa chỉ IP của máy chủ Consumer đang chạy.
  - CLIENT-ID: Tên Client mà Consumer sử dụng.

- Reset offset cho một Consumer Group (ví dụ: đưa Group `my_group` đọc lại từ đầu Topic `my_topic`):

      kafka-consumer-groups \
          --bootstrap-server localhost:9092 \
          --group my_group \
          --topic my_topic \
          --reset-offsets \
          --to-earliest \
          --execute

------------------------------------------------------------
Chúc bạn thành công trong việc quản trị Kafka và xử lý dữ liệu với Elasticsearch!
