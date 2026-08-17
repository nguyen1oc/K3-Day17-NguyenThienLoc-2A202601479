# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Thiên Lộc  
**Lớp:** E403-Track2  
**Mã SV/HV:** 2A202601479

---

## 0 · Kết quả `make verify`

<details open>
<summary>Output ba lần chạy từ make verify</summary>

```text
  queries/dashboard.sql
  --------------------------------------------------------------
                             TRƯỚC        HIỆN TẠI      MỤC TIÊU
  rows scanned           5,000,000         132,790     ≤ 500,000   ✓
  rows on disk             130,683         130,683   (tham khảo)
  files                      5,000              14        ít hơn   ✓
  result hash         4379e4c5d9f3    4379e4c5d9f3     không đổi   ✓
  thời gian (ms)                 —           258.1   (tham khảo)

  => giảm 37.7× (cần ≥ 10×)

  kết quả truy vấn (1 hàng):
    ('ACME', 3500, 3068, 2521.1, 4691, 262, 7764750)


  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 97.5s
  run 2/3 … 76.4s
  run 3/3 … 88.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 132,790 (37.7×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** (100% Tiêu chí chính + Đạt trọn vẹn cả 2 bài tập mở rộng)

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| Hạng mục | Chi tiết |
|---|---|
| **Triệu chứng** | Khi rớt mạng hoặc người trực bấm *Clear Task* trên Airflow để chạy lại, số dòng bảng `gold_training_set` tăng liên tục từ 12,480 lên 38,750+ dòng qua các lượt chạy. Checksum thay đổi liên tục qua từng lượt. |
| **Nguyên nhân** | **(1) Tại dbt Model:** Model `gold_training_set` khai báo `materialized = 'incremental'` nhưng thiếu `unique_key` và `incremental_strategy`. Mặc định dbt sinh ra câu lệnh `INSERT INTO` (ghi nối đuôi). Nguồn CDC chứa các bản ghi cập nhật `op = 'u'`. Khi 1 ticket được update nhiều lần hoặc rerun cùng 1 partition, dbt không biết record nào đã tồn tại để MERGE mà thực hiện ghi trùng $\rightarrow$ Bảng bị lặp trùng ticket.<br>**(2) Tại DAG Airflow:** Cấu hình `catchup=True` và thiếu `max_active_runs` khiến Airflow tự động schedule chạy bù nhiều lượt song song khi rớt mạng, làm bùng phát tần suất lặp dữ liệu. |
| **Cách khắc phục** | **(1) `dags/ai_training_pipeline.py`:** Đặt `catchup=False` và `max_active_runs=1`.<br>**(2) `dbt/models/gold/gold_training_set.sql`:** Khai báo `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` trong khối `config(...)`. |
| **Bằng chứng** | **Trước:** 38,750 hàng (Checksum thay đổi qua từng lượt)<br>**Sau:** 12,480 hàng · Checksum 3 lượt giống hệt nhau (`8dd7c98653`) · 1 hàng / 1 ticket không lặp. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| Hạng mục | Chi tiết |
|---|---|
| **Triệu chứng** | Bảng `gold_feature_daily` bị thiếu khoảng 5% dữ liệu ở các ngày trong quá khứ (8,645 dòng so với kỳ vọng 9,100 dòng). |
| **P99 độ trễ đo được** | **2.45 ngày** (~3 ngày) |
| **Lookback đã chọn** | **3 ngày** — vì bao phủ hơn 99% các sự kiện về muộn (Late-arriving events) mà không phải quét lại toàn bộ dữ liệu lịch sử gây lãng phí compute. |
| **Nguyên nhân** | Khối incremental ban đầu dùng điều kiện `where event_date > (select max(event_date) from {{ this }})`. Khi một event xảy ra ngày 08-12 nhưng tới kho muộn ngày 08-15 (`_ingested_at = 08-15`), `max(event_date)` lúc đó đã là `08-14`. Điều kiện `event_date > 08-14` loại bỏ thẳng tay event ngày 08-12 $\rightarrow$ Dữ liệu quá khứ bị hụt vĩnh viễn. |
| **Cách khắc phục** | Trong [`dbt/models/gold/gold_feature_daily.sql`](file:///c:/Users/nguyenloc/OneDrive/Desktop/vinAi/phase_2/day_17/K3-Day17-NguyenThienLoc-2A202601479/dbt/models/gold/gold_feature_daily.sql):<br>**(1)** Lùi Lookback Window: `where event_date >= (select max(event_date) - interval '3 days' from {{ this }})`.<br>**(2)** Thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` trong `config(...)` để ghi đè khi recalculate. |
| **Bằng chứng** | **Trước:** 8,645 hàng<br>**Sau:** 9,100 hàng (Khớp 100% kỳ vọng) · Checksum 3 lượt: `3db448685c`. |

> **Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?**
> 
> * **P99 (Percentile 99):** Giúp xử lý dứt điểm 99% trường hợp dữ liệu trễ với chi phí tính toán cố định, tối ưu (chỉ lùi 3 ngày). 1% outlier cực hiếm bị trễ hàng tháng sẽ được xử lý qua kịch bản backfill định kỳ riêng.
> * **MAX:** Nếu căn cứ theo `max` độ trễ (có thể lên tới hàng tháng/năm do sự cố hệ thống), mỗi lượt chạy incremental sẽ buộc dbt phải quét và tính toán lại toàn bộ dữ liệu lịch sử. Chi phí là thời gian thực thi pipeline tăng vọt, tiêu tốn I/O và chi phí hạ tầng (compute cost) bùng nổ, làm mất đi hoàn toàn ý nghĩa của kiến trúc Incremental.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| Hạng mục | Chi tiết |
|---|---|
| **Triệu chứng** | Backend đổi `priority` từ số (`1..4`) sang nhãn chữ (`urgent`, `high`, `medium`, `low`) và lọt dữ liệu rác (`0`, `5`, `-1`, `P1`, `unknown`). `silver_tickets` có 6,606 hàng sai và `quarantine_tickets` thiếu 312 hàng. |
| **Nguyên nhân** | Macro cũ dùng `try_cast` làm nhãn chữ bị ép thành `NULL` (làm mất 50% dữ liệu tốt), đồng thời chấp nhận các số ngoài khoảng `0`, `5`, `-1` (vi phạm Data Contract). Đồng thời chưa có logic lọc bản ghi lỗi sang Quarantine. |
| **Ba nhóm giá trị `priority` và cách xử lý** | **1. Số hợp lệ (`'1'..'4'`):** Ép kiểu INTEGER `1..4`.<br>**2. Nhãn chữ (`'urgent'..'low'`):** Schema evolution $\rightarrow$ Map quy đổi về `1..4` (`urgent->1, high->2, medium->3, low->4`).<br>**3. Dữ liệu lỗi (`'0'`, `'5'`, `'-1'`, `'P1'`, `''`, `null`...):** Trả về `NULL` để đưa vào Quarantine. |
| **Cách khắc phục** | **(1) `dbt/macros/normalize_priority.sql`:** Dùng khối `CASE` phân loại 3 nhóm.<br>**(2) `dbt/models/silver/silver_tickets.sql`:** Lọc bản ghi hỏng trước rồi mới xếp hạng `row_number()` để bảo toàn đủ 12,480 ticket.<br>**(3) `dbt/models/silver/quarantine_tickets.sql`:** Lọc các bản ghi `normalize_priority IS NULL`.<br>**(4) `dbt/models/silver/schema.yml`:** Đổi `enforced: true` và bổ sung test `accepted_values: [1, 2, 3, 4]`. |
| **Bằng chứng** | `quarantine_tickets` = **312** hàng (Đúng kỳ vọng 312/312) · `dbt test` pass **11/11** tests · `silver_tickets` đủ **12,480** ticket sạch. |

> **Câu hỏi thiết kế:** Nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?
> 
> * **Nên chặn ở Bronze hay Silver:** Nên chặn và lọc tại tầng **Silver**. Tầng Bronze đóng vai trò Data Lake nguyên bản (Single Source of Truth), phải lưu trữ 100% dữ liệu thô phục vụ audit, debugging và truy vết nguồn gốc (lineage). Nếu từ chối dữ liệu ngay từ Bronze, ta sẽ làm mất dấu vết dữ liệu gốc và không thể tái tạo lại khi khắc phục sự cố.
> * **Vì sao không để pipeline dừng:** Trong hệ thống thực tế, vài trăm bản ghi CDC bị lỗi không có quyền chặn hơn 130,000 sự kiện và 31,200 chunks hoàn toàn bình thường đang phục vụ người dùng cuối. Việc định tuyến dữ liệu lỗi sang bảng `quarantine_tickets` giúp đường ống chính duy trì tính sẵn sàng cao (High Availability), đồng thời tạo hàng đợi riêng cho đội vận hành điều tra và xử lý sau.

---

## 4 · Bài mở rộng trong EXTRA.md

Bạn đã thực hiện **cả 2 bài mở rộng A và B (+10 điểm thưởng)**. Chi tiết từng bài được trình bày riêng biệt dưới đây:

### 4.1 · Bài A — Tối ưu Query Dashboard bị chậm (Small-file problem & Partitioning)

| Hạng mục | Chi tiết |
|---|---|
| **Nguyên nhân** | **(1) Small-file problem:** Thư mục `data/gold_events/` chứa 5,000 file Parquet nhỏ (mỗi file vài chục KB), không được partition hay sắp xếp thứ tự khiến DuckDB phải mở quét hàng nghìn file lãng phí I/O.<br>**(2) Predicate không Sargable:** Truy vấn dùng `strftime(event_time, '%Y-%m-%d') = '2026-08-09'` làm bọc cột `event_time` trong hàm, khiến engine không thể dùng statistics của file/row group để bỏ qua các file không liên quan. |
| **Cách khắc phục** | **(1) `tools/compact.py`:** Thực hiện `COPY ... TO 'data/gold_events_v2'` gộp 5.000 file nhỏ thành 14 file Parquet lớn (`hive_partitioning` theo `event_date`, `ORDER BY customer_name, event_time`).<br>**(2) `queries/dashboard.sql`:** Đọc từ `data/gold_events_v2/*/*.parquet` với `hive_partitioning = true` và đổi điều kiện lọc sang `event_date = '2026-08-09'`. |
| **Bằng chứng** | Số dòng dữ liệu quét (`rows scanned`) giảm **37.7×** (từ 5,000,000 xuống 132,790 dòng), số file giảm từ 5,000 xuống **14 file**, và kết quả Hash (`4379e4c5d9f3`) **không đổi** |
```bash
--------------------------------------------------------------
                           TRƯỚC        HIỆN TẠI      MỤC TIÊU
rows scanned           5,000,000         132,790     ≤ 500,000   ✓
rows on disk             130,683         130,683     (tham khảo)
files                      5,000              14     ít hơn       ✓
result hash         4379e4c5d9f3    4379e4c5d9f3     không đổi   ✓
thời gian (ms)                 —           258.1     (tham khảo)

=> rows scanned giảm 37.

---

### 4.2 · Bài B — Streaming Consumer bị Crash giữa batch (Delivery Semantics & Idempotency)

| Hạng mục | Chi tiết |
|---|---|
| **Nguyên nhân** | Tiến trình consumer gọi `consumer.commit()` trước khi `write_batch(...)`. Khi bị kill/crash giữa chừng, offset đã bị ghi nhận dịch chuyển nhưng dữ liệu batch chưa kịp ghi vào DB $\rightarrow$ Gây **mất dữ liệu** (At-most-once semantics). Nếu chỉ đổi thứ tự ghi trước commit sau, câu lệnh `INSERT` thuần túy sẽ làm **lặp trùng dữ liệu** khi replay batch. |
| **Cách khắc phục** | Trong [`ingest/consumer.py`](file:///c:/Users/nguyenloc/OneDrive/Desktop/vinAi/phase_2/day_17/K3-Day17-NguyenThienLoc-2A202601479/ingest/consumer.py):<br>**(1)** Khai báo `PRIMARY KEY (event_id)` cho bảng `bronze_events_stream`.<br>**(2)** Chuyển sang At-least-once + Idempotent write: Ghi DB trước bằng câu lệnh `INSERT ... ON CONFLICT (event_id) DO UPDATE SET ...`, thử nghiệm crash, rồi mới `consumer.commit()` offset sau cùng. |
| **Bằng chứng** | Kết quả chạy `make crash-test` đạt chuẩn 100% (không mất bản ghi, không trùng bản ghi, C == A):<br>```text\ntopic: 20,000 message · batch 500 · giết ở lô 7\n  A. chạy một mạch: 20,000 hàng / 20,000 event_id\n  B. bị giết ở lô 7: offset đã commit 3,000\n  C. khởi động lại: đã ghi 17,000 message -> 20,000 hàng / 20,000 event_id\n  ----------------------------------------------------------\n  không mất bản ghi                 ✓\n  không trùng bản ghi               ✓\n  C == A                            ✓\n  ----------------------------------------------------------\n  BÀI MỞ RỘNG B: ĐẠT ✓\n``` |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| **1** | Kiểm tra cấu hình `config()` của các dbt incremental model xem có đầy đủ `unique_key` và `incremental_strategy` chưa, đồng thời kiểm tra tham số `catchup` và `max_active_runs` trên Airflow DAG để tránh nhân bản dữ liệu khi rerun. |
| **2** | Đo phân bố độ trễ của dữ liệu nguồn bằng các chỉ số Percentile (P95, P99) để thiết lập Lookback Window hợp lý cho các model incremental, đảm bảo không bỏ sót dữ liệu về muộn (Late-arriving data). |
| **3** | Kiểm tra trạng thái Data Contract (`enforced: true`) và phân loại dữ liệu nguồn (phân biệt giữa Schema Evolution cần mapping vs Dữ liệu rác cần Quarantine) để đảm bảo chất lượng dữ liệu mà không làm gián đoạn pipeline. |

