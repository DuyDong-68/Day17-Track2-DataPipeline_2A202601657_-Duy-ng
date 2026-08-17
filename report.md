# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Duy Dong  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 35.6s
  run 2/3 … 35.0s
  run 3/3 … 35.0s

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

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Bảng `gold_training_set` số hàng bị nhân lên nhiều lần sau nhiều vòng lập (12,480 -> 24,960 -> 37,440), gây tốn kém tài nguyên và data bị duplicate. |
| **Nguyên nhân** | Cấu hình mặc định cho bảng `incremental` của dbt là `append` (chèn thêm). DAG Airflow cũng bắt đầu quét hết lịch sử lại từ đầu (`catchup=True`) dẫn đến dữ liệu lịch sử bị chạy chèn liên tục. |
| **Cách khắc phục** | `gold_training_set.sql`: Đổi sang `incremental_strategy='merge'` kết hợp `unique_key='ticket_id'`. `ai_training_pipeline.py`: Đặt `catchup=False` và `max_active_runs=1`. |
| **Bằng chứng** | trước: 37,440 hàng · sau: 12,480 hàng · checksum 3 lượt: giống hệt nhau (8dd7c98653) |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Số hàng của bảng `gold_feature_daily` bị thiếu 455 bản ghi (~5%) so với kỳ vọng 9,100 hàng ở những ngày trong quá khứ. |
| **P99 độ trễ đo được** | **2.72 ngày** |
| **Lookback đã chọn** | 3 ngày — vì bao trọn phân vị P99 và tối ưu hơn so với việc lấy độ trễ Max hay lùi quá nhiều ngày, cân bằng giữa hiệu suất và độ chính xác (99%). |
| **Nguyên nhân** | Điều kiện Incremental trong `gold_feature_daily` thiết lập lọc quá khắt khe: `where event_date > max(event_date)`. Do đó dữ liệu về trễ của các ngày cũ sẽ bị bỏ qua ở các luồng run ngày hiện tại. Đồng thời model thiếu key quy định upsert làm mất đặc tính idempotent. |
| **Cách khắc phục** | Đặt `unique_key=['event_date', 'customer_id']` và strategy `merge` trong `gold_feature_daily.sql`. Nới lỏng điều kiện is_incremental() thành lùi về quá khứ 3 ngày (`- interval 3 day`). |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Chọn P99 giúp cân bằng độ chính xác và hiệu suất. Nếu chọn Max (ví dụ outlier lên tới 30 ngày), mỗi lượt chạy Pipeline sẽ phải quét, tải, tính toán và UPSERT lại khối lượng dữ liệu khổng lồ của toàn bộ 30 ngày qua thay vì 3 ngày. Điều này làm lãng phí rất nhiều tài nguyên cho một lượng nhỏ % dữ liệu về quá muộn.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Data về priority của silver bị chứa Null và số sai lệch miền hợp lệ (1..4). Kiểm định data contract bị hỏng. `quarantine_tickets` rỗng dù có bản ghi lỗi. |
| **Nguyên nhân** | Schema evolution thay đổi từ định dạng int sang string label (`urgent`, `high`...) nhưng macro `normalize_priority` chỉ làm động tác `try_cast`. `schema.yml` cũng bị tắt check data contract. Các record lỗi cũng không được lọc ra khỏi luồng xử lý trước khi tính rank. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Nhóm 1 (số 1..4 hợp lệ): Giữ nguyên kiểu int. Nhóm 2 (nhãn chuỗi): Map thành số 1..4 tương ứng bằng block CASE. Nhóm 3 (giá trị rác, trống): Set thành NULL và bắt lỗi đẩy vào Quarantine. |
| **Cách khắc phục** | - Fix `normalize_priority.sql` dùng `case when`.<br>- Lọc `where priority is not null` bên trong CTE `ranked` của `silver_tickets.sql`.<br>- Gom dòng lỗi bằng `priority is null` ở `quarantine_tickets.sql`.<br>- Bật `enforced: true` và `accepted_values = [1,2,3,4]` trong schema.yml. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` 11/11 pass |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Nên chặn ở tầng Silver vì Bronze có trách nhiệm giữ nguyên bản gốc luồng CDC mà không can thiệp. Pipeline không nên dừng khi gặp lỗi nhỏ vì vài trăm dòng lỗi không có quyền chặn hơn 100k sự kiện đúng (chiếm 99.9%), cung cấp tính năng dashboard và AI kịp thời cho người dùng. Dùng Quarantine để hứng các record lỗi giúp dev điều tra sau mà hệ thống vẫn sống.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | A & B |
| **Nguyên nhân** | (A) Dashboard chậm do truy vấn scan 5000 file nhỏ (small-file problem). Cột filter bị bọc function làm hỏng push-down. <br> (B) Consumer thiết kế at-most-once (commit xong mới ghi) nên mất 500 dòng khi chết. Hơn nữa, phép ghi không hỗ trợ idempotent (Upsert). |
| **Cách khắc phục** | (A) Dùng `compact.py` đọc lại toàn bộ và ghi vào thư mục phân mảnh `partition_by(event_date)`, gom theo khối 100k dòng, đổi query so khớp đúng tên thư mục partition. <br> (B) Đổi Consumer thành at-least-once (ghi xong mới commit). Thêm `PRIMARY KEY` cho id và sửa câu `INSERT` thành `ON CONFLICT DO UPDATE SET` để replay dòng bị lặp không gây đúp. |
| **Bằng chứng** | (A) `rows scanned` giảm từ 5,000,000 còn 137,438. `files` giảm từ 5000 còn 14.<br>(B) `make crash-test` kết luận không trùng không mất dữ liệu, `C == A (20,000) ✓`. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Các DAG Schedule có bật `catchup` bừa bãi không? Bảng có cơ chế khoá (Unique Key) chưa? |
| 2 | Mẫu dữ liệu thực tế đến trễ (đo min/max/percentile latency của ingest date) để đặt Limit window chuẩn. |
| 3 | Schema file YAML có đang tắt Data Contract không? Các bước kiểm định (`not_null`, `accepted_values`) đã đủ mạnh chưa? |
