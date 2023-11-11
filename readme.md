# Bài toán:
***
Các nhà khoa học dữ liệu tại BigMart đã thu thập dữ liệu bán hàng cho 1559 sản phẩm trên 10 cửa hàng ở các thành phố khác nhau trong năm 2013. Bây giờ, mỗi sản phẩm có những thuộc tính nhất định khiến nó khác biệt so với các sản phẩm khác. 

### Phân tích Bài toán: 
Đây là bài toán học máy có giám sát. Giá trị mục tiêu sẽ là Item_Outlet_Sales. 

### Mục tiêu: 
Mục tiêu là tạo ra một mô hình có thể dự đoán doanh số bán hàng cho mỗi sản phẩm tại mỗi cửa hàng. Sử dụng mô hình này, BigMart sẽ cố gắng hiểu các thuộc tính của sản phẩm và cửa hàng, những yếu tố nào đóng vai trò quan trọng trong việc tăng doanh số bán hàng.
***

# Bộ dữ liệu
***
Bộ dữ liệu có tên là bigmart-sales-data. Bộ dữ liệu này được lấy từ [BigMart Sales Data](https://www.kaggle.com/datasets/brijbhushannanda1979/bigmart-sales-data). Bộ dữ liệu này bao gồm 8523 dòng và 12 cột. Các cột là:
1. **Item_Identifier**: ID sản phẩm duy nhất
2. **Item_Weight**: Trọng lượng của sản phẩm
3. **Item_Fat_Content**: Sản phẩm có chứa chất béo thấp hay không
4. **Item_Visibility**: Phần trăm tổng diện tích trưng bày của tất cả các sản phẩm trong cửa hàng được phân bổ cho sản phẩm cụ thể
5. **Item_Type**: Loại sản phẩm
6. **Item_MRP**: Giá bán lẻ tối đa (giá niêm yết) của sản phẩm
7. **Outlet_Identifier**: ID cửa hàng duy nhất
8. **Outlet_Establishment_Year**: Năm cửa hàng được thành lập
9. **Outlet_Size**: Kích thước của cửa hàng theo diện tích đất được phủ
10. **Outlet_Location_Type**: Loại thành phố nơi cửa hàng đặt
11. **Outlet_Type**: Cửa hàng là cửa hàng tạp hóa hay siêu thị
12. **Item_Outlet_Sales**: Doanh số của sản phẩm tại cửa hàng cụ thể (nhãn)
    Đây là biến kết quả cần dự đoán.

# Quy trình giải quyết bài toán
**Khởi tạo:**

Đầu tiên, mã bắt đầu bằng việc nhập các thư viện cần thiết từ PySpark và khởi tạo một phiên Spark.

**Đọc Dữ liệu:**

Dữ liệu được đọc từ các tệp CSV ('Train.csv' và 'Test.csv') vào các DataFrame của Spark.

**Làm Sạch Dữ liệu:**

- Kiểm tra cấu trúc của dữ liệu và xác định các giá trị thiếu.
- Thay thế giá trị thiếu trong cột 'Outlet_Size' bằng giá trị phổ biến nhất trong cả tập huấn luyện và tập kiểm tra.
- Thay thế giá trị thiếu trong cột 'Item_Weight' bằng giá trị trung bình trong cả hai tập dữ liệu.

**Tạo Đặc Trưng:**

- Phân chia các cột thành cột số và cột phân loại.
- Tạo một cột mới 'Outlet_Age' dựa trên sự khác biệt giữa năm hiện tại và 'Outlet_Establishment_Year'.
- Mã hóa biến phân loại sử dụng StringIndexer và OneHotEncoder.
  + StringIndexer: Chuyển đổi các biến phân loại thành các số.
  + OneHotEncoder: Chuyển đổi các biến phân loại thành các vector đặc trưng.
- Tạo vector đặc trưng.

**Phân Chia Dữ liệu:**

Dữ liệu được chia thành tập huấn luyện và tập kiểm tra.

**Huấn Luyện Mô Hình (Hồi Quy Tuyến Tính):**

- Khởi tạo mô hình Hồi quy Tuyến tính.
- Mô hình được huấn luyện trên tập dữ liệu huấn luyện.
- Dự đoán trên tập kiểm tra.

**Xác Thực Chéo:**

Thực hiện xác thực chéo trên mô hình Hồi quy Tuyến tính và đánh giá hiệu suất bằng các chỉ số như điểm R2, RMSE, MSE, và MAE.  

**Phân Tích Sai Số (Residual Analysis):**  
- Tính toán sai số (sự chênh lệch giữa giá trị thực và giá trị dự đoán).
- Tạo một DataFrame chứa 'Item_Outlet_Sales', 'prediction', và 'residuals'.
- Lưu DataFrame sai số vào một tệp CSV ('predictions.csv'). 

Lưu ý: Đoạn mã này giả định đã thiết lập và cấu hình môi trường PySpark đúng đắn. Đồng thời, đảm bảo rằng các tệp dữ liệu cần thiết có sẵn tại các đường dẫn đã chỉ định ('bigmart-sales-data/Train.csv' và 'bigmart-sales-data/Test.csv').
