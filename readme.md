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