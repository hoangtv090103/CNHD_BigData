#!/usr/bin/env python
# coding: utf-8

# In[469]:

if __name__ == '__main__':
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import *

    spark = SparkSession.builder.appName('BigData').getOrCreate()

    #

    # In[ ]:


    # Đọc dữ liệu từ file csv
    train_df = spark.read.csv('bigmart-sales-data/Train.csv', header=True, inferSchema=True)
    test_df = spark.read.csv('bigmart-sales-data/Test.csv', header=True, inferSchema=True)

    # In[471]:


    # Kiểm tra cấu trúc của dữ liệu
    train_df.printSchema()

    # In[472]:


    # Liệt kế giá trị null của từng đặc trưng
    missing_value = train_df.select(
        [count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in train_df.columns]).toPandas().T
    missing_value = missing_value.rename(columns={0: 'count'})
    print(missing_value)

    #

    # In[473]:


    # Tính giá trị xuất hiện nhiều nhất trong tập dữ liệu
    mode_train = train_df.groupBy('Outlet_Size').count().orderBy('count', ascending=False).first()[0]

    # Thay thế giá trị null bằng giá trị xuất hiện nhiều nhất cho tập train
    train_df = train_df.fillna({'Outlet_Size': mode_train})

    # Kiểm tra lại giá trị null
    mode_test = test_df.groupBy('Outlet_Size').count().orderBy('count', ascending=False).first()[0]

    # Thay thế giá trị null bằng giá trị xuất hiện nhiều nhất cho tập test
    test_df = test_df.fillna({'Outlet_Size': mode_test})

    # Kiểm tra số lượng giá trị null còn lại của 'Outlet_Size'
    missing_train = train_df.filter(col('Outlet_Size').isNull()).count()
    missing_test = test_df.filter(col('Outlet_Size').isNull()).count()

    print(missing_train, missing_test)

    # In[474]:


    from pyspark.sql.functions import mean

    # Tính giá trị trung bình của 'Item_Weight'
    mean_train = train_df.select(mean(col('Item_Weight'))).collect()[0][0]

    mean_test = test_df.select(mean(col('Item_Weight'))).collect()[0][0]

    # Thay thế giá trị null bằng giá trị trung bình
    train_df = train_df.fillna({'Item_Weight': mean_train})

    test_df = test_df.fillna({'Item_Weight': mean_test})

    # Kiểm tra số lượng giá trị null còn lại của 'Item_Weight'
    missing_train = train_df.filter(col('Item_Weight').isNull()).count()
    missing_test = test_df.filter(col('Item_Weight').isNull()).count()

    print(missing_train, missing_test)

    # In[475]:


    # Get the column names and their data types
    column_data_types = train_df.dtypes

    # Separate numeric and categorical columns
    num_columns = [column for column, dtype in column_data_types if dtype in ['int', 'double']]
    cat_columns = [column for column, dtype in column_data_types if dtype == 'string']

    # Create dataframes for numeric and categorical columns
    BM_num = train_df.select(num_columns)
    BM_cat = train_df.select(cat_columns)

    # Print the column names
    print(num_columns)
    print(cat_columns)

    # Print the value counts for each categorical column
    for column in cat_columns[1:]:
        train_df.groupBy(column).count().show()

    #

    # In[490]:


    from pyspark.sql.functions import when, col

    # Đổi tên giá trị 'LF', 'low fat' thành 'Low Fat' và 'reg' thành 'Regular'
    train_df = train_df.withColumn('Item_Fat_Content',
                                   when(col('Item_Fat_Content') == 'LF', 'Low Fat')
                                   .when(col('Item_Fat_Content') == 'low fat', 'Low Fat')
                                   .when(col('Item_Fat_Content') == 'reg', 'Regular')
                                   .otherwise(col('Item_Fat_Content')))

    test_df = test_df.withColumn('Item_Fat_Content',
                                 when(col('Item_Fat_Content') == 'LF', 'Low Fat')
                                 .when(col('Item_Fat_Content') == 'low fat', 'Low Fat')
                                 .when(col('Item_Fat_Content') == 'reg', 'Regular')
                                 .otherwise(col('Item_Fat_Content')))

    # Kiểm tra lại giá trị của 'Item_Fat_Content'
    train_df.groupBy('Item_Fat_Content').count().show()

    # In[477]:


    from pyspark.sql.functions import lit
    from datetime import date

    # Tạo cột 'Outlet_Age' với giá trị là hiện tại trừ đi năm thành lập cửa hàng
    train_df = train_df.withColumn('Outlet_Age', lit(2013) - col('Outlet_Establishment_Year'))

    test_df = test_df.withColumn('Outlet_Age', lit(2013) - col('Outlet_Establishment_Year'))

    # Kiểm tra lại giá trị của 'Outlet_Age'
    train_df.select('Outlet_Age').show()
    test_df.select('Outlet_Age').show()

    # In[478]:


    from pyspark.sql.functions import countDistinct

    # Tính số lượng giá trị duy nhất của từng cột
    unique_values = {column: BM_cat.select(countDistinct(column)).first()[0] for column in BM_cat.columns}

    # In ra số lượng giá trị duy nhất của từng cột
    for column, unique_count in unique_values.items():
        print(f"{column}: {unique_count}")

    # In[479]:


    from pyspark.ml.feature import StringIndexer

    # Liệt kê các cột cần được mã hóa (do có giá trị là string, chuỗi)
    columns_to_encode = ['Item_Fat_Content', 'Outlet_Size', 'Outlet_Location_Type']

    # Áp dụng StringIndexer (biến đổi chuỗi thành số) cho từng cột trong list
    for column in columns_to_encode:
        indexer = StringIndexer(inputCol=column, outputCol=column + "_index")
        train_df = indexer.fit(train_df).transform(train_df)
        test_df = indexer.fit(test_df).transform(test_df)

    # Hiển thị dữ liệu
    train_df.show()

    # In[480]:


    from pyspark.ml.feature import OneHotEncoder, StringIndexer

    # Liệt kê các cột cần được mã hóa (do có giá trị là string, chuỗi)
    columns_to_encode = ['Item_Type', 'Outlet_Type']

    # Áp dụng StringIndexer (biến đổi chuỗi thành số) và OneHotEncoder (biến đổi số thành vector) cho từng cột trong danh sách cột cần được mã hóa

    for column in columns_to_encode:
        # StringIndexer
        indexer = StringIndexer(inputCol=column, outputCol=column + "_index")
        train_indexed = indexer.fit(train_df).transform(train_df)
        test_indexed = indexer.fit(test_df).transform(test_df)

        # OneHotEncoder
        encoder = OneHotEncoder(inputCols=[column + "_index"], outputCols=[column + "_vec"])
        train_fe = encoder.fit(train_indexed).transform(train_indexed)
        test_fe = encoder.fit(test_indexed).transform(test_indexed)

    # In[481]:


    # Danh sách các cột sẽ bị xóa
    columns_to_drop = ['Item_Identifier', 'Outlet_Identifier', 'Outlet_Establishment_Year', 'Outlet_Type', 'Item_Type',
                       'Item_Fat_Content', 'Outlet_Size', 'Outlet_Location_Type']

    # Xoá các cột trong danh sách ở trên
    train_fe = train_df.drop(*columns_to_drop)

    test_fe = test_df.drop(*columns_to_drop)

    # In[482]:


    from pyspark.ml.feature import VectorAssembler

    # Danh sách các cột đầu vào
    input_columns = [column for column in train_fe.columns if column != 'Item_Outlet_Sales']

    # Khỏw tạo đối tượng VectorAssembler (tạo vector đầu vào)
    assembler = VectorAssembler(inputCols=input_columns, outputCol='features')

    # Áp dụng VectorAssembler cho tập train và test
    train_data = assembler.transform(train_fe)
    test_data = assembler.transform(test_fe)

    # Now 'features' column is added to the dataframes


    # In[483]:


    # Chia tập dữ liệu thành tập train và test
    X_train, X_test = train_data.randomSplit([0.8, 0.2], seed=0)

    # In[484]:


    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


    def cross_val(model_name, model, X, y, num_folds):
        """
        :param model_name: Tên của mô hình
        :param model: Mô hình
        :param X: Tập dữ liệu đầu vào
        :param y: Tên cột chứa giá trị mục tiêu
        :param num_folds: Số lượng folds (folds: tập con)
        :return:
        """

        # tạo lưới tham số (lưới tham số: tập các tham số mà mô hình sẽ thử)
        paramGrid = ParamGridBuilder().build()

        # Khởi tạo đối tượng RegressionEvaluator (đánh giá mô hình hồi quy)
        evaluator = RegressionEvaluator(labelCol=y, predictionCol="prediction")

        # Khởi tạo đối tượng CrossValidator (xác thực chéo)
        crossval = CrossValidator(estimator=model,
                                  estimatorParamMaps=paramGrid,
                                  evaluator=evaluator,
                                  numFolds=num_folds)

        # Fit the model
        cv_model = crossval.fit(X)

        # Generate predictions
        predictions = cv_model.transform(X)

        # Calculate R2 score
        r2_score = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        print(f"{model_name} - R2 score: {r2_score:.3f}")

        # Calculate RMSE
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        print(f"{model_name} - RMSE: {rmse:.3f}")

        # Calculate MSE
        mse = evaluator.evaluate(predictions, {evaluator.metricName: "mse"})
        print(f"{model_name} - MSE: {mse:.3f}")

        # Calculate MAE
        mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
        print(f"{model_name} - MAE: {mae:.3f}")


    # In[485]:


    from pyspark.ml.regression import LinearRegression

    # Initialize the Linear Regression model
    lr = LinearRegression(featuresCol='features', labelCol='Item_Outlet_Sales', maxIter=10, regParam=0.3,
                          elasticNetParam=0.8)

    # Fit the model to the training data
    lr_model = lr.fit(X_train)  # Use the complete training data with features and target variable

    # # Make predictions on the testing data
    # predictions = lr_model.transform(X_test)

    # Predict from test_fe data
    predictions = lr_model.transform(test_data)

    # Cross validate the Linear Regression model
    cross_val('Linear Regression', lr, train_data, 'Item_Outlet_Sales', 3)

    # In[489]:


    # Thêm côt 'residuals' (sai số) cho tập dữ liệu
    predictions = predictions.withColumn('residuals', col('Item_Outlet_Sales') - col('prediction'))

    actual_pred_df = predictions.select(['Item_Outlet_Sales', 'prediction', 'residuals'])

    actual_pred_df.show()

    actual_pred_df.write.csv('predictions.csv', header=True)
