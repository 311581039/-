from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import sys

spark = SparkSession.builder \
    .appName("data_preprocessing") \
    .getOrCreate()

def data_preprocess(FILENAME):
    # Read File
    csv_file = spark.read.csv(FILENAME, header = True) 
    #csv_file.show(5)


    # Drop unnecessary columns and rows
    cols = ("發生年度","發生月份")
    clean_file = csv_file. \
                    drop(*cols). \
                    filter(csv_file["道路類別-第1當事者-名稱"] != "國道")
    #print("Totol count:", csv_file.count(), "\n[國道] count:", csv_file.count() - clean_file.count())
    #clean_file.show(5)


    # Fix the column of "Gender" ,add new colume "Object", fix column "發生時間"
    #clean_file.groupBy("當事者屬-性-別名稱").count().show()

    row_list = clean_file.collect()
    dict_list = []
    for row in row_list:
        temp = row.asDict()
        temp["無或物"] = False
        temp["發生時間"] = temp["發生時間"][:2]
        dict_list.append(temp)

    for i in range(len(dict_list)):
        if dict_list[i]["當事者屬-性-別名稱"] == "無或物(動物、堆置物)":
            count = int(dict_list[i]["當事者順位"])-1 #有幾個row要加欄位
            while count == 1:
                dict_list[i-count]["無或物"] = True
                count -= 1

    cols = clean_file.columns
    cols.insert(34 , "無或物")
    df = spark.createDataFrame(dict_list).select(cols)

    # rename column to "Gender" and drop rows of "當事者屬-性-別名稱" != "無或物(動物、堆置物)"
    df = df.filter(df["當事者屬-性-別名稱"] != "無或物(動物、堆置物)").withColumnRenamed("當事者屬-性-別名稱","性別")


    # validate
    #df.groupBy("性別").count().show()
    #df.show(10)


    # Split 死亡 and 受傷

    df = df.withColumn('死亡人數', f.split(df['死亡受傷人數'], ';').getItem(0)) \
        .withColumn('受傷人數', f.split(df['死亡受傷人數'], ';').getItem(1)) \
        .withColumn("死亡人數", f.regexp_replace(f.col("死亡人數"), "[死亡]", "")) \
        .withColumn("受傷人數", f.regexp_replace(f.col("受傷人數"), "[受傷]", ""))


    #df.select('死亡人數', '受傷人數').show()


    # Extract 發生地點
    df=df.withColumn('site_id', df.發生地點.substr(1, 6))


    # Save as parquet
    """
    Why parquet?
    - Parquet has helped its users reduce storage requirements by at least one-third on large datasets
    - it greatly improved scan and deserialization time, hence the overall costs.
    """
    print("---------------Save as parquet----------------")
    df.write.format("parquet")\
        .option("encoding", "UTF-8")\
        .option("charset", "UTF-8")\
        .save("data_parquet/"+FILENAME[:-4])
    print("----------------------Done--------------------")


    # Combine with calendar
    """
    以下參考用：合併兩個資料集的code
    """
    #calendar=spark.read.csv('111年中華民國政府行政機關辦公日曆表.csv',header=True, encoding='utf8')

    #calendar = calendar.drop(calendar.columns[1],calendar.columns[3]) \
    #                .withColumnRenamed(calendar.columns[0],"Date") \
    #                .withColumnRenamed(calendar.columns[2],"holiday")

    #df = df.join(calendar, df.發生日期 ==  calendar.Date, "inner") \
    #        .drop(calendar.Date)

    #df.show(10)


if __name__ == "__main__":
    filename = str(sys.argv[1])
    data_preprocess(filename)
    spark.stop()
