# BIG DATA FINAL PROJECT
## INTRODUCTION

### Environment
- platform: MACOS, Windows
- framework: Pyspark
    - [setup on windows ](https://sparkbyexamples.com/pyspark/how-to-install-and-run-pyspark-on-windows/)
    - [setup on macos](https://sparkbyexamples.com/pyspark/how-to-install-pyspark-on-mac/)
    - `pip install pyspark`
### Dataset 
- \
    - 108年傷亡道路交通事故資料\
    - 109年傷亡道路交通事故資料\
    - 110年傷亡道路交通事故資料\
    - 111年傷亡道路交通事故資料\
    - 108-111population\
    - 108-111calendars\
    - data_parquet\
    - data_preprocess.py
    - data_preprocess.ipynb
    - ./preprocessing.sh
- 資料來源 - 政府資料開放平臺
    - [108-111年傷亡道路交通事故資料](https://data.gov.tw/datasets/search?p=1&size=10&s=pubdate.date_desc&rft=%E5%82%B7%E4%BA%A1%E9%81%93%E8%B7%AF%E4%BA%A4%E9%80%9A%E4%BA%8B%E6%95%85%E8%B3%87%E6%96%99)
    - [108-111年中華民國政府行政機關辦公日曆表](https://data.gov.tw/dataset/14718)
    - [108-111年各鄉鎮市區人口密度](https://data.gov.tw/dataset/8410)
### Data Preprocess
- Run `chmod 777 preprocessing.sh` to make permission
- Run `time ./preprocessing.sh` 
    - `preprocessing.sh` do data preprocess with script `data_preprocess.py`
    - `time` get total elapsed time 
    - ![](https://hackmd.io/_uploads/BJSX0O7B3.png)
- In `data_preprocess.py`
    1. Drop unnecessary columns and rows
    2. Fix the column of "Gender" ,add new colume "Object", fix column "發生時間"
    3. Split 死亡 and 受傷
    4. Extract 發生地點
    5. Save as parquet
    - Why parquet?
        - Parquet has helped its users reduce storage requirements by at least one-third on large datasets
        - it greatly improved scan and deserialization time, hence the overall costs.
        - ```python=
          df.write.format("parquet")\
            .option("encoding", "UTF-8")\
            .option("charset", "UTF-8")\
            .save("data_parquet")
          ```
        - encoding, charset setting for Chinese 
    6. (Combine with other dataframe)
        - e.g. Combine with calendar
        - ```python=
          filename = "108-111calendars\111年中華民國政府行政機關辦公日曆表.csv"
          calendar = spark.read.csv(filename, header=True, encoding='utf8')

          calendar = calendar.drop(calendar.columns[1],calendar.columns[3]) \
                        .withColumnRenamed(calendar.columns[0],"Date") \
                        .withColumnRenamed(calendar.columns[2],"holiday")

          df = df.join(calendar, df.發生日期 ==  calendar.Date, "inner") \
                .drop(calendar.Date)
          ```


### Model
- Read data
    ```python=
    df = spark.read.parquet("data_parquet/*/*",header = True)`

    df.withColumn("year", f.substring('發生日期', 1,4) ) \
        .groupBy('year').count() \
        .show(5)
    ```
    |year| count|
    |----| -----|
    |2019|721056|
    |2021|751059|
    |2020|765306|
    |2022|787441|


### Validation



### Evaluation



### Summary