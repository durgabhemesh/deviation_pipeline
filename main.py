from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, timedelta



class DispoDeviation:
    def __init__(self):
        self.jdbcUrl = "jdbc:mysql://localhost:3306/pipeline"
        self.username = "root"
        self.password = "Badri@123"



    def dataReader(self):

        spark=SparkSession.builder.appName("Dispo").config("spark.driver.extraClassPath","C:\\Users\\durga\\Downloads\\mysql-connector-j-8.3.0\\mysql-connector-j-8.3.0\\mysql-connector-j-8.3.0.jar")\
                .getOrCreate()
        df=spark.read.format("jdbc") \
            .option("url", self.jdbcUrl) \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("dbtable", "input_data") \
            .option("inferSchema", "true") \
            .load()
        return df
    def dataProcessing(self,df):
        # current_date = datetime.now().strftime("%Y-%m-%d")
        current_date = '2024-02-08'
        y = datetime.strptime(current_date, ("%Y-%m-%d"))
        previous = (y - timedelta(7)).strftime("%Y-%m-%d")

        grouped_data = df.groupBy("Disposition", "DATE").agg(sum("Trnasaction_Value").alias("TotalValue"))

        df1 = grouped_data.filter((col("DATE") == current_date)).sort(col("DATE").asc())
        df2 = grouped_data.filter(col("DATE") == previous).sort(col("DATE").asc())

        df2 = df2.withColumn("TotalValue2", col("TotalValue"))

        joined_df = df1.join(df2, (df1["Disposition"] == df2["Disposition"]), "left").select(df1["*"],
                                                                                             df2["TotalValue2"])

        result = joined_df.withColumn("Percent", ((col("TotalValue") - col("TotalValue2")) / col("TotalValue2")) * 100)

        return result

    def dataWriter(self,result):
            result.write \
            .format("jdbc") \
            .option("url", self.jdbcUrl) \
            .option("dbtable", "disposition_deviation3") \
            .option("user", self.username) \
            .option("password", self.password) \
            .option("truncate", "true") \
            .mode("overwrite") \
            .save()




if __name__ == '__main__':
   c1=DispoDeviation()

   c2=c1.dataProcessing(c1.dataReader())
   c1.dataWriter(c2)



