from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class StoreData:

    def __init__(self):
        self.jdbcUrl = "jdbc:mysql://localhost:3306/pipeline"
        self.username = "root"
        self.password = "Badri@123"
        self.spark = SparkSession.builder.appName("Json_to_Table").config("spark.driver.extraClassPath",
                                                             "C:\\Users\\durga\\Downloads\\mysql-connector-j-8.3.0\\mysql-connector-j-8.3.0\\mysql-connector-j-8.3.0.jar") \
            .getOrCreate()

    def get_schema(self):
        return """
 type string , name string , ppu float , batters array <struct < id string, type  string> >, topping array <struct < id string, type  string> >
"""

    def dataReader(self,schema_ddl):

        df = self.spark.read.option("multiline", "true").schema(schema_ddl).json(
            "C:\\Users\\durga\\PycharmProjects\\pythonProject3\\data.json")
        return df

    def json_to_tabular(self,df):
        df2 = df.selectExpr("type", "name", "ppu", "explode(batters) as batter", "topping")

        df3 = df2.withColumn("batter_Id", expr("batter.id")) \
            .withColumn("batter_Type", expr("batter.type"))

        df4 = df3.selectExpr("type", "name", "ppu", "explode(topping) as top", "batter_Id", "batter_Type")
        final_df = df4.withColumn("topping_Id", expr("top.id")) \
            .withColumn("topping_Type", expr("top.type")) \
            .drop("top")

        final_df.createOrReplaceTempView("data3")




    def data_tables(self):
        donut_conf = {

            "squery": """
            select * from data3 where type in ('donut') and topping_Id='5001' ;
            """,
            "t_name": "donut2"
        }

        pizza_conf = {

            "squery": """
            select * from data3 where type in ('Pizza') and topping_Id='5001' ;
            """,
            "t_name": "pizza2"
        }

        data_list = [donut_conf, pizza_conf]

        for i in range(0, len(data_list)):
            self.spark.sql(data_list[i].get("squery")).write \
                .format("jdbc") \
                .option("url", self.jdbcUrl) \
                .option("dbtable", f"{data_list[i].get('t_name')}") \
                .option("user", self.username) \
                .option("password", self.password) \
                .option("truncate", "true") \
                .mode("overwrite") \
                .save()

c1=StoreData()

df=c1.dataReader(c1.get_schema())
c1.json_to_tabular(df)
c1.data_tables()