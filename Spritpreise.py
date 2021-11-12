#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, DateType, StringType, IntegerType, TimestampType

spark = SparkSession.builder.appName('Spritpreise').getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(spark.sparkContext)

#
# Lade Tankstellen Stationen als Schema
#
print("1. Laden von Tankstellen Stationen.csv")
station_schema = StructType()
station_schema.add("uuid", StringType(), True)
station_schema.add("name", StringType(), True)
station_schema.add("brand", StringType(), True)
station_schema.add("street", StringType(), True)
station_schema.add("house_number", StringType(), True)
station_schema.add("post_code", StringType(), True)
station_schema.add("city", StringType(), True)
station_schema.add("latitude", StringType(), True)
station_schema.add("logitude", StringType(), True)
station_schema.add("first_active", StringType(), True)
station_schema.add("opening_times_json", StringType(), True)

station_schema = sqlContext.read.options(delimiter=',').schema(station_schema).csv('2021-11-05-stations.csv')

station_schema.show()
station_schema.printSchema();

#
# Lade Benzinpreise von einem Tag als Schema
#
print("2. Laden von Spritpresien.csv")
gas_schema = StructType()
gas_schema.add("date", TimestampType(), True)
gas_schema.add("uuid", StringType(), True)
gas_schema.add("diesel", StringType(), True)
gas_schema.add("e5", StringType(), True)
gas_schema.add("e10", StringType(), True)
gas_schema.add("diesel_change", StringType(), True)
gas_schema.add("e5_change", StringType(), True)
gas_schema.add("e10_change", StringType(), True)

gas_schema = sqlContext.read.options(delimiter=',').schema(gas_schema).csv('2021-11-05-prices.csv')

gas_schema.show()
gas_schema.printSchema();

print("Änderung Spritpreise")

# [row][colum]
test_piv = gas_schema.filter(gas_schema.uuid == station_schema.collect()[1][0])
test_piv.show()

gas_station = station_schema.join(test_piv, station_schema.uuid == test_piv.uuid, "inner")
gas_station.show()
gas_station.to_csv('test')

"""
df_piv1 = gas_schema.orderBy("date", "uuid")
#df_piv1 = df_piv1.dropDuplicates(["uuid","diesel","e5","e10"])
df_piv1.show()
df_piv1 = gas_schema.sort("uuid")

# [row][colum]
print(station_schema.collect()[1][0])
"""
