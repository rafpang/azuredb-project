# Databricks notebook source
# MAGIC %md
# MAGIC ### The drivers dataset
# MAGIC This dataset contains all of the formula one drivers that have participated in the sport. 

# COMMAND ----------

#Reading all the files
drivers = spark.read \
        .option("header",True)\
        .option("inferSchema" , True) \
        .csv("dbfs:/mnt/databricksf1projectdl/raw/drivers.csv").select('driverId','driverRef','nationality')

results = spark.read \
        .option("header",True)\
        .option("inferSchema" , True) \
        .csv("dbfs:/mnt/databricksf1projectdl/raw/results.csv")

races = spark.read \
        .option("header",True)\
        .option("inferSchema" , True) \
        .csv("dbfs:/mnt/databricksf1projectdl/raw/races.csv").select('raceId','year','name','round')        

constructors = spark.read \
        .option("header",True)\
        .option("inferSchema" , True) \
        .csv("dbfs:/mnt/databricksf1projectdl/raw/constructors.csv").select('constructorId','name','nationality')    


#Joining the dataframes
df = results.join(races, results["raceID"] == races["raceID"], 'left') 
df= df.join(drivers, df["driverId"] == drivers["driverId"], 'left').withColumnRenamed('nationality','driver_nationality') 
df = df.join(constructors, df["constructorId"] == constructors["constructorId"], 'left').withColumnRenamed('nationality','constructor_nationality') 




# COMMAND ----------


#Let's take a look at our final joined data!
display(df.limit(10))

# COMMAND ----------

#Top 10 Nationalities in drivers dataset: Visualization if viewed on Databricks!

display(df.groupBy("driver_nationality").count().orderBy("count", ascending=False).limit(10))

# COMMAND ----------

#Which drivers have (or had) the most grandprix wins?

display(df.filter(df.positionOrder == 1).select('driverRef','resultId').groupBy('driverRef').count().orderBy("count", ascending=False).limit(10)) 

# COMMAND ----------


