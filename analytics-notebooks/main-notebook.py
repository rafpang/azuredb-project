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
        .csv("dbfs:/mnt/databricksf1projectdl/raw/races.csv").withColumnRenamed("name","circuit_name").select('raceId','year','circuit_name','round')        

constructors = spark.read \
        .option("header",True)\
        .option("inferSchema" , True) \
        .csv("dbfs:/mnt/databricksf1projectdl/raw/constructors.csv").select('constructorId','name','nationality')    


#Joining the dataframes
df = results.join(races, results["raceID"] == races["raceID"], 'left') 
df= df.join(drivers, df["driverId"] == drivers["driverId"], 'left').withColumnRenamed('nationality','driver_nationality') 
df = df.join(constructors, df["constructorId"] == constructors["constructorId"],'left').withColumnRenamed('nationality','constructor_nationality') \
        .withColumnRenamed("name","constructors_name")




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

#Winningest Constructors
display(df.filter(df.positionOrder == 1).select('constructors_name','resultId').groupBy('constructors_name').count().orderBy("count", ascending=False).limit(10)) 

# COMMAND ----------

#My favorite team is Ferrari, Ferrari hasn't had the best of seasons of late. How have they performed in the 2000s
display(df.filter(df.constructors_name == "Ferrari").select('year','positionOrder').filter((df.positionOrder==1)& (df.year >=2000)).groupBy('year').count())


# COMMAND ----------

#Ferrari's strongest tracks (by points)

display(df.filter((df.constructors_name=="Ferrari") & (df.year >= 2000)).select('circuit_name','points').groupBy('circuit_name').sum('points').orderBy('sum(points)', ascending =False))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


