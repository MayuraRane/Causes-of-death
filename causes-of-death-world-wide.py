# Causes of Death
# Mayura Rane

# Importing libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd

# Authentication with AWS
secret_key ='***'
access_key ='***'
aws_bucket_name = "world-bank-dataset"
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)


# reading raw data from S2 bucket
raw_data = spark.read.csv('s3://causes-of-death-raw-data/annual-number-of-deaths-by-cause.csv',inferSchema=True,header=True)
display(raw_data.limit(10))

# Data Cleaning

# Checking datatypes of columns
raw_data.printSchema()

# Renaming column names and removing unwanted columns
renamed_df = raw_data.withColumnRenamed('Entity','COUNTRY')\
    .withColumnRenamed('Code','CODE')\
    .withColumnRenamed('Deaths - Meningitis - Sex: Both - Age: All Ages (Number)','MENINGITIS')\
    .withColumnRenamed('Deaths - Neoplasms - Sex: Both - Age: All Ages (Number)','NEOPLASMS')\
    .withColumnRenamed('Deaths - Fire, heat, and hot substances - Sex: Both - Age: All Ages (Number)','FIRE_AND_HEAT')\
    .withColumnRenamed('Deaths - Malaria - Sex: Both - Age: All Ages (Number)','MALARIA')\
    .withColumnRenamed('Deaths - Drowning - Sex: Both - Age: All Ages (Number)','DROWNING')\
    .withColumnRenamed('Deaths - Interpersonal violence - Sex: Both - Age: All Ages (Number)','INTERPERSONAL_VIOLENCE')\
    .withColumnRenamed('Deaths - HIV/AIDS - Sex: Both - Age: All Ages (Number)','HIV_AIDS')\
    .withColumnRenamed('Deaths - Drug use disorders - Sex: Both - Age: All Ages (Number)','DRUG_USE')\
    .withColumnRenamed('Deaths - Tuberculosis - Sex: Both - Age: All Ages (Number)','TUBERCULOSIS')\
    .withColumnRenamed('Deaths - Road injuries - Sex: Both - Age: All Ages (Number)','ROAD_INJURIES')\
    .withColumnRenamed('Deaths - Maternal disorders - Sex: Both - Age: All Ages (Number)','MATERNAL_DISORDERS')\
    .withColumnRenamed('Deaths - Lower respiratory infections - Sex: Both - Age: All Ages (Number)','LOWER_RESPIRATORY_INFECTIONS')\
    .withColumnRenamed('Deaths - Neonatal disorders - Sex: Both - Age: All Ages (Number)','NEONATAL_DISORDERS')\
    .withColumnRenamed('Deaths - Alcohol use disorders - Sex: Both - Age: All Ages (Number)','ALCOHOL_USE')\
    .withColumnRenamed('Deaths - Exposure to forces of nature - Sex: Both - Age: All Ages (Number)','FORCE_OF_NATURE')\
    .withColumnRenamed('Deaths - Diarrheal diseases - Sex: Both - Age: All Ages (Number)','DIARRHEA')\
    .withColumnRenamed('Deaths - Environmental heat and cold exposure - Sex: Both - Age: All Ages (Number)','ENVIRONMENTAL_HEAT_AND_COLD_EXPOSURE')\
    .withColumnRenamed('Deaths - Nutritional deficiencies - Sex: Both - Age: All Ages (Number)','NUTRITIONAL_DEFICIENCIES')\
    .withColumnRenamed('Deaths - Self-harm - Sex: Both - Age: All Ages (Number)','SELF_HARM')\
    .withColumnRenamed('Deaths - Conflict and terrorism - Sex: Both - Age: All Ages (Number)','CONFLICT_AND_TERRORISM')\
    .withColumnRenamed('Deaths - Diabetes mellitus - Sex: Both - Age: All Ages (Number)','DIABETES_MELLITUS')\
    .withColumnRenamed('Deaths - Poisonings - Sex: Both - Age: All Ages (Number)','POISONINGS')\
    .withColumnRenamed('Deaths - Protein-energy malnutrition - Sex: Both - Age: All Ages (Number)','PROTEIN_ENERGY_MALNUTRITION')\
    .withColumnRenamed('Terrorism (deaths)','TERRORISM')\
    .withColumnRenamed('Deaths - Cardiovascular diseases - Sex: Both - Age: All Ages (Number)','CARDIOVASCULAR_DISEASES')\
    .withColumnRenamed('Deaths - Chronic kidney disease - Sex: Both - Age: All Ages (Number)','KIDNEY_DISEASE')\
    .withColumnRenamed('Deaths - Chronic respiratory diseases - Sex: Both - Age: All Ages (Number)','CHRONIC_RESPIRATORY_DISEASES')\
    .withColumnRenamed('Deaths - Cirrhosis and other chronic liver diseases - Sex: Both - Age: All Ages (Number)','CHRONIC_LIVER_DISEASES')\
    .withColumnRenamed('Deaths - Digestive diseases - Sex: Both - Age: All Ages (Number)','DIGESTIVE_DISEASES')\
    .withColumnRenamed('Deaths - Acute hepatitis - Sex: Both - Age: All Ages (Number)','ACUTE_HEPATITIS')\
    .withColumnRenamed('Deaths - Alzheimer\'s disease and other dementias - Sex: Both - Age: All Ages (Number)','ALZHEIMERS_DISEASE')\
    .withColumnRenamed('Deaths - Parkinson\'s disease - Sex: Both - Age: All Ages (Number)','PARKINSONS_DISEASE')\
    .drop('Number of executions (Amnesty International)')

display(renamed_df.limit(10))


# Changing data type of Year to string
renamed_df = renamed_df.withColumn('YEAR', F.col('Year').cast('string'))
renamed_df.printSchema()


# Checking for nulls
display(renamed_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in renamed_df.columns]))


# Removing null values from Code
no_nulls_df = renamed_df.na.drop(subset='CODE')
display(no_nulls_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in no_nulls_df.columns]))


# Replacing nulls with 0
no_nulls_df = no_nulls_df.na.fill(0)
display(no_nulls_df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in no_nulls_df.columns]))


# Remove entriest for "World"
cleaned_data = no_nulls_df.filter(F.col('COUNTRY') != 'World')
display(cleaned_data)

# Only considering coutries which have all year's data from 1990 to 2019

windowSpec = Window.partitionBy('COUNTRY')
allYearsPresent = cleaned_data.withColumn('Total_years', F.count('YEAR').over(windowSpec))
allYearsPresent = allYearsPresent.orderBy('Total_years')
removeCountries = allYearsPresent.filter(F.col('Total_years') != 30).select('COUNTRY', 'Total_years').distinct()
display(removeCountries)


filtered_df = cleaned_data.join(removeCountries, 'COUNTRY', 'left_anti')
display(filtered_df)


# Snowflake connection
snowflake_options = {
    "sfURL" : "https://***.aws.snowflakecomputing.com",
    "sfUser" : "***",
    "sfPassword" : "***",
    "sfDatabase" : "CAUSES_OF_DEATH",
    "sfWarehouse" : "COMPUTE_WH",
    "sfSchema": "MY_SCHEMA"
}


# Writing initial data to Snowflake
filtered_df.write.format("snowflake").options(**snowflake_options).mode("overwrite").option("dbtable", "data").save()
#df = spark.read.format("snowflake").options(**snowflake_options).option("query", "select * from data;").load()
#display(df)


# Converting PySpark DataFrame to Pandas DataFrame
pandas_df = filtered_df.toPandas()


# Unpiviotting data
melted_df = pd.melt(pandas_df, id_vars=['COUNTRY', 'CODE', 'YEAR'], var_name='DISEASE', value_name='DEATHS')

print("\nResulting DataFrame:")
display(melted_df)

# Converting Pandas DataFrame back to Spark DataFrame
melted_df = spark.createDataFrame(melted_df)


# Saving final output in snowflake
melted_df.write.format("snowflake").options(**snowflake_options).mode("overwrite").option("dbtable", "unpivioted_full_data").save()

# Data Analysis


# Top 10 entities with highest total deaths
top_10_deaths = melted_df.groupby(F.col('COUNTRY')).agg(F.sum(F.col('DEATHS')).alias('TOTAL_DEATHS')).orderBy(F.col('TOTAL_DEATHS').desc())
top_10_deaths.show(10)

# Top diseases
top_diseases = melted_df.groupBy("DISEASE").agg(F.sum("DEATHS").alias("TOTAL_DEATHS"))
display(top_diseases.orderBy(F.desc("TOTAL_DEATHS")))

