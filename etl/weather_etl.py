#Weather ETL Pipeline - AWS Glue (PySpark)

import requests
from datetime import datetime
from awsglue.utils import getResolvedOptions
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round as spark_round
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

# Glue boilerplate
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize SparkSession
spark = SparkSession.builder.appName("WeatherETL").getOrCreate()

def extract_weather():
    api_key = 'YOUR_API_KEY'
    url = f"http://api.openweathermap.org/data/2.5/weather?lat=33.6844&lon=73.0479&appid={api_key}&units=metric"
    
    print("Extracting weather data from API...")
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        print(f"API Response: {data}")
        
        return [{
            'date': str(datetime.now().date()),
            'temp': data['main']['temp'],
            'humidity': data['main']['humidity'],
            'description': data['weather'][0]['description']
        }]
    else:
        raise Exception(f"API call failed: {response.status_code}")

def transform_weather(spark_df):
    print("Transforming data with PySpark...")
    
    transformed_df = spark_df.withColumn(
        'temp_category',
        when(col('temp') > 25, 'Hot')
        .when(col('temp') > 15, 'Mild')
        .otherwise('Cold')
    ).withColumn('temp', spark_round(col('temp'), 2)) \
     .withColumn('humidity', col('humidity').cast(IntegerType()))
    
    print(f"Transformed {transformed_df.count()} records")
    return transformed_df

def load_weather(spark_df):
    print("Loading data to S3...")
    
    bucket = 'islamabad-weather-data-project'
    key = f'weather_{datetime.now().date()}.csv'
    s3_path = f's3://{bucket}/{key}'
    
    spark_df.write.mode('overwrite').csv(s3_path, header=True)
    print(f"Data loaded to S3: {s3_path}")

try:
    raw_data = extract_weather()
    
    schema = StructType([
        StructField('date', StringType(), True),
        StructField('temp', FloatType(), True),
        StructField('humidity', IntegerType(), True),
        StructField('description', StringType(), True)
    ])
    spark_df = spark.createDataFrame(raw_data, schema)
    
    transformed_df = transform_weather(spark_df)
    load_weather(transformed_df)
    
    print("ETL pipeline completed successfully!")
    
except Exception as e:
    print(f"ETL pipeline failed: {str(e)}")
    raise

spark.stop()
