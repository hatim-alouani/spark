import warnings
import math
import pyspark
import numpy as np
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from flask import Flask, render_template, request, jsonify
import os
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

warnings.filterwarnings("ignore", message="Signature.*does not match any known type")

app = Flask(__name__)

spark = SparkSession.builder \
    .appName("Spark") \
    .master("local[1]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.warehouse.dir", "/tmp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
session.set_keyspace('course_recommendation')

def insert_recommendation(user_id, course_id, course_name, predicted_rating):
    session.execute(
        """
        INSERT INTO recommendations (user_id, course_id, course_name, predicted_rating)
        VALUES (%s, %s, %s, %s)
        """,
        (user_id, course_id, course_name, predicted_rating)
    )

def get_recommendations_from_db(user_id):
    rows = session.execute(
        """
        SELECT course_id, course_name, predicted_rating
        FROM recommendations
        WHERE user_id=%s
        """,
        (user_id,)
    )
    return rows

def get_and_save_recommendations(user_id):
    os.system("clear")
    print(f"Getting recommendations for User ID: {user_id}")
    dataFrame = spark.read.format('csv') \
        .schema('id int, course_id int, rate float, date string, display_name string, comment string') \
        .option('header', True) \
        .load('hdfs://localhost:9000/spark/new_file.csv')
    dataFrame = dataFrame.select('id', 'course_id', 'rate').dropna(subset=['id', 'course_id', 'rate'])
    
    if dataFrame.filter(col("id") == user_id).count() == 0:
        print("User not found in dataset!")
        return [], False

    trainSet, testSet = dataFrame.randomSplit([0.8, 0.2], seed=42)
    als = pyspark.ml.recommendation.ALS(userCol="id", itemCol="course_id", ratingCol="rate", coldStartStrategy="drop", nonnegative=True)
    model = als.fit(trainSet)

    course_ids = dataFrame.select("course_id").distinct()
    user_data = course_ids.withColumn("id", lit(user_id))
    user_predictions = model.transform(user_data)

    rated_courses = dataFrame.filter(col("id") == user_id).select("course_id").distinct()
    unrated_courses = user_predictions.join(rated_courses, "course_id", "left_anti")

    recommended_courses = unrated_courses.orderBy(col("prediction").desc()).limit(5).collect()

    for row in recommended_courses:
        course_name = get_course_name(row.course_id)
        insert_recommendation(user_id, row.course_id, course_name, row.prediction)

    return recommended_courses, True

def get_course_name(course_id):
    data2 = spark.read.format('csv') \
        .option('header', True) \
        .option('inferSchema', True) \
        .load("hdfs://localhost:9000/spark/Course_info.csv")
    course_info = data2.select(col('id').alias('course_id'), col('title'))
    course = course_info.filter(col("course_id") == course_id).collect()
    return course[0].title if course else 'Unknown Course'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/recommendations', methods=['GET'])
def recommendations():
    try:
        user_id = int(request.args.get('user_id'))
        recommended_courses, user_found = get_and_save_recommendations(user_id)

        if not user_found:
            return jsonify({"message": "User not found in the dataset"}), 404

        rows = get_recommendations_from_db(user_id)
        course_list = [
            {"course_id": row.course_id, "course_name": row.course_name, "rating": row.predicted_rating}
            for row in rows
        ]
        
        course_list = sorted(course_list, key=lambda x: x['rating'], reverse=True)
        return jsonify(course_list)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"message": "Error processing the request"}), 500

if __name__ == '__main__':
    app.run(debug=True)
