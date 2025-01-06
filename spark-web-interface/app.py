import warnings
warnings.filterwarnings("ignore", message="Signature.*does not match any known type")

from flask import Flask, render_template, request, jsonify
import math
import pyspark
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
import os

app = Flask(__name__)

# Set up PySpark session
spark = SparkSession.builder \
    .appName("Spark") \
    .master("local[1]") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .config("spark.sql.warehouse.dir", "/tmp") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # Options: ALL, DEBUG, INFO, WARN, ERROR, FATAL, OFF

# Function to get recommendations for a user
def get_recommendations(user_id):
    os.system("clear")
    print(f"Getting recommendations for User ID: {user_id}")
    dataFrame = spark.read.format('csv') \
        .schema('id int, course_id int, rate float, date string, display_name string, comment string') \
        .option('header', True) \
        .load('hdfs://localhost:9000/spark/new_file.csv')
    dataFrame = dataFrame.select('id', 'course_id', 'rate')\
        .dropna(subset=['id', 'course_id', 'rate'])
    # Check if user exists in the data
    if dataFrame.filter(col("id") == user_id).count() == 0:
        print("User not found in dataset!")
        return [], False
    # Train ALS model
    trainSet, testSet = dataFrame.randomSplit([0.8, 0.2], seed=42)
    als = ALS(userCol="id", itemCol="course_id", ratingCol="rate", coldStartStrategy="drop", nonnegative=True)
    model = als.fit(trainSet)
    # Generate predictions for unrated courses
    course_ids = dataFrame.select("course_id").distinct()
    user_data = course_ids.withColumn("id", lit(user_id))
    user_predictions = model.transform(user_data)
    rated_courses = dataFrame.filter(col("id") == user_id).select("course_id").distinct()
    unrated_courses = user_predictions.join(rated_courses, "course_id", "left_anti")
    # Get Top 5 Recommendations
    recommended_courses = unrated_courses.orderBy(col("prediction").desc()).limit(5)
    return recommended_courses.collect(), True

# Route to display the interface
@app.route('/')
def index():
    return render_template('index.html')

# API route to get recommendations based on user ID
@app.route('/recommendations', methods=['GET'])
def recommendations():
    try:
        user_id = int(request.args.get('user_id'))
        recommended_courses, user_found = get_recommendations(user_id)

        if not user_found:
            return jsonify({"message": "User not found in the dataset"}), 404

        # Convert recommendations to JSON-serializable format
        course_list = []
        for row in recommended_courses:
            course_name = get_course_name(row.course_id)  # Retrieve course name
            course_list.append({
                'course_id': row.course_id,
                'course_name': course_name,
                'rating': row.prediction  # Add the predicted rating
            })

        if not course_list:
            return jsonify({"message": "No recommendations available"}), 404

        return jsonify(course_list)

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"message": "Error processing the request"}), 500

# Assuming you have a course info dataset loaded with course_id and course_name
def get_course_name(course_id):
    data2 = spark.read.format('csv') \
        .option('header', True) \
        .option('inferSchema', True) \
        .load("hdfs://localhost:9000/spark/Course_info.csv")
    course_info = data2.select(col('id').alias('course_id'), col('title'))
    course = course_info.filter(col("course_id") == course_id).collect()

    if course:
        return course[0].title
    else:
        return 'Unknown Course'

if __name__ == '__main__':
    app.run(debug=True)
