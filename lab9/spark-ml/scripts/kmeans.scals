%spark.pyspark

from pyspark.mllib.clustering import KMeans
from pyspark.mllib.linalg import Vectors
from math import sqrt

# Load and parse the data
data = sc.textFile("/opt/resources/kmeans_data.txt")
parsedData = data.map(lambda line: Vectors.dense([float(x) for x in line.split(' ')])).cache()

# Build the model (cluster the data)
clusters = KMeans.train(parsedData, 2, maxIterations=10, runs=1, initializationMode="k-means||")

# Evaluate clustering by computing the sum of squared errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([(x - y) ** 2 for x, y in zip(point, center)]))

cost = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Sum of squared error = " + str(cost))