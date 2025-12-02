%spark

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

// Load and parse the data.
val data = sc.textFile("/opt/resources/kmeans_data.txt")
val parsedData = data.map(line => Vectors.dense(line.split(' ').map(_.toDouble))).cache()
println(parsedData)
// Cluster the data into two classes using KMeans.
val clusters = KMeans.train(parsedData, 2, 10, 1, initializationMode = "k-means||")
// val clusters = KMeans.train(parsedData, 2, 20)
// Compute the sum of squared errors.
val cost = clusters.computeCost(parsedData)
// Compute the sum of squared errors.
println("Sum of squared errors = " + cost)