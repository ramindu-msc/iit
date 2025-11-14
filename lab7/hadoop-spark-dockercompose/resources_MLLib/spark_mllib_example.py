from pyspark.ml.regression import LinearRegression
import pandas as pd
from pyspark.ml.linalg import DenseVector, Vectors, VectorUDT
from pyspark.sql.functions import udf
from pyspark.sql.types import UserDefinedType, StringType

import numpy as np
from bokeh.io import output_notebook, show
from bokeh.plotting import figure

num = 1000
x = np.linspace(0, 10, num)
y = 2 * x + np.random.normal(0,4, num)

print("x values:")
print(x)
print("\ny values:")
print(y)

to_vector = udf(lambda x: Vectors.dense(x), VectorUDT())

df = pd.DataFrame({'features': x, 'label': y})
training = spark.createDataFrame(df).withColumn('features', to_vector('features'))

lr = LinearRegression(maxIter=50, regParam=0.3, elasticNetParam=0.8, solver='l-bfgs')

# Fit the model
lrModel = lr.fit(training)

# Print the coefficients and intercept for linear regression
print("Coefficients: %s" % str(lrModel.coefficients))
print("Intercept: %s" % str(lrModel.intercept))

# Summarize the model over the training set and print out some metrics
trainingSummary = lrModel.summary
print("numIterations: %d" % trainingSummary.totalIterations)
print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
trainingSummary.residuals.show()
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)