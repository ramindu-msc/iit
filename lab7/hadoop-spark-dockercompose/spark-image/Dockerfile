# Use a recent Ubuntu base image
FROM ubuntu:22.04

# Set environment variables for Spark, Hadoop, and Java
ENV SPARK_VERSION=2.4.8 \
    HADOOP_VERSION=2.8.5 \
    JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 \
    PATH=$PATH:/opt/hadoop/bin:/opt/spark/bin:/opt/spark/sbin \
    PYSPARK_PYTHON=python3

# Install necessary dependencies
RUN apt-get update && apt-get install -y \
    openjdk-8-jdk \
    wget \
    curl \
    python3 \
    python3-pip \
    bash \
    git \
    && apt-get clean

# Install Hadoop 2.8
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-2.8.5/hadoop-2.8.5.tar.gz \
    && tar -xzf hadoop-2.8.5.tar.gz -C /opt \
    && mv /opt/hadoop-2.8.5 /opt/hadoop \
    && rm hadoop-2.8.5.tar.gz

# Install Spark 2.4.8
RUN wget https://archive.apache.org/dist/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz \
    && tar -xzf spark-2.4.8-bin-hadoop2.7.tgz -C /opt \
    && mv /opt/spark-2.4.8-bin-hadoop2.7 /opt/spark \
    && rm spark-2.4.8-bin-hadoop2.7.tgz

# Install Python dependencies for Jupyter
RUN pip3 install numpy pandas jupyter

# Expose Spark and Jupyter ports
EXPOSE 8080 7077 8081 4040 8888

# Copy initialization script
COPY start-spark.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start-spark.sh

# Set the default command
CMD ["start-spark.sh"]
