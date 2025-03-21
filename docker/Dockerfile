# Use official Apache Spark image
FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV SPARK_MASTER_URL=spark://spark-master:7077
ENV SPARK_LOCAL_DIRS=/tmp
ENV SPARK_WORKER_DIR=/tmp
ENV HADOOP_USER_NAME=root
ENV SPARK_USER=root
ENV HADOOP_CONF_DIR=/opt/spark/conf
ENV SPARK_OPTS=--conf spark.hadoop.security.authentication=simple
ENV PYTHONPATH=/app
# Add ~/.local/bin to PATH for user-installed packages like pytest
ENV PATH=$PATH:/home/spark/.local/bin

# Create writable directories for PySpark dependencies
RUN mkdir -p /tmp/.ivy2 /opt/spark/work && chmod -R 777 /tmp/.ivy2 /opt/spark/work

# Switch to root temporarily to install packages
USER root
RUN apt-get update && apt-get install -y python3-pip


# Install PySpark in the user directory
RUN pip install --upgrade --user pip && pip install pyspark

RUN pip install pytest

# Download PostgreSQL JDBC Driver
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.2.27.jar -P /opt/spark/jars/

# Set Spark classpath to include the JDBC driver
ENV SPARK_CLASSPATH="/opt/spark/jars/postgresql-42.2.27.jar"

# Switch back to the default Spark user
USER spark

# Set working directory
WORKDIR /app

# Copy application files
COPY src/ src/
COPY config/ config/
COPY config/core-site.xml /opt/spark/conf/core-site.xml