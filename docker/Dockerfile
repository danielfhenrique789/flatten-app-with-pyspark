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
ENV PYSPARK_PYTHON=python3.12
ENV PYSPARK_DRIVER_PYTHON=python3.12

# Create writable directories for PySpark dependencies
RUN mkdir -p /tmp/.ivy2 /opt/spark/work && chmod -R 777 /tmp/.ivy2 /opt/spark/work

# Switch to root temporarily to install packages
USER root
RUN apt-get update && \
    apt-get install -y software-properties-common curl && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.12 python3.12-venv python3-pip && \
    ln -sf /usr/bin/python3.12 /usr/bin/python && \
    ln -sf /usr/bin/python3.12 /usr/bin/python3


# Install PySpark in the user directory
# Use python3.12’s pip to install packages
RUN apt-get update && \
    apt-get install -y \
        build-essential \
        libssl-dev \
        zlib1g-dev \
        libncurses5-dev \
        libncursesw5-dev \
        libreadline-dev \
        libsqlite3-dev \
        libgdbm-dev \
        libdb5.3-dev \
        libbz2-dev \
        libexpat1-dev \
        liblzma-dev \
        libffi-dev \
        wget && \
    wget https://www.python.org/ftp/python/3.12.2/Python-3.12.2.tgz && \
    tar -xf Python-3.12.2.tgz && \
    cd Python-3.12.2 && \
    ./configure --enable-optimizations && \
    make -j$(nproc) && \
    make altinstall && \
    ln -sf /usr/local/bin/python3.12 /usr/bin/python && \
    ln -sf /usr/local/bin/pip3.12 /usr/bin/pip

# Install Python packages
RUN pip install --upgrade pip && pip install pyspark pytest

#RUN pip install pytest

# Download PostgreSQL JDBC Driver
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.2.27.jar -P /opt/spark/jars/

# Set Spark classpath to include the JDBC driver
ENV SPARK_CLASSPATH="/opt/spark/jars/postgresql-42.2.27.jar"

# Switch back to the default Spark user
USER spark

# Set working directory
WORKDIR /app

RUN mkdir -p /app/data && chmod -R 777 /app/data

# Copy application files
COPY src/ src/
COPY config/ config/
COPY config/core-site.xml /opt/spark/conf/core-site.xml