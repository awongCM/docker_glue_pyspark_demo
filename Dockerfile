# Use AWS Glue image as the base
FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

# https://stackoverflow.com/a/74598849
RUN rm /home/glue_user/spark/conf/hive-site.xml

# Removed as they conflict with structured streaming jars below  
RUN rm /home/glue_user/spark/jars/org.apache.commons_commons-pool2-2.6.2.jar
RUN rm /home/glue_user/spark/jars/org.apache.kafka_kafka-clients-2.6.0.jar
RUN rm /home/glue_user/spark/jars/org.spark-project.spark_unused-1.0.0.jar

# Switch to root user to install necessary dependencies
USER root

# Upgrade pip to the latest version
RUN pip3 install --upgrade pip

# Install Jupyterlab
RUN pip3 install jupyterlab

# Expose Jupyter port
EXPOSE 8888

# Install Poetry for dependency management
RUN pip3 install poetry

# Add Kafka JAR for structured streaming
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar /home/glue_user/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar /home/glue_user/aws-glue-libs/jars/

ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar /home/glue_user/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar /home/glue_user/aws-glue-libs/jars/

ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /home/glue_user/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /home/glue_user/aws-glue-libs/jars/

# Add Iceberg JARs for Iceberg integration
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/0.14.0/iceberg-spark-runtime-3.3_2.12-0.14.0.jar /home/glue_user/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/0.14.0/iceberg-spark-runtime-3.3_2.12-0.14.0.jar /home/glue_user/aws-glue-libs/jars/

# ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-api/0.14.0/iceberg-api-0.14.0.jar /home/glue_user/spark/jars/
# ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-core/0.14.0/iceberg-core-0.14.0.jar /home/glue_user/spark/jars/


# for checkpointing bronze hadoop
# RUN mkdir -p /tmp/spark-staging

# To ensure poetry shell will launch appropriately inside docker
ENV SHELL /bin/bash

WORKDIR /app
RUN poetry config virtualenvs.in-project true

COPY pyproject.toml poetry.lock* /app/
COPY plain /app/plain
COPY purchase_order /app/purchase_order

# Disable Poetry's virtual environment creation, install project dependencies via poetry   
RUN poetry install

# Run the container - non-interactive
ENTRYPOINT [ "/bin/bash", "-l", "-c" ]
CMD ["/bin/bash"]

# Start the history server
# CMD ["/bin/bash", "-c", "/home/glue_user/spark/sbin/start-history-server.sh"]