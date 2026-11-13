FROM apache/airflow:2.10.5-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/opt/java

COPY docker/constraints-3.11.txt /tmp/constraints.txt
USER airflow
RUN pip install --no-cache-dir --constraint /tmp/constraints.txt \
    apache-airflow==2.10.5 pyspark==3.5.4 boto3==1.36.3

# PySpark 3.5.4 ships Hadoop 3.3.4. S3A needs these two matching jars.
# Download once at image build time; scheduled tasks need no Internet access.
RUN mkdir -p /opt/airflow/s3-jars \
    && curl -fSL --retry 3 \
       https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
       -o /opt/airflow/s3-jars/hadoop-aws-3.3.4.jar \
    && curl -fSL --retry 3 \
       https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
       -o /opt/airflow/s3-jars/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /opt/airflow
