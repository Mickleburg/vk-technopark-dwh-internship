FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# PySpark 3.5.3 bundles Hadoop 3.3.4; use matching S3A dependencies.
RUN python -c "import pathlib,urllib.request,pyspark; p=pathlib.Path(pyspark.__file__).parent/'jars'; [(urllib.request.urlretrieve('https://repo.maven.apache.org/maven2/'+s, p/s.split('/')[-1])) for s in ['org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar','com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar']]"
WORKDIR /work
