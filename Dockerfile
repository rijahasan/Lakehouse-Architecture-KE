# Use an existing Spark image as the base
FROM alexmerced/spark35nb:latest

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y openjdk-11-jdk python3 python3-distutils python3-pip

# Install Python 3.9 and pip
#RUN apt-get update && apt-get install -y software-properties-common tzdata && \
#    ln -fs /usr/share/zoneinfo/UTC /etc/localtime && \
 #   dpkg-reconfigure --frontend noninteractive tzdata && \
  #  add-apt-repository ppa:deadsnakes/ppa -y && \
   # apt-get update && \
#    apt-get install -y python3.9 python3.9-distutils && \
#    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
#    python3.9 get-pip.py && \
#    ln -s /usr/bin/python3.9 /usr/bin/python

RUN python3 -m pip install --upgrade pip

# Install necessary Python packages
COPY requirements.txt /requirements.txt
RUN python3 -m pip install --upgrade pip && \
    python3 -m pip install -r /requirements.txt

# Set environment variables for Spark and Java
ENV SPARK_HOME=/opt/spark
#ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:/usr/local/bin:$PATH


# Create a directory for custom JARs and download necessary JARs
RUN mkdir -p /opt/spark/workjars

RUN curl -o /opt/spark/workjars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar

RUN curl -o /opt/spark/workjars/nessie-spark-extensions-3.5_2.12-0.77.1.jar \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.77.1/nessie-spark-extensions-3.5_2.12-0.77.1.jar

RUN curl -o /opt/spark/workjars/bundle-2.24.8.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.24.8/bundle-2.24.8.jar

RUN curl -o /opt/spark/workjars/url-connection-client-2.24.8.jar \
    https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.24.8/url-connection-client-2.24.8.jar

# Copy the startup script into the container
#COPY startup.sh /startup.sh
#RUN chmod +x /startup.sh

# Set the working directory
WORKDIR /opt/spark

# Expose necessary ports
EXPOSE 4040 7077 8080 8081 8888

# Run the startup script
CMD ["/bin/bash", "-c", "python3 /filewatcher & /opt/spark/sbin/start-master.sh && /opt/spark/sbin/start-worker.sh spark://localhost:7077 && mkdir -p /tmp/spark-events && start-history-server.sh && jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' && tail -f /dev/null"]

#CMD ["/startup.sh"]