FROM apache/spark-py:latest

USER root

# Install GDAL & utils
RUN apt-get -y update && apt-get install -y libgdal-dev wget tar bash nmap vim

# Downloads Postgres jars
RUN mkdir -p /opt/spark/extra_jars/
# RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.25.jre6.jar && mv postgresql-42.2.25.jre6.jar /opt/spark/extra_jars/
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar && mv postgresql-42.6.0.jar /opt/spark/extra_jars/
# java -jar /opt/spark/extra_jars/postgresql-42.2.25.jre6.jar

# Install Python libraries
COPY requirements.txt .
RUN pip install -r requirements.txt --no-cache-dir

USER 1001