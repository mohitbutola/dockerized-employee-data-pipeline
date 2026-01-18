FROM apache/spark:3.5.0

USER root

RUN curl -L -o /opt/spark/jars/postgresql-42.7.3.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

RUN mkdir -p /opt/spark-app

COPY employee_cleaning.py /opt/spark-app/employee_cleaning.py

ENTRYPOINT ["/opt/spark/bin/spark-submit","--master","local[*]","/opt/spark-app/employee_cleaning.py"]
