FROM python:3.7
RUN pip3 install 'apache-airflow' 
RUN airflow initdb 
CMD (airflow scheduler &) && airflow webserver 