FROM python:3.12.2

RUN apt-get install wget
RUN pip install pandas psycopg2 pyarrow sqlalchemy

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]