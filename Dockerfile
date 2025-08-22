FROM apache/airflow:3.0.4

COPY requirements.txt /requirements.txt

RUN python -m pip install --no-cache-dir --upgrade pip
RUN python -m pip install --no-cache-dir -r /requirements.txt