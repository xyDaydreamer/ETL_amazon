ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
ARG AIRFLOW_BASE_IMAGE=apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}
FROM ${AIRFLOW_BASE_IMAGE}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
COPY requirements/airflow.txt /tmp/requirements-airflow.txt
COPY requirements/business.txt /tmp/requirements-business.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    -r /tmp/requirements-airflow.txt \
    -r /tmp/requirements-business.txt

