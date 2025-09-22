FROM apache/airflow:2.7.3-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Copy the project files
COPY --chown=airflow:root ./dags /opt/airflow/dags
COPY --chown=airflow:root ./plugins /opt/airflow/plugins
COPY --chown=airflow:root ./sql /opt/airflow/sql