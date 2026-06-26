FROM apache/airflow:2.9.0

USER root
RUN mkdir -p /opt/airflow/scripts && chown airflow: /opt/airflow/scripts
USER airflow

# Only install packages missing from the base image (avoid conflicting with Airflow's own deps)
RUN pip install --no-cache-dir \
    faker==40.21.0 \
    matplotlib==3.10.9 \
    seaborn==0.13.2 \
    kafka-python==2.0.2
