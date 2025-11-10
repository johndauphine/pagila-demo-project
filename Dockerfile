FROM astrocrpublic.azurecr.io/runtime:3.1-3

# Increase worker resources for warehouse-scale streaming
ENV AIRFLOW__CORE__PARALLELISM=16
ENV AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=8
ENV AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=1
