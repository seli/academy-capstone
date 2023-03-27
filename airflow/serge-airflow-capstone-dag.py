import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.batch import BatchOperator

serge_capstone_batch_job = BatchOperator(
    task_id = 'serge_pxl_batch_job',
    job_name = 'serge_pxl_batch_job-' + str(pendulum.now()),
    job_queue = 'academy-capstone-pxl-2023-job-queue',
    job_definition = 'serge-pxl-capstone',
    overrides = {}
)