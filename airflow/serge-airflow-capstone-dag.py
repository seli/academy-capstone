import pendulum
from airflow import DAG

serge_capstone_batch_job = BatchOperator(
    task_id = 'serge_pxl_batch_job',
    job_name = 'serge_pxl_batch_job-' + pendulum.now(),
    job_queue = 'academy-capstone-pxl-2023-job-queue',
    job_definition = 'serge-pxl-capstone',
    overrides = {}
)