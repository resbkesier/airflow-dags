from datetime import datetime, timedelta

from airflow.models import DAG, BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.data_factory import (
    AzureDataFactoryRunPipelineOperator,
)
from airflow.providers.microsoft.azure.sensors.data_factory import (
    AzureDataFactoryPipelineRunStatusSensor,
)
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="example",
    start_date=datetime(2023, 8, 17),
    schedule="* * * * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        "azure_data_factory_conn_id": "azure_datafactory",
        "factory_name": "dev-knfftg",
        "resource_group_name": "arcelorMittal",
    },
    default_view="graph",
) as dag:
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")
    with TaskGroup(group_id="group1") as group1pipeline:
        run_group1_pipeline1 = AzureDataFactoryRunPipelineOperator(
            task_id="run_group1_pipeline1", pipeline_name="pipeline1"
        )
        run_group1_pipeline2 = AzureDataFactoryRunPipelineOperator(
            task_id="run_group1_pipeline2", pipeline_name="pipeline2"
        )
        run_group1_pipeline1 >> run_group1_pipeline2

    with TaskGroup(group_id="group2") as group2pipeline:
        run_group2_pipeline1 = AzureDataFactoryRunPipelineOperator(
            task_id="run_group2_pipeline1", pipeline_name="pipeline3"
        )
        run_group2_pipeline2 = AzureDataFactoryRunPipelineOperator(
            task_id="run_group2_pipeline2",
            pipeline_name="pipeline4",
            wait_for_termination=False,
        )
        pipeline_run_sensor: BaseOperator = AzureDataFactoryPipelineRunStatusSensor(
            task_id="pipeline_run_sensor",
            run_id=run_group2_pipeline2.output["run_id"],
            poke_interval=3,
        )
        [run_group1_pipeline1, pipeline_run_sensor]

    begin >> [group1pipeline, group2pipeline] >> end