import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from docker.types import Mount

DATA_DIRECTORY = os.environ.get("DATA_DIRECTORY")
MODELS_DIRECTORY = os.environ.get("MODELS_DIRECTORY")

DATA_DIRECTORY_MNT = Mount(target='/HMRecommenderSystem/data',
                           source=DATA_DIRECTORY,
                           type='bind')

MODELS_DIRECTORY_MNT = Mount(target='/HMRecommenderSystem/models',
                             source=MODELS_DIRECTORY,
                             type='bind')

RECSYS_PIPELINE_IMAGE_NAME = "hmrecommendersystem_recsys_pipeline"
DOCKER_URL = "unix://var/run/docker.sock"
AUTO_REMOVE_CONTAINERS = True

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='recsys_pipeline',
         start_date=datetime(2016, 1, 1),
         default_args=default_args,
         description='H&M Recommender System Pipeline',
         schedule_interval=None,
         ) as dag:
    start_pipeline = DummyOperator(task_id='start_pipeline')

    with TaskGroup("data_preprocessing") as preprocess:
        preprocess_data = DockerOperator(
            task_id='preprocess',
            image=RECSYS_PIPELINE_IMAGE_NAME,
            mounts=[DATA_DIRECTORY_MNT, MODELS_DIRECTORY_MNT],
            docker_url=DOCKER_URL,
            auto_remove=AUTO_REMOVE_CONTAINERS,
            command='python3 scripts/preprocess.py',
        )

        repartition_data = DockerOperator(
            task_id='repartition',
            image=RECSYS_PIPELINE_IMAGE_NAME,
            mounts=[DATA_DIRECTORY_MNT, MODELS_DIRECTORY_MNT],
            docker_url=DOCKER_URL,
            auto_remove=AUTO_REMOVE_CONTAINERS,
            command='python3 scripts/repartition.py',
        )

    with TaskGroup("features_generation") as generate_features:
        compute_articles_features = DockerOperator(
            task_id='compute_articles_features',
            image=RECSYS_PIPELINE_IMAGE_NAME,
            mounts=[DATA_DIRECTORY_MNT, MODELS_DIRECTORY_MNT],
            docker_url=DOCKER_URL,
            auto_remove=AUTO_REMOVE_CONTAINERS,
            command='python3 scripts/compute_articles_features.py',
        )

        compute_customers_features = DockerOperator(
            task_id='compute_customers_features',
            image=RECSYS_PIPELINE_IMAGE_NAME,
            mounts=[DATA_DIRECTORY_MNT, MODELS_DIRECTORY_MNT],
            docker_url=DOCKER_URL,
            auto_remove=AUTO_REMOVE_CONTAINERS,
            command='python3 scripts/compute_customers_features.py',
        )

    with TaskGroup("candidates_generation") as candidates_generation:
        compute_train_candidates = DockerOperator(
            task_id='compute_train_candidates',
            image=RECSYS_PIPELINE_IMAGE_NAME,
            mounts=[DATA_DIRECTORY_MNT, MODELS_DIRECTORY_MNT],
            docker_url=DOCKER_URL,
            auto_remove=AUTO_REMOVE_CONTAINERS,
            command='python3 scripts/compute_train_candidates.py',
        )

        compute_inference_candidates = DockerOperator(
            task_id='compute_inference_candidates',
            image=RECSYS_PIPELINE_IMAGE_NAME,
            mounts=[DATA_DIRECTORY_MNT, MODELS_DIRECTORY_MNT],
            docker_url=DOCKER_URL,
            auto_remove=AUTO_REMOVE_CONTAINERS,
            command='python3 scripts/compute_inference_candidates.py',
        )

    with TaskGroup("model_training_and_validation") as model_training_and_validation:
        fit_model = DockerOperator(
            task_id='fit_model',
            image=RECSYS_PIPELINE_IMAGE_NAME,
            mounts=[DATA_DIRECTORY_MNT, MODELS_DIRECTORY_MNT],
            docker_url=DOCKER_URL,
            auto_remove=AUTO_REMOVE_CONTAINERS,
            command='python3 scripts/fit.py',
        )

    with TaskGroup("recommendations_generation") as recommendations_generation:
        predict_recommendations = DockerOperator(
            task_id='predict_recommendations',
            image=RECSYS_PIPELINE_IMAGE_NAME,
            mounts=[DATA_DIRECTORY_MNT, MODELS_DIRECTORY_MNT],
            docker_url=DOCKER_URL,
            auto_remove=AUTO_REMOVE_CONTAINERS,
            command='python3 scripts/predict.py',
        )

        submit_recommendations = DockerOperator(
            task_id='submit_recommendations',
            image=RECSYS_PIPELINE_IMAGE_NAME,
            mounts=[DATA_DIRECTORY_MNT, MODELS_DIRECTORY_MNT],
            docker_url=DOCKER_URL,
            auto_remove=AUTO_REMOVE_CONTAINERS,
            command='python3 scripts/submit.py',
        )

    end_pipeline = DummyOperator(task_id="end_pipeline")

    start_pipeline.set_downstream(preprocess_data)
    preprocess_data.set_downstream(repartition_data)
    repartition_data.set_downstream(compute_articles_features)
    compute_articles_features.set_downstream(compute_customers_features)
    compute_customers_features.set_downstream(compute_train_candidates)
    compute_train_candidates.set_downstream(compute_inference_candidates)
    compute_inference_candidates.set_downstream(fit_model)
    fit_model.set_downstream(predict_recommendations)
    predict_recommendations.set_downstream(submit_recommendations)
    submit_recommendations.set_downstream(end_pipeline)
