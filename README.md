# H&M Recommender System

This project contains the code I used for the H&M Personalized Fashion Recommendations Kaggle competition. The aim of
the competition is to build a recommendation engine capable of predicting H&M clients future purchases based on their
profile information (age, postal code, etc.), transactions history and articles metadata (product type, description,
etc.). For further information, refer to the official competition's home page
on [kaggle](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/).

# 1. Project Description

The current project workflow is divided into multiple stages.

## 1.1 Data Preprocessing & Partitioning

This step includes enforcing data frames data types, dealing with missing values, and converting raw csv files to
parquet. The resultant parquet files are partitioned either by customer or by articles groups to allow loading and processing
smaller chunks of data in a parallel fashion during the following pipeline steps.

## 1.2 Features Generation

Aside from the static metadata provided by the competition host, I generated a set of dynamic features for both
customers and articles via the aggregation of transactions data. An example of features for customers would be the
average price of their purchased articles.

## 1.3. Candidates Generation & Embedding

In the current competition's context, candidates represents a set of articles that a specific customer might purchase in the
future. For computational resources considerations, I opted for a non-personalized candidates generation scheme, meaning
that the candidates are the same for all the customers regardless of their previous purchases. These candidates are
obtained by retrieving the top-N trending articles during the last two weeks preceding the competition evaluation week.
Each (customer, articles) pair in the candidates' data set is embedded using articles and customers features (dynamic &
static) as well as a set of additional features linking a given customer to its candidate. An example of these features
would be the number of products having the same type of the candidates and purchased by the customer.

## 1.4. Model Training & Validation

The (base) model I used for this competition, is an [XGBoost](https://xgboost.readthedocs.io/en/stable/) Ranker model.
The candidates used be to train and validate the model are based on the full transactions' history except for the last week.
The last week in the data wil be used in the inference step. Due to computational resources limitations, I opted for a simple
Grouped K-folds validation split, in which 80% of customers are used for training and 20% for validation.

## 1.5. Recommendations Generation

During This phase the fitted model is used to score and then rank the candidates obtained with the full transactions' history. 
For each customer in the dataset, only the top-12 scoring candidates were kept in the submission file as required by the competition host.
Note that the final submission was obtained by blending several recommendations obtained via multiple ranking models fitted/validated 
using different hyperparameters and combinations of the generated features.

# 2. Project Structure 
```
.
├── recsys   
│  ├── dataloaders     # package for handling data partitions          
│  ├── engine          # recommender system core package   
│  │   ├── candidates  # candidates generation package 
│  │   ├── features    # features generation package
│  │   ├── models      # models versionning package
│  │   ├── validation  # recommendations validation package
│  │   submission.py   # submission utilties    
│
├── scripts            # project pipeline
├── workflow           # airflow workflow directory 
│   
├── setup              # resys package setup
├── requirements       # requirements
├── Dockerfile         # dockerfile for building the project image
├── docker-compose.ym  # docker-compose file for monitoring the project workflow with airflow
├── .env               # environment variables
├── README.md          # Documentation
└── 
```

# 3. Project Environment

There are two options for setting up the right environment for the current project. One option is to create a virtual
python environment using [venv](https://docs.python.org/3/library/venv.html)
and install the dependencies.

Open a terminal in the project's root directory and type the following commands:

``` bash
$ python3 -m venv hm_recsys_env
$ source /hm_recsys_env/bin/activate
$ pip3 install -r requirements.txt
```

The second option is to build a [docker](https://www.docker.com/) image, copy the source code and install the required
packages within it. If you do not have docker installed on your machine refer to docker's official
installation [guide](https://docs.docker.com/get-docker/).

Once you have docker installed, Open a terminal in the project's root directory and type the following commands:

``` bash
$ sudo service docker start
$ docker build -f Dockerfile --tag hm_recys_image
```

# 4. Running The Project

There are three options to start the project's workflow.

First, you have to set two environment variables DATA_DIRECTORY and MODELS_DIRECTORY in your machine environment.
DATA_DIRECTORY is the absolute path to the directory that will host the pipeline raw and processed data, whereas
MODELS_DIRECTORY represents a folder in which the fitted model will be stored on your machine. Open A terminal and type
the following commands:

````bash
$ export DATA_DIRECTORY=/path/to/competition/data
$ export MODELS_DIRECTORY=/path/to/models/directory
$ cd $DATA_DIRECTORY && mkdir raw
````

Once the environment variables are set, place the competition raw data files (*.csv) in the $DATA_DIRECTORY/raw folder.

### 4.1. Running with a python virtual environment

If you installed the project dependencies in a python virtual environment as described in the previous section, you can
run the pipeline within it. Under the project's root directory open a terminal and type the following commands:

````bash
$ source /hm_recsys_env/bin/activate 
$ python3 ./scripts/run.py
````

### 4.2. Running with docker

If you installed the project dependencies in a docker image as described in the previous section, you can run the
pipeline as a container on top of that image. Under the project's root directory open a terminal and run the following
commands:

````bash
$ docker run --rm -d --name hm_recsys_pipeline -v ${DATA_DIRECOTRY}:/HMRecommenderSystem/data -v ${MODELS_DIRECOTRY}:/HMRecommenderSystem/models hm_recys_image python3 ./scripts/run.py
````

To check the status of the container pipeline type the following:

````bash
$ docker ps
````

To visualize the container resources usage summary, type the following:

````bash
$ docker stats hm_recsys_pipeline
````

to visualize the container's logs, type the following:

````bash
$ docker logs hm_recsys_pipeline
````

### 4.3. Running with docker-compose

Running the project with this option is highly recommended as you can interact, visualize, pause and run the project's
pipeline via a web interface. The project's workflow is handled with [airflow](https://airflow.apache.org/). All the
services and components needed for running the airflow UI are automatically handled for you.

First set the DATA_DIRECTORY and MODELS_DIRECTORY environment variables in the .env file. Second, install docker-compose
on your machine. Please refer to the docker-compose official
installation [guide](https://docs.docker.com/compose/install/) for more details.

Once docker-compose is installed and environment variables are set, open a terminal under the project's root directory
and run the following commands:

````bash
$ docker-compose up
````

Note that the build will take several minutes. Open a web browser and type localhost:8080. This will redirect you to the
airflow login page. the default username and password are airflow and airflow respectively. Once you're logged in you
can visualize the different steps, start and pause the pipeline. For more details about using the airflow webserver UI
please refer to the official documentation
[website](https://airflow.apache.org/docs/apache-airflow/stable/ui.html).
