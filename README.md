# Olist Data Warehouse Project

## Overview
This project implements a comprehensive data pipeline for the Olist e-commerce platform. It extracts raw data from CSV files, preprocesses it, stages it in BigQuery, and then transforms it into a production-ready data warehouse using a star schema model. The project includes a Looker dashboard that displays key performance indicators (KPIs) and visuals to gain insights from the data. The entire pipeline is containerized using Docker and orchestrated with Apache Airflow for automated, scheduled runs.

## Project Structure
The project consists of several key components:
```
Olist_Datawarehouse/
│
├── dags/                          # Directory for Airflow DAGs (workflows)
│   └── etl_dag.py                 # Main DAG for the ETL workflow
│
├── schemas/                       # SQL scripts for data modeling (star schema)
│   ├── customers_dim.sql          # SQL script for Customers Dimension table
│   ├── date_dim.sql               # SQL script for Date Dimension table
│   ├── orders_fact.sql            # SQL script for Orders Fact table
│   ├── products_dim.sql           # SQL script for Products Dimension table
│   └── sellers_dim.sql            # SQL script for Sellers Dimension table
│
├── scripts/                       # Python scripts for data processing and pipelines
│   ├── preprocess.py              # Script for preprocessing data before loading
│   ├── production_pipeline.py     # Production ETL pipeline script
│   └── staging_pipeline.py        # Staging ETL pipeline script
│
├── .dockerignore                  # Files and directories ignored by Docker
├── .gitignore                     # Files and directories ignored by Git
├── Dockerfile                     # Dockerfile to containerize the project
├── README.md                      # Project documentation (this file)
├── docker-compose.yaml            # Docker Compose configuration for setting up the environment
├── requirements.txt               # Python dependencies for the project
```

1. **Preprocessing**: `preprocess.py` script cleans and prepares the raw data.
2. **Staging Pipeline**: `staging_pipeline.py` moves preprocessed data to a staging area in BigQuery.
3. **Data Modeling**: SQL files in `shemas/` define dimension and fact table schemas for a star schema model.
4. **Production Pipeline**: `production_pipeline.py` executes the schemas and populates the production data warehouse in BigQuery.
5. **Data Visualization**: A Looker dashboard connected to the BigQuery data warehouse for data visualization and insights.
6. **Workflow Orchestration**: Airflow DAG `dags/etl_dag.py` to manage and schedule the execution of pipeline scripts.
7. **Containerization**: Dockerfile and docker-compose.yaml for easy deployment and scalability.

## Key Features
- End-to-end data pipeline from raw CSV files to a structured data warehouse
- Data preprocessing for improved data quality
- Staging area in BigQuery for intermediate data storage
- Star schema data modeling for efficient querying and analysis
- Production-grade data warehouse in BigQuery
- Looker dashboard for real-time data visualization and KPI tracking
- Docker containerization for consistent environments and easy deployment
- Apache Airflow for workflow orchestration and scheduling


## Installation and Setup

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/olist-datawarehouse.git
   cd olist-datawarehouse
   ```

2. Set up Google Cloud credentials:
   - Create a service account and download the JSON key file
   - Place the key file in the project directory and rename it to `service-account-file.json`

3. Configure environment variables:
   - Install dependencies `pip install -r requirements.txt`
   - Setup `.env` and fill in the necessary variables like dataset id as `PROJECT_ID`, staging dataset id as `DATASET_ID`, production dataset id `PROD_DATASET_ID`

5. Download dataset from kaggle
   - Name the dataset as `rawOlist`


## Data Model
This project implements a star schema with 4 dimension and 1 fact table.



## Airflow DAG
The Airflow DAG (`etl_dag.py`) orchestrates the following tasks in order:
1. Preprocess raw data
2. Load data to staging area
3. Execute data modelling schemas for production data warehouse

## Looker Dashboard
Dashboard can be accessed at: [Link](https://lookerstudio.google.com/s/rwDQIflFSEc)


## Usage with Docker

For this project I used two approaches:

### Approach 1: Standard Dockerfile

Before taking this approach move python scripts from `/scripts` to project directory so that it can access data, credentials and environment variables easily. 
This approach uses a standard Dockerfile to create an image for running the data pipeline scripts.

1. Build the Docker image:
   ```
   docker build -t olist-datawarehouse .
   ```

2. Run the container:
   ```
   docker run -d --name olist-pipeline olist-datawarehouse
   ```

### Approach 2: Airflow with Docker Compose

This approach uses the official Airflow docker-compose setup, which provides a full Airflow environment including the webserver, scheduler, and workers.

1. Initialize the Airflow database:
   ```
   docker compose up airflow-init
   ```

2. Start the Airflow services:
   ```
   docker compose up -d
   ```
   
3. Access the Airflow web interface at `http://localhost:8080`


4. Enable the Olist pipeline DAG.
5. The pipeline will run according to the schedule defined in the DAG, or you can trigger it manually from the Airflow UI.

To stop and remove the containers, networks, and volumes:
```
docker compose down --volumes --rmi all
```

For more detailed information on the Airflow docker-compose setup, refer to the [official Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

PS: After step 2 I was not able to complete the second approach because of my machine's limitations. But I hope that the remaining steps will work properly.
