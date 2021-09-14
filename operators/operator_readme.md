# Starburst Operators

## Starburst Operator

The Starburst operator is responsible for querying the Starburst Enterprise server and can only be used to execute queries against the server.


## Starburst S3 Operator

The Starburst S3 operator is responsible for executing queries against the Starburst server and subsequentally download the response query to the host machine. This operator is solely used for selecting larger queries and automates the process of CTAS statements. 




# Using the Starburst S3 Operator with a Dockerfile

## Requirements
    1. Docker 
    2. Starburst Server with Hive connector 
    3. Amazon S3 Credentials 
    
## Initializing the environment

As per Airflow instructions located: <https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html>
1. Download the airflow docker-compose file `curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.1.3/docker-compose.yaml'`
2. Create required directories : `mkdir -p ./dags ./logs ./plugins ./queries`
3. Create Docker environment variable file `echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env`
4. Mount the queries directory in your docker-compose.yaml `./queries:/opt/airflow/queries`
    

For the initial run you must first initialize a fresh instance of airflow via `docker-compose up airflow-init`

Once completed you should see a similar message

    airflow-init_1       | Upgrades done
    airflow-init_1       | Admin user airflow created
    airflow-init_1       | 2.1.3
    start_airflow-init_1 exited with code 0
    
A default account has been created along with credentials

    login : airflow
    password : airflow

To start all services run `docker-compose up`


Once the services are started, you can add the Starburst and S3 Connections.

For the S3 Connection, if using temporary credentials you must pass the connection via the session key, secret, and token.

    Conn ID : aws_default
    Login : aws_access_key_id
    Password : aws_secret_access_key
    Extra : { 
                'aws_session_token' : "AWS_SESSION_TOKEN",
                'region'            : "AWS_REGION" 
            }
            
If using the example_aws dag, you should set the query_location, s3_location, and sql executed

    query_location : Location for query to be stored 
    s3_location : Location to store the query in S3
    sql : Select statement to be executed
    

Note: The query_location variable should be set to the same location as the mounted volume `/opt/airflow/queries`
