import datetime
import pandas as pd
import boto3
import json
import os
import logging
import sys
import copy # For deep copying task definition parts

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def generate_date_batches(start_date_obj, end_date_obj, batch_size_days):
    """Generates date batches of a specified size."""
    batches = []
    current_batch_start = start_date_obj
    while current_batch_start <= end_date_obj:
        current_batch_end = current_batch_start + datetime.timedelta(days=batch_size_days - 1)
        if current_batch_end > end_date_obj:
            current_batch_end = end_date_obj
        batches.append((current_batch_start, current_batch_end))
        current_batch_start = current_batch_end + datetime.timedelta(days=1)
    return batches

def main():
    logger.info("Starting ECS batch launcher...")

    # --- Environment Variable Configuration ---
    config = {}
    from settings import AWS_GLUE_DB, S3_BUCKET
    todays_date = str(datetime.datetime.now().date())

    # Define environment variables and their defaults.
    # The key is the ENV_VAR_NAME, the value is its default.
    env_var_defaults = {
        "LAUNCHER_START_DATE": "2008-01-01",
        "LAUNCHER_END_DATE": todays_date,
        "LAUNCHER_TYPES": "win,place",
        "LAUNCHER_COUNTRIES": "gb,ire,fr,usa",
        "LAUNCHER_CLUSTER_NAME": "horse-racing-trader",
        "LAUNCHER_SUBNETS": "subnet-7ff27625",
        "LAUNCHER_TASK_DEFINITION_FAMILY": "full-refresh",
        "LAUNCHER_GLUE_DATABASE_NAME": AWS_GLUE_DB,
        "LAUNCHER_S3_BUCKET_NAME": S3_BUCKET,
        "LAUNCHER_TASK_ROLE_ARN": "ecs-service-role",
        "LAUNCHER_BATCH_SIZE_DAYS": "365",
        "LAUNCHER_ASSIGN_PUBLIC_IP": "ENABLED",
        "LAUNCHER_LAUNCH_TYPE": "FARGATE"
    }

    for env_var_name, default_value in env_var_defaults.items():
        config_key = env_var_name.lower().replace('launcher_', '')
        config[config_key] = os.getenv(env_var_name, default_value)

    # Handle LAUNCHER_SECURITY_GROUPS (optional, no default means it's omitted from network config if not set)
    config["security_groups"] = os.getenv("LAUNCHER_SECURITY_GROUPS")

    # Type conversions and validations
    try:
        config["batch_size_days"] = int(config["batch_size_days"])
    except ValueError:
        logger.error(f"Invalid LAUNCHER_BATCH_SIZE_DAYS: {config['batch_size_days']}. Must be an integer.")
        sys.exit(1)

    try:
        start_date_obj = datetime.datetime.strptime(config["start_date"], "%Y-%m-%d").date()
        end_date_obj = datetime.datetime.strptime(config["end_date"], "%Y-%m-%d").date()
    except ValueError as e:
        logger.error(f"Invalid date format for LAUNCHER_START_DATE or LAUNCHER_END_DATE: {e}. Expected YYYY-MM-DD.")
        sys.exit(1)

    if start_date_obj > end_date_obj:
        logger.error(f"LAUNCHER_START_DATE ({config['start_date']}) cannot be after LAUNCHER_END_DATE ({config['end_date']}).")
        sys.exit(1)

    if config["launch_type"].upper() not in ["FARGATE", "EC2"]:
        logger.error(f"Invalid LAUNCHER_LAUNCH_TYPE: {config['launch_type']}. Must be FARGATE or EC2.")
        sys.exit(1)
    config["launch_type"] = config["launch_type"].upper() # Ensure it's uppercase for the API call

    logger.info(f"Launcher configuration loaded: {config}")

    # Initialize AWS clients
    ecs_client = boto3.client('ecs')
    glue_client = boto3.client('glue') 
    s3_client = boto3.client('s3') 

    # --- Delete S3 Data for specified types --- 
    types_to_process = [t.strip().lower() for t in config["types"].split(',')]
    s3_bucket = config["s3_bucket_name"]
    logger.info(f"Attempting to delete S3 data in bucket '{s3_bucket}' for types: {types_to_process}")

    for type_name in types_to_process:
        s3_prefix = f"{type_name}_price_datasets/"
        logger.info(f"Deleting objects under S3 prefix: s3://{s3_bucket}/{s3_prefix}")
        
        try:
            # List objects
            objects_to_delete = []
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        objects_to_delete.append({'Key': obj['Key']})
            
            if objects_to_delete:
                # Delete objects (max 1000 per request)
                for i in range(0, len(objects_to_delete), 1000):
                    chunk = objects_to_delete[i:i + 1000]
                    delete_response = s3_client.delete_objects(
                        Bucket=s3_bucket,
                        Delete={'Objects': chunk, 'Quiet': True}
                    )
                    if 'Errors' in delete_response and delete_response['Errors']:
                        logger.error(f"Errors deleting S3 objects chunk for prefix {s3_prefix}: {delete_response['Errors']}")
                        # Decide if you want to halt or continue if some deletions fail
                logger.info(f"Successfully submitted deletion for {len(objects_to_delete)} objects under S3 prefix: {s3_prefix}")
            else:
                logger.info(f"No objects found under S3 prefix: {s3_prefix} to delete.")

        except Exception as e:
            logger.error(f"Error deleting S3 objects for prefix {s3_prefix}: {e}")
            logger.error("Halting execution as S3 data deletion is a critical pre-step.")
            sys.exit(1)

    # --- Delete Glue Tables for specified types --- 
    glue_database_name = config["glue_database_name"]
    logger.info(f"Attempting to delete Glue tables in database '{glue_database_name}' for types: {types_to_process}")

    for type_name in types_to_process:
        table_name_to_delete = f"betfair_{type_name.lower()}_prices"
        logger.info(f"Attempting to delete Glue table: {glue_database_name}.{table_name_to_delete}")
        try:
            glue_client.delete_table(
                DatabaseName=glue_database_name,
                Name=table_name_to_delete
            )
            logger.info(f"Successfully deleted Glue table: {glue_database_name}.{table_name_to_delete}")
        except glue_client.exceptions.EntityNotFoundException:
            logger.info(f"Glue table {glue_database_name}.{table_name_to_delete} not found. Continuing.")
        except Exception as e: # Catch other potential errors (permissions, etc.)
            logger.error(f"Error deleting Glue table {glue_database_name}.{table_name_to_delete}: {e}")
            logger.error("Halting execution as table deletion is a critical pre-step.")
            sys.exit(1)

    date_batches = generate_date_batches(start_date_obj, end_date_obj, config["batch_size_days"])
    logger.info(f"Generated {len(date_batches)} date batches of up to {config['batch_size_days']} days each.")

    for i, (batch_start, batch_end) in enumerate(date_batches):
        batch_start_str = batch_start.strftime("%Y-%m-%d")
        batch_end_str = batch_end.strftime("%Y-%m-%d")
        logger.info(f"Processing batch {i+1}/{len(date_batches)}: {batch_start_str} to {batch_end_str}")

        # Prepare network configuration
        awsvpc_config = {
            "subnets": config["subnets"].split(','),
            "assignPublicIp": config["assign_public_ip"]
        }
        if config["security_groups"]:
            awsvpc_config["securityGroups"] = config["security_groups"].split(',')
        
        network_config = {"awsvpcConfiguration": awsvpc_config}

        # Prepare environment variable overrides
        # The container name is assumed/known for the given task definition family.
        container_name_to_override = "full-refresh"

        # These are the environment variables we will explicitly set or override.
        # Other environment variables defined in the base task definition will still be present.
        env_overrides_list = [] 

        vars_to_set = {
            "START_DATE": batch_start_str,
            "END_DATE": batch_end_str,
            "TYPES": config["types"],
            "COUNTRIES": config["countries"]
        }

        for name, value in vars_to_set.items():
            env_overrides_list.append({'name': name, 'value': value})
        
        container_override = {
            'name': container_name_to_override, 
            'environment': env_overrides_list
        }

        try:
            response = ecs_client.run_task(
                cluster=config["cluster_name"],
                taskDefinition=config["task_definition_family"],
                count=1,
                platformVersion='LATEST', # For Fargate
                launchType=config["launch_type"],
                networkConfiguration=network_config,
                overrides={
                    'containerOverrides': [container_override]
                },
                propagateTags='TASK_DEFINITION'
            )
            if response.get('tasks') and len(response['tasks']) > 0:
                task_arn = response['tasks'][0]['taskArn']
                logger.info(f"Successfully launched task for batch {batch_start_str}-{batch_end_str}. Task ARN: {task_arn}")
            else:
                logger.warning(f"Launched task for batch {batch_start_str}-{batch_end_str}, but no task ARN returned in response: {response}")
        
        except Exception as e:
            logger.error(f"Failed to launch task for batch {batch_start_str}-{batch_end_str}: {e}")
            # Optionally, decide if you want to stop on error or continue with other batches

    logger.info("All date batches processed.")

if __name__ == '__main__':
    main()
