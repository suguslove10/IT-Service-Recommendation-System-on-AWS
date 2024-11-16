import logging
import boto3
from personalize.personalize_manager import PersonalizeManager

# Constants
DATA_SET_GROUP_NAME = "it-service-recommendations"
DATA_SET_SCHEMA_NAME = "it-service-schema"
IMPORT_JOB_NAME = "it-service-import-job"
DATASET_BUCKET_NAME = "it-service-bucket"
BUCKET_ROLE_NAME = "PersonalizeITServiceRole"

# Set up logging
logging.basicConfig(format='%(asctime)s-%(levelname)s: %(message)s',
                    filename="logs/personalize-creator.log", level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)

def delete_existing_schema(personalize, schema_name):
    try:
        schemas = personalize.list_schemas()['schemas']
        for schema in schemas:
            if schema['name'] == schema_name:
                try:
                    personalize.delete_schema(schemaArn=schema['schemaArn'])
                    logger.info(f"Deleted existing schema: {schema['schemaArn']}")
                except Exception as e:
                    if 'ResourceInUseException' in str(e):
                        logger.error("Schema is in use by existing datasets. Please run cleanup first.")
                        raise
                    logger.error(f"Error deleting schema: {e}")
                    raise
    except Exception as e:
        logger.error(f"Error checking existing schemas: {e}")
        raise

def main():
    try:
        # Initialize PersonalizeManager
        personalize_manager = PersonalizeManager(
            data_set_group_name=DATA_SET_GROUP_NAME,
            data_set_schema_name=DATA_SET_SCHEMA_NAME,
            import_job_name=IMPORT_JOB_NAME,
            bucket_name=DATASET_BUCKET_NAME,
            role_name=BUCKET_ROLE_NAME
        )

        # Delete existing schema if it exists
        delete_existing_schema(personalize_manager.personalize, DATA_SET_SCHEMA_NAME)

        # Set up and configure resources
        personalize_manager.setup_personalize_datasetgroup()
        personalize_manager.configure_personalize_dataset_group()
        personalize_manager.configure_dataset()
        personalize_manager.configure_s3_interaction_dataset()
        personalize_manager.import_data_set_to_personalize()
        personalize_manager.configure_personalize_solution()
        personalize_manager.create_solution_version()
        personalize_manager.evaluate_solution_version()
        personalize_manager.create_campaing()
        personalize_manager.store_data_manager()

    except Exception as e:
        logger.error(f"Error occurred during Personalize setup: {e}")
        raise

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Setup failed: {e}")