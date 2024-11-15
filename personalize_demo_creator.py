import logging
from personalize.personalize_manager import PersonalizeManager
from personalize.data_manager import DataManager

# Constants for Personalize setup
DATA_SET_GROUP_NAME = "it-service-recommendations"
DATA_SET_SCHEMA_NAME = "it-service-schema"
IMPORT_JOB_NAME = "it-service-import-job"
DATASET_BUCKET_NAME = "it-service-bucket"
BUCKET_ROLE_NAME = "PersonalizeITServiceRole"

# Set up logging
logging.basicConfig(format='%(asctime)s-%(filename)s-%(module)s-%(funcName)s-%(levelname)s:%(message)s',
                    filename="logs/personalize-creator.log", level="INFO")
logging.getLogger().addHandler(logging.StreamHandler())

# Initialize the PersonalizeManager with the required parameters
personalize_manager = PersonalizeManager(
    data_set_group_name=DATA_SET_GROUP_NAME,
    data_set_schema_name=DATA_SET_SCHEMA_NAME,
    import_job_name=IMPORT_JOB_NAME,
    bucket_name=DATASET_BUCKET_NAME,
    role_name=BUCKET_ROLE_NAME
)

# Set up and configure the Personalize dataset group and related resources
try:
    personalize_manager.setup_personalize_datasetgroup()
    personalize_manager.configure_personalize_dataset_group()
    personalize_manager.configure_dataset()
    personalize_manager.configure_s3_interaction_dataset()
    personalize_manager.import_data_set_to_personalize()

    # Configure solution and create a version of it
    personalize_manager.configure_personalize_solution()
    personalize_manager.create_solution_version()

    # Evaluate the solution version
    personalize_manager.evaluate_solution_version()

    # Create a campaign for the solution version
    personalize_manager.create_campaing()

    # Store the data manager for later use (this step may vary based on your setup)
    personalize_manager.store_data_manager()

except Exception as e:
    logging.error(f"Error occurred during Personalize setup: {e}")
