import logging
import boto3
import time
from botocore.exceptions import ClientError

# Set up logging
logging.basicConfig(
    format='%(asctime)s-%(filename)s-%(module)s-%(funcName)s-%(levelname)s:%(message)s',
    filename="logs/personalize-cleanup.log", 
    level="INFO"
)
logging.getLogger().addHandler(logging.StreamHandler())
logger = logging.getLogger(__name__)

def delete_personalize_resources():
    try:
        personalize = boto3.client('personalize')
        
        # Get dataset groups
        dataset_groups = personalize.list_dataset_groups()['datasetGroups']
        
        for group in dataset_groups:
            group_arn = group['datasetGroupArn']
            logger.info(f"Processing dataset group: {group_arn}")
            
            try:
                # Get all campaigns in the dataset group
                paginator = personalize.get_paginator('list_campaigns')
                for page in paginator.paginate():
                    for campaign in page['campaigns']:
                        if campaign['solutionArn'].startswith(group_arn.replace('dataset-group', 'solution')):
                            campaign_arn = campaign['campaignArn']
                            logger.info(f"Deleting campaign: {campaign_arn}")
                            try:
                                personalize.delete_campaign(campaignArn=campaign_arn)
                            except ClientError as e:
                                logger.warning(f"Error deleting campaign {campaign_arn}: {str(e)}")
                
                # Get all solutions
                paginator = personalize.get_paginator('list_solutions')
                for page in paginator.paginate():
                    for solution in page['solutions']:
                        if solution['datasetGroupArn'] == group_arn:
                            solution_arn = solution['solutionArn']
                            logger.info(f"Deleting solution: {solution_arn}")
                            try:
                                personalize.delete_solution(solutionArn=solution_arn)
                            except ClientError as e:
                                logger.warning(f"Error deleting solution {solution_arn}: {str(e)}")
                
                # Get all datasets
                datasets = personalize.list_datasets(datasetGroupArn=group_arn)['datasets']
                for dataset in datasets:
                    dataset_arn = dataset['datasetArn']
                    logger.info(f"Deleting dataset: {dataset_arn}")
                    try:
                        personalize.delete_dataset(datasetArn=dataset_arn)
                    except ClientError as e:
                        logger.warning(f"Error deleting dataset {dataset_arn}: {str(e)}")
                
                # Delete the dataset group
                logger.info(f"Deleting dataset group: {group_arn}")
                personalize.delete_dataset_group(datasetGroupArn=group_arn)
                
            except ClientError as e:
                logger.error(f"Error processing group {group_arn}: {str(e)}")
                continue
        
        # Delete schemas
        schemas = personalize.list_schemas()['schemas']
        for schema in schemas:
            if 'it-service-schema' in schema['name'].lower():
                schema_arn = schema['schemaArn']
                logger.info(f"Deleting schema: {schema_arn}")
                try:
                    personalize.delete_schema(schemaArn=schema_arn)
                except ClientError as e:
                    logger.warning(f"Error deleting schema {schema_arn}: {str(e)}")
                    
        return True
    except Exception as e:
        logger.error(f"Error in delete_personalize_resources: {str(e)}")
        return False

def cleanup_s3_and_iam():
    try:
        # Initialize AWS clients
        s3 = boto3.client('s3')
        iam = boto3.client('iam')
        
        # Get account information
        account_id = boto3.client('sts').get_caller_identity()['Account']
        region = boto3.session.Session().region_name
        bucket_name = f"{account_id}-{region}-it-service-bucket"
        role_name = "PersonalizeITServiceRole"

        # Clean up S3
        logger.info(f"Cleaning up S3 bucket: {bucket_name}")
        try:
            bucket = boto3.resource('s3').Bucket(bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
        except ClientError as e:
            logger.warning(f"Error cleaning up S3 bucket: {str(e)}")

        # Clean up IAM role
        logger.info(f"Cleaning up IAM role: {role_name}")
        try:
            # Detach policies
            policies = [
                'arn:aws:iam::aws:policy/service-role/AmazonPersonalizeFullAccess',
                'arn:aws:iam::aws:policy/AmazonS3FullAccess'
            ]
            for policy_arn in policies:
                try:
                    iam.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchEntity':
                        logger.warning(f"Error detaching policy {policy_arn}: {str(e)}")

            # Delete the role
            try:
                iam.delete_role(RoleName=role_name)
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchEntity':
                    logger.warning(f"Error deleting role: {str(e)}")
                    
        except ClientError as e:
            logger.warning(f"Error cleaning up IAM role: {str(e)}")

        return True
    except Exception as e:
        logger.error(f"Error in cleanup_s3_and_iam: {str(e)}")
        return False

def main():
    logger.info("Starting cleanup process...")
    
    # Delete Personalize resources first
    if delete_personalize_resources():
        logger.info("Successfully cleaned up Personalize resources")
    else:
        logger.warning("Issues encountered while cleaning up Personalize resources")

    # Clean up S3 and IAM resources
    if cleanup_s3_and_iam():
        logger.info("Successfully cleaned up S3 and IAM resources")
    else:
        logger.warning("Issues encountered while cleaning up S3 and IAM resources")
    
    logger.info("Cleanup process completed")

if __name__ == "__main__":
    main()