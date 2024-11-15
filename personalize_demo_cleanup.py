import logging
import boto3
import time
from botocore.exceptions import ClientError

logging.basicConfig(
    format='%(asctime)s-%(levelname)s: %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def wait_for_resource_deletion(personalize, arn, check_method):
    """Wait for resource deletion to complete"""
    max_tries = 20
    tries = 0
    while tries < max_tries:
        try:
            check_method(arn)
            time.sleep(10)
            tries += 1
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                return True
            raise e
    return False

def delete_personalize_resources():
    try:
        personalize = boto3.client('personalize')
        
        # Get all dataset groups
        dataset_groups = personalize.list_dataset_groups()['datasetGroups']
        
        for group in dataset_groups:
            group_arn = group['datasetGroupArn']
            logger.info(f"Processing dataset group: {group_arn}")
            
            # First, get all datasets
            try:
                datasets = personalize.list_datasets(datasetGroupArn=group_arn).get('datasets', [])
                
                # For each dataset, get and delete its import jobs
                for dataset in datasets:
                    dataset_arn = dataset['datasetArn']
                    logger.info(f"Processing dataset: {dataset_arn}")
                    
                    try:
                        import_jobs = personalize.list_dataset_import_jobs(
                            datasetArn=dataset_arn
                        ).get('datasetImportJobs', [])
                        
                        for job in import_jobs:
                            job_arn = job['datasetImportJobArn']
                            logger.info(f"Waiting for import job to complete: {job_arn}")
                            while True:
                                status = personalize.describe_dataset_import_job(
                                    datasetImportJobArn=job_arn
                                )['datasetImportJob']['status']
                                if status in ['ACTIVE', 'CREATE FAILED']:
                                    break
                                time.sleep(10)
                    except Exception as e:
                        logger.warning(f"Error processing import jobs: {str(e)}")
                
                # Now find and delete all campaigns
                try:
                    solutions = personalize.list_solutions(
                        datasetGroupArn=group_arn
                    ).get('solutions', [])
                    
                    for solution in solutions:
                        solution_arn = solution['solutionArn']
                        
                        # Get and delete campaigns for this solution
                        campaigns = personalize.list_campaigns(
                            solutionArn=solution_arn
                        ).get('campaigns', [])
                        
                        for campaign in campaigns:
                            campaign_arn = campaign['campaignArn']
                            logger.info(f"Deleting campaign: {campaign_arn}")
                            try:
                                personalize.delete_campaign(campaignArn=campaign_arn)
                                wait_for_resource_deletion(
                                    personalize, 
                                    campaign_arn,
                                    lambda arn: personalize.describe_campaign(campaignArn=arn)
                                )
                            except Exception as e:
                                logger.warning(f"Error deleting campaign: {str(e)}")
                        
                        # Delete solution versions
                        versions = personalize.list_solution_versions(
                            solutionArn=solution_arn
                        ).get('solutionVersions', [])
                        
                        for version in versions:
                            version_arn = version['solutionVersionArn']
                            logger.info(f"Deleting solution version: {version_arn}")
                            try:
                                personalize.delete_solution_version(solutionVersionArn=version_arn)
                                time.sleep(5)
                            except Exception as e:
                                logger.warning(f"Error deleting solution version: {str(e)}")
                        
                        # Delete the solution
                        logger.info(f"Deleting solution: {solution_arn}")
                        try:
                            personalize.delete_solution(solutionArn=solution_arn)
                            time.sleep(5)
                        except Exception as e:
                            logger.warning(f"Error deleting solution: {str(e)}")
                            
                except Exception as e:
                    logger.warning(f"Error processing solutions and campaigns: {str(e)}")
                
                # Delete datasets
                for dataset in datasets:
                    dataset_arn = dataset['datasetArn']
                    logger.info(f"Deleting dataset: {dataset_arn}")
                    try:
                        personalize.delete_dataset(datasetArn=dataset_arn)
                        time.sleep(5)
                    except Exception as e:
                        logger.warning(f"Error deleting dataset: {str(e)}")
                
                # Delete dataset group
                logger.info(f"Deleting dataset group: {group_arn}")
                try:
                    personalize.delete_dataset_group(datasetGroupArn=group_arn)
                    time.sleep(5)
                except Exception as e:
                    logger.warning(f"Error deleting dataset group: {str(e)}")
                
            except Exception as e:
                logger.error(f"Error processing datasets: {str(e)}")
        
        # Finally, clean up schemas
        try:
            schemas = personalize.list_schemas()['schemas']
            for schema in schemas:
                if 'it-service' in schema['name'].lower():
                    schema_arn = schema['schemaArn']
                    logger.info(f"Deleting schema: {schema_arn}")
                    try:
                        personalize.delete_schema(schemaArn=schema_arn)
                    except Exception as e:
                        logger.warning(f"Error deleting schema: {str(e)}")
        except Exception as e:
            logger.warning(f"Error cleaning up schemas: {str(e)}")
            
        return True
    except Exception as e:
        logger.error(f"Error in delete_personalize_resources: {str(e)}")
        return False

def cleanup_s3_and_iam():
    try:
        # Get account info
        account_id = boto3.client('sts').get_caller_identity()['Account']
        region = boto3.session.Session().region_name
        bucket_name = f"{account_id}-{region}-it-service-bucket"
        role_name = "PersonalizeITServiceRole"

        # Clean up S3
        logger.info(f"Cleaning up S3 bucket: {bucket_name}")
        try:
            s3 = boto3.resource('s3')
            bucket = s3.Bucket(bucket_name)
            bucket.objects.all().delete()
            bucket.delete()
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchBucket':
                logger.warning(f"Error cleaning up S3: {str(e)}")

        # Clean up IAM
        logger.info(f"Cleaning up IAM role: {role_name}")
        try:
            iam = boto3.client('iam')
            for policy_arn in [
                'arn:aws:iam::aws:policy/service-role/AmazonPersonalizeFullAccess',
                'arn:aws:iam::aws:policy/AmazonS3FullAccess'
            ]:
                try:
                    iam.detach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
                    time.sleep(2)
                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchEntity':
                        logger.warning(f"Error detaching policy: {str(e)}")
            
            try:
                iam.delete_role(RoleName=role_name)
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchEntity':
                    logger.warning(f"Error deleting role: {str(e)}")
                    
        except Exception as e:
            logger.warning(f"Error cleaning up IAM: {str(e)}")

        return True
    except Exception as e:
        logger.error(f"Error in cleanup_s3_and_iam: {str(e)}")
        return False

if __name__ == "__main__":
    logger.info("Starting cleanup process...")
    delete_personalize_resources()
    cleanup_s3_and_iam()
    logger.info("Cleanup process completed")