import logging
import boto3
import time
from botocore.exceptions import ClientError
from typing import List, Dict, Optional
from personalize.data_manager import DataManager

class PersonalizeCleanup:
    def __init__(self):
        self.logger = self._setup_logger()
        self.personalize = boto3.client('personalize')
        self.s3 = boto3.resource('s3')
        self.iam = boto3.client('iam')
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        self.region = boto3.session.Session().region_name
        self.data_manager = DataManager().load_data_to_json()

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('PersonalizeCleanup')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def wait_for_resource_status(self, arn: str, describe_method, status_field: str, 
                               target_statuses: List[str], timeout: int = 3600) -> bool:
        """Wait for a resource to reach target status"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                response = describe_method(arn)
                current_status = response.get(status_field, {}).get('status')
                self.logger.info(f"Current status of {arn}: {current_status}")
                
                if current_status in target_statuses:
                    return True
                    
                time.sleep(60)  # Check every minute
            except ClientError as e:
                if 'ResourceNotFoundException' in str(e):
                    return True
                raise e
        return False

    def wait_for_campaign(self, campaign_arn: str) -> bool:
        """Wait for campaign to be in a deletable state"""
        return self.wait_for_resource_status(
            campaign_arn,
            lambda arn: self.personalize.describe_campaign(campaignArn=arn),
            'campaign',
            ['ACTIVE', 'CREATE FAILED']
        )

    def wait_for_solution(self, solution_arn: str) -> bool:
        """Wait for solution to be in a deletable state"""
        return self.wait_for_resource_status(
            solution_arn,
            lambda arn: self.personalize.describe_solution(solutionArn=arn),
            'solution',
            ['ACTIVE', 'CREATE FAILED']
        )

    def delete_campaign_with_retry(self, campaign_arn: str, max_attempts: int = 5) -> bool:
        """Delete campaign with retries"""
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"Attempting to delete campaign (attempt {attempt + 1}): {campaign_arn}")
                
                # Wait for campaign to be in deletable state
                if not self.wait_for_campaign(campaign_arn):
                    self.logger.error(f"Campaign failed to reach deletable state: {campaign_arn}")
                    continue

                self.personalize.delete_campaign(campaignArn=campaign_arn)
                
                # Wait for deletion to complete
                start_time = time.time()
                while time.time() - start_time < 1800:  # 30 minutes timeout
                    try:
                        self.personalize.describe_campaign(campaignArn=campaign_arn)
                        time.sleep(30)
                    except ClientError as e:
                        if 'ResourceNotFoundException' in str(e):
                            self.logger.info(f"Campaign successfully deleted: {campaign_arn}")
                            return True
                        raise e
                        
            except ClientError as e:
                if 'ResourceNotFoundException' in str(e):
                    return True
                elif 'ResourceInUseException' in str(e):
                    self.logger.warning(f"Campaign in use, waiting 2 minutes before retry: {str(e)}")
                    time.sleep(120)
                else:
                    self.logger.error(f"Error deleting campaign: {str(e)}")
            except Exception as e:
                self.logger.error(f"Unexpected error deleting campaign: {str(e)}")
            
            if attempt < max_attempts - 1:
                time.sleep(60)  # Wait between attempts
                
        return False

    def delete_solution_with_retry(self, solution_arn: str, max_attempts: int = 5) -> bool:
        """Delete solution with retries"""
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"Attempting to delete solution (attempt {attempt + 1}): {solution_arn}")
                
                # Wait for solution to be in deletable state
                if not self.wait_for_solution(solution_arn):
                    self.logger.error(f"Solution failed to reach deletable state: {solution_arn}")
                    continue

                self.personalize.delete_solution(solutionArn=solution_arn)
                
                # Wait for deletion to complete
                start_time = time.time()
                while time.time() - start_time < 1800:  # 30 minutes timeout
                    try:
                        self.personalize.describe_solution(solutionArn=solution_arn)
                        time.sleep(30)
                    except ClientError as e:
                        if 'ResourceNotFoundException' in str(e):
                            self.logger.info(f"Solution successfully deleted: {solution_arn}")
                            return True
                        raise e
                        
            except ClientError as e:
                if 'ResourceNotFoundException' in str(e):
                    return True
                elif 'ResourceInUseException' in str(e):
                    self.logger.warning(f"Solution in use, waiting 2 minutes before retry: {str(e)}")
                    time.sleep(120)
                else:
                    self.logger.error(f"Error deleting solution: {str(e)}")
            except Exception as e:
                self.logger.error(f"Unexpected error deleting solution: {str(e)}")
            
            if attempt < max_attempts - 1:
                time.sleep(60)  # Wait between attempts
                
        return False

    def cleanup_all(self) -> None:
        """Main cleanup method with improved error handling and retries"""
        self.logger.info("Starting cleanup process...")
        
        try:
            # Delete campaign first
            if hasattr(self.data_manager, 'campaign_arn') and self.data_manager.campaign_arn:
                campaign_deleted = self.delete_campaign_with_retry(self.data_manager.campaign_arn)
                if not campaign_deleted:
                    self.logger.error("Failed to delete campaign after all retries")
                time.sleep(60)

            # Delete solution
            if hasattr(self.data_manager, 'solution_arn') and self.data_manager.solution_arn:
                solution_deleted = self.delete_solution_with_retry(self.data_manager.solution_arn)
                if not solution_deleted:
                    self.logger.error("Failed to delete solution after all retries")
                time.sleep(60)

            # Delete dataset
            if hasattr(self.data_manager, 'interactions_dataset_arn') and self.data_manager.interactions_dataset_arn:
                try:
                    self.logger.info(f"Deleting dataset: {self.data_manager.interactions_dataset_arn}")
                    self.personalize.delete_dataset(
                        datasetArn=self.data_manager.interactions_dataset_arn
                    )
                    time.sleep(60)
                except ClientError as e:
                    self.logger.error(f"Error deleting dataset: {str(e)}")

            # Delete schema
            if hasattr(self.data_manager, 'schema_arn') and self.data_manager.schema_arn:
                try:
                    self.logger.info(f"Deleting schema: {self.data_manager.schema_arn}")
                    self.personalize.delete_schema(schemaArn=self.data_manager.schema_arn)
                    time.sleep(30)
                except ClientError as e:
                    self.logger.error(f"Error deleting schema: {str(e)}")

            # Delete dataset group
            if hasattr(self.data_manager, 'dataset_group_arn') and self.data_manager.dataset_group_arn:
                try:
                    self.logger.info(f"Deleting dataset group: {self.data_manager.dataset_group_arn}")
                    self.personalize.delete_dataset_group(
                        datasetGroupArn=self.data_manager.dataset_group_arn
                    )
                except ClientError as e:
                    self.logger.error(f"Error deleting dataset group: {str(e)}")

            # Cleanup S3 and IAM
            self.cleanup_s3_resources()
            self.cleanup_iam_resources()

            self.logger.info("Cleanup process completed")
            
        except Exception as e:
            self.logger.error(f"Error in cleanup process: {str(e)}")
            raise

    def cleanup_s3_resources(self) -> None:
        """Clean up S3 bucket and contents"""
        try:
            bucket_name = f"{self.account_id}-{self.region}-it-service-bucket"
            bucket = self.s3.Bucket(bucket_name)
            
            self.logger.info(f"Cleaning up S3 bucket: {bucket_name}")
            bucket.objects.all().delete()
            bucket.delete()
            
        except ClientError as e:
            if e.response['Error']['Code'] != 'NoSuchBucket':
                self.logger.error(f"Error cleaning up S3: {str(e)}")

    def cleanup_iam_resources(self) -> None:
        """Clean up IAM roles and policies"""
        try:
            role_name = "PersonalizeITServiceRole"
            
            # Detach policies
            for policy_arn in [
                'arn:aws:iam::aws:policy/service-role/AmazonPersonalizeFullAccess',
                'arn:aws:iam::aws:policy/AmazonS3FullAccess'
            ]:
                try:
                    self.iam.detach_role_policy(
                        RoleName=role_name,
                        PolicyArn=policy_arn
                    )
                except ClientError as e:
                    if e.response['Error']['Code'] != 'NoSuchEntity':
                        self.logger.error(f"Error detaching policy: {str(e)}")

            # Delete the role
            try:
                self.iam.delete_role(RoleName=role_name)
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchEntity':
                    self.logger.error(f"Error deleting role: {str(e)}")
                    
        except Exception as e:
            self.logger.error(f"Error cleaning up IAM resources: {str(e)}")

if __name__ == "__main__":
    cleanup = PersonalizeCleanup()
    cleanup.cleanup_all()