import os
import pandas as pd
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

class SyntheticDataHandler:
    def __init__(self):
        self.data_directory = "dataset"
        self.interactions_filename = "interactions.csv"
        self.source_filename = "aws_synthetic_service_recommendation_data.csv"
        self.mapping_filename = "service_mapping.csv"
        self.interactions_df = None
        self.logger = logging.getLogger(__name__)
        self._setup_logging()

    def _setup_logging(self):
        """Configure logging for the handler"""
        if not self.logger.handlers:
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _create_service_mapping(self, services: list) -> dict:
        """Create a mapping between service names and IDs"""
        # Create a consistent mapping that preserves service names
        service_mapping = {}
        for idx, service in enumerate(sorted(services)):
            clean_name = service.strip()
            service_mapping[clean_name] = str(idx)
            
        # Log the mapping for verification
        self.logger.info(f"Created service mapping: {service_mapping}")
        return service_mapping

    def _validate_source_data(self, df: pd.DataFrame) -> None:
        """Validate source data format and content"""
        required_columns = {'User ID', 'AWS Service', 'Interaction Type', 'Rating'}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"Source data missing required columns: {missing_columns}")

        if df['AWS Service'].nunique() < 2:
            raise ValueError("Source data must contain at least 2 unique services")

        if df['User ID'].nunique() < 1:
            raise ValueError("Source data must contain at least 1 user")

    def setup_datasource_data(self):
        """Set up the data source with proper validation"""
        try:
            if not os.path.isdir(self.data_directory):
                os.makedirs(self.data_directory)
                self.logger.info(f"Created data directory: {self.data_directory}")
                
            source_file = os.path.join(self.data_directory, self.source_filename)
            if not os.path.exists(source_file):
                raise FileNotFoundError(
                    f"Source data file {self.source_filename} not found in {self.data_directory}"
                )
            
            # Validate file size
            if os.path.getsize(source_file) == 0:
                raise ValueError(f"Source file {self.source_filename} is empty")
            
            self.logger.info(f"Successfully verified source data file: {source_file}")
            print(f"Successfully verified source data file: {source_file}")
            
        except Exception as e:
            self.logger.error(f"Error in setup_datasource_data: {str(e)}")
            raise

    def prepare_dataset(self):
        """Prepare the dataset with enhanced validation and error handling"""
        try:
            source_file = os.path.join(self.data_directory, self.source_filename)
            self.logger.info(f"Reading source data from: {source_file}")
            print(f"Reading source data from: {source_file}")
            
            # Read and validate source data
            service_data = pd.read_csv(source_file)
            self._validate_source_data(service_data)
            
            # Create a copy for processing
            self.interactions_df = service_data.copy()
            
            # Create and save service mapping
            unique_services = sorted(service_data['AWS Service'].unique())
            service_mapping = self._create_service_mapping(unique_services)
            
            mapping_df = pd.DataFrame([
                {'ServiceName': service, 'ServiceID': service_id}
                for service, service_id in service_mapping.items()
            ])
            mapping_path = os.path.join(self.data_directory, self.mapping_filename)
            mapping_df.to_csv(mapping_path, index=False)
            self.logger.info(f"Saved service mapping with {len(unique_services)} services to {mapping_path}")
            print(f"Saved service mapping with {len(unique_services)} services")
            
            # Transform the data
            self.interactions_df['USER_ID'] = self.interactions_df['User ID'].astype(str)
            self.interactions_df['ITEM_ID'] = self.interactions_df['AWS Service'].map(service_mapping).astype(str)
            self.interactions_df['EVENT_TYPE'] = self.interactions_df['Interaction Type'].str.lower()
            
            # Add timestamps
            current_time = int(time.time())
            self.interactions_df['TIMESTAMP'] = range(
                current_time,
                current_time + len(self.interactions_df)
            )
            
            # Select and order required columns
            self.interactions_df = self.interactions_df[[
                'USER_ID',
                'ITEM_ID',
                'EVENT_TYPE',
                'TIMESTAMP'
            ]]
            
            # Validate the prepared dataset
            self._validate_prepared_data()
            
            self.logger.info(f"Successfully prepared dataset with shape: {self.interactions_df.shape}")
            print(f"Successfully prepared dataset with shape: {self.interactions_df.shape}")
            
        except Exception as e:
            self.logger.error(f"Error preparing dataset: {str(e)}")
            self.logger.error(f"Current working directory: {os.getcwd()}")
            print(f"Error preparing dataset: {str(e)}")
            print(f"Current working directory: {os.getcwd()}")
            raise

    def _validate_prepared_data(self):
        """Validate the processed dataset"""
        if self.interactions_df is None:
            raise ValueError("No data loaded")
            
        required_columns = ['USER_ID', 'ITEM_ID', 'EVENT_TYPE', 'TIMESTAMP']
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in self.interactions_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Validate data types
        expected_types = {
            'USER_ID': 'string',
            'ITEM_ID': 'string',
            'EVENT_TYPE': 'string',
            'TIMESTAMP': 'int64'
        }
        
        for col, expected_type in expected_types.items():
            actual_type = str(self.interactions_df[col].dtype)
            if actual_type != expected_type:
                self.logger.warning(f"Column {col} has type {actual_type}, converting to {expected_type}")
                if expected_type == 'string':
                    self.interactions_df[col] = self.interactions_df[col].astype(str)
                elif expected_type == 'int64':
                    self.interactions_df[col] = self.interactions_df[col].astype('int64')
        
        # Check for null values
        null_counts = self.interactions_df[required_columns].isnull().sum()
        if null_counts.sum() > 0:
            raise ValueError(f"Found null values in required columns: {null_counts[null_counts > 0]}")
        
        # Verify minimum requirements
        n_users = self.interactions_df['USER_ID'].nunique()
        n_items = self.interactions_df['ITEM_ID'].nunique()
        n_interactions = len(self.interactions_df)
        
        if n_users < 2:
            raise ValueError("Dataset must have at least 2 unique users")
        if n_items < 2:
            raise ValueError("Dataset must have at least 2 unique items")
            
        self.logger.info(f"Data statistics: {n_users} users, {n_items} items, {n_interactions} interactions")
        print(f"Data statistics: {n_users} users, {n_items} items, {n_interactions} interactions")

    def write_data_set(self):
        """Write the processed dataset to file with backup functionality"""
        if self.interactions_df is None:
            raise ValueError("No data prepared to write")
            
        try:
            output_path = os.path.join(self.data_directory, self.interactions_filename)
            
            # Create backup if file exists
            if os.path.exists(output_path):
                backup_path = f"{output_path}.bak.{int(time.time())}"
                os.rename(output_path, backup_path)
                self.logger.info(f"Created backup of existing file: {backup_path}")
                print(f"Created backup of existing file: {backup_path}")
            
            # Write the new file
            self.interactions_df.to_csv(output_path, index=False, float_format='%.0f')
            
            # Verify the written file
            if not os.path.exists(output_path):
                raise IOError(f"Failed to write dataset to {output_path}")
            
            if os.path.getsize(output_path) == 0:
                raise IOError(f"Written file is empty: {output_path}")
                
            self.logger.info(f"Successfully wrote dataset to: {output_path}")
            print(f"Successfully wrote dataset to: {output_path}")
            
        except Exception as e:
            self.logger.error(f"Error writing dataset: {str(e)}")
            raise