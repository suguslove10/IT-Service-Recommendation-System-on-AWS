import os
import pandas as pd
import logging
import time

class SyntheticDataHandler:
    def __init__(self):
        self.data_directory = "dataset"
        self.interactions_filename = "interactions.csv"
        self.source_filename = "aws_synthetic_service_recommendation_data.csv"
        self.interactions_df = None
        self.logger = logging.getLogger(__name__)
    
    def setup_datasource_data(self):
        if not os.path.isdir(self.data_directory):
            os.makedirs(self.data_directory)
            
        self.logger.info(f"Checking for data file: {self.data_directory}/{self.source_filename}")
        if not os.path.exists(f"{self.data_directory}/{self.source_filename}"):
            raise FileNotFoundError(f"Source data file {self.source_filename} not found in {self.data_directory}")
    
    def prepare_dataset(self):
        try:
            # Read the source data
            self.logger.info("Reading source data file")
            service_data = pd.read_csv(f"{self.data_directory}/{self.source_filename}")
            
            # Log the columns we found
            self.logger.info(f"Found columns: {service_data.columns.tolist()}")
            
            # Create a copy for processing
            self.interactions_df = service_data.copy()
            
            # Rename columns to match Personalize requirements
            column_mapping = {
                'User ID': 'USER_ID',
                'AWS Service': 'ITEM_ID',
                'Interaction Type': 'EVENT_TYPE'
            }
            
            self.interactions_df.rename(columns=column_mapping, inplace=True)
            
            # Convert IDs to strings
            self.interactions_df['USER_ID'] = self.interactions_df['USER_ID'].astype(str)
            
            # Create a service ID mapping
            unique_services = self.interactions_df['ITEM_ID'].unique()
            service_id_mapping = {service: str(i) for i, service in enumerate(unique_services)}
            
            # Apply the service ID mapping
            self.interactions_df['ITEM_ID'] = self.interactions_df['ITEM_ID'].map(service_id_mapping)
            
            # Save the mapping for future reference
            mapping_df = pd.DataFrame(list(service_id_mapping.items()), 
                                    columns=['ServiceName', 'ServiceID'])
            mapping_df.to_csv(f"{self.data_directory}/service_mapping.csv", index=False)
            
            # Clean up event types
            event_type_mapping = {
                'Like': 'like',
                'Purchase': 'purchase',
                'View': 'view'
            }
            self.interactions_df['EVENT_TYPE'] = self.interactions_df['EVENT_TYPE'].map(
                event_type_mapping).fillna('interaction')
            
            # Add timestamp
            current_time = int(time.time())
            self.interactions_df['TIMESTAMP'] = range(
                current_time, 
                current_time + len(self.interactions_df)
            )
            
            # Keep only required columns in the correct order
            self.interactions_df = self.interactions_df[['USER_ID', 'ITEM_ID', 'EVENT_TYPE', 'TIMESTAMP']]
            
            self._validate_data()
            
        except Exception as e:
            self.logger.error(f"Error preparing dataset: {e}")
            self.logger.error(f"Current working directory: {os.getcwd()}")
            self.logger.error(f"Data file path: {self.data_directory}/{self.source_filename}")
            raise
    
    def _validate_data(self):
        if self.interactions_df is None:
            raise ValueError("No data loaded")
            
        required_columns = ['USER_ID', 'ITEM_ID', 'EVENT_TYPE', 'TIMESTAMP']
        missing_columns = [col for col in required_columns if col not in self.interactions_df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        # Check for null values
        null_counts = self.interactions_df[required_columns].isnull().sum()
        if null_counts.sum() > 0:
            raise ValueError(f"Found null values in required columns: {null_counts[null_counts > 0]}")
            
        # Verify minimum requirements
        n_users = self.interactions_df['USER_ID'].nunique()
        n_items = self.interactions_df['ITEM_ID'].nunique()
        n_interactions = len(self.interactions_df)
        
        self.logger.info(f"Data statistics: {n_users} users, {n_items} items, {n_interactions} interactions")
        
        if n_users < 2 or n_items < 2:
            self.logger.warning(
                f"Data may not meet Personalize minimum requirements: "
                f"{n_users} users, {n_items} items, {n_interactions} interactions"
            )
    
    def write_data_set(self):
        if self.interactions_df is None:
            raise ValueError("No data prepared to write")
            
        output_path = f"{self.data_directory}/{self.interactions_filename}"
        self.interactions_df.to_csv(output_path, index=False, float_format='%.0f')
        self.logger.info(f"Dataset written to {output_path}")