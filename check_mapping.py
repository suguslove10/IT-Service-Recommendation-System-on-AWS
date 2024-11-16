import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_mappings():
    try:
        # Read mapping file
        mapping_df = pd.read_csv('dataset/service_mapping.csv')
        logger.info("Service Mapping Contents:")
        logger.info(mapping_df)
        
        # Read interaction data
        data_df = pd.read_csv('dataset/aws_synthetic_service_recommendation_data.csv')
        logger.info("\nUnique Services in Data:")
        logger.info(sorted(data_df['AWS Service'].unique()))
        
        # Verify mappings
        service_to_id = dict(zip(mapping_df['ServiceName'], mapping_df['ServiceID']))
        id_to_service = dict(zip(mapping_df['ServiceID'], mapping_df['ServiceName']))
        
        logger.info("\nService to ID mapping:")
        for service, id in service_to_id.items():
            logger.info(f"{service} -> {id}")
            
        logger.info("\nID to Service mapping:")
        for id, service in id_to_service.items():
            logger.info(f"{id} -> {service}")
            
    except Exception as e:
        logger.error(f"Error checking mappings: {e}")

if __name__ == "__main__":
    check_mappings()