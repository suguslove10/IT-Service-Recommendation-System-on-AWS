import boto3
import pandas as pd
import os
import logging
from personalize.data_manager import DataManager

# Initialize AWS Personalize runtime client
personalize_runtime = boto3.client('personalize-runtime')

# Set up logging
logging.basicConfig(format='%(asctime)s-%(levelname)s: %(message)s',
                    filename="logs/personalize-executor.log", level=logging.INFO)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())

def load_service_mapping():
    """Load the service ID mapping file"""
    mapping_df = pd.read_csv('dataset/service_mapping.csv')
    return dict(zip(mapping_df['ServiceID'], mapping_df['ServiceName']))

def get_recommendations(campaign_arn, user_id, num_recommendations=10):
    """Get recommendations for a specific user"""
    try:
        response = personalize_runtime.get_recommendations(
            campaignArn=campaign_arn,
            userId=str(user_id),
            numResults=num_recommendations
        )
        return response['itemList']
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        return None

def main():
    # Load the data manager
    data_manager = DataManager().load_data_to_json()
    
    if data_manager is None:
        logger.error("Must create the personalize objects first with: python3 personalize_demo_creator.py")
        return
        
    try:
        # Load original data and service mapping
        logger.info("Loading data...")
        data = pd.read_csv('dataset/aws_synthetic_service_recommendation_data.csv')
        service_mapping = load_service_mapping()
        
        # Get unique users
        unique_users = data['User ID'].unique()
        logger.info(f"Found {len(unique_users)} unique users")
        
        # Get recommendations for a few sample users
        num_test_users = 5
        sample_users = pd.Series(unique_users).sample(n=min(num_test_users, len(unique_users)))
        
        for user_id in sample_users:
            logger.info(f"\nGetting recommendations for User {user_id}")
            
            # Show user's history
            user_history = data[data['User ID'] == user_id]
            logger.info("\nUser's Service History:")
            for _, row in user_history.iterrows():
                logger.info(f"- {row['AWS Service']} (Interaction: {row['Interaction Type']}, Rating: {row['Rating']})")
            
            # Get recommendations
            recommendations = get_recommendations(data_manager.campaign_arn, str(user_id))
            
            if recommendations:
                logger.info("\nRecommended Services:")
                for rank, item in enumerate(recommendations, 1):
                    service_name = service_mapping.get(item['itemId'], f"Unknown Service ({item['itemId']})")
                    score = float(item['score']) if 'score' in item else 0.0
                    logger.info(f"{rank}. {service_name} (Score: {score:.4f})")
            else:
                logger.error("Failed to get recommendations")
            
            logger.info("-" * 80)
            
    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise

if __name__ == "__main__":
    main()