import boto3
import pandas as pd
import logging
from flask import Flask, render_template, request, jsonify
from personalize.data_manager import DataManager
from datetime import datetime, timedelta

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PersonalizeService:
    def __init__(self):
        try:
            self.personalize_runtime = boto3.client('personalize-runtime')
            self.data_manager = DataManager().load_data_to_json()
            
            # Load service mapping and user data
            self.service_mapping_df = pd.read_csv('dataset/service_mapping.csv')
            self.user_data = pd.read_csv('dataset/aws_synthetic_service_recommendation_data.csv')
            
            # Create bidirectional service mapping
            self.service_to_id = dict(zip(
                self.service_mapping_df['ServiceName'].astype(str), 
                self.service_mapping_df['ServiceID'].astype(str)
            ))
            self.id_to_service = dict(zip(
                self.service_mapping_df['ServiceID'].astype(str), 
                self.service_mapping_df['ServiceName'].astype(str)
            ))
            
            logger.info(f"PersonalizeService initialized with {len(self.service_to_id)} services")
            logger.info(f"Service mappings loaded: {self.id_to_service}")
            logger.info(f"Campaign ARN: {self.data_manager.campaign_arn}")
            
        except Exception as e:
            logger.error(f"Error initializing PersonalizeService: {e}")
            raise

    def get_user_history(self, user_id: int) -> list:
        """Get formatted user history"""
        try:
            user_history = self.user_data[self.user_data['User ID'] == int(user_id)]
            history = []
            
            for _, row in user_history.iloc[::-1].iterrows():
                date = (datetime.now() - timedelta(days=len(history))).strftime('%Y-%m-%d')
                history.append({
                    'service': row['AWS Service'],
                    'type': row['Interaction Type'],
                    'rating': int(row['Rating']),
                    'date': date
                })
            
            logger.info(f"Found {len(history)} history items for user {user_id}")
            return history
            
        except Exception as e:
            logger.error(f"Error getting user history: {e}")
            return []

    def get_service_details(self, service_name: str) -> dict:
        """Get descriptive details for a service"""
        categories = {
            'compute': ['EC2', 'Lambda'],
            'security': ['GuardDuty', 'Security Hub', 'IAM', 'WAF'],
            'storage': ['S3'],
            'migration': ['Cloud Migration'],
            'development': ['Amplify'],
            'consulting': ['Solutions Architect', 'Well-Architected']
        }
        
        category = 'other'
        for cat, keywords in categories.items():
            if any(keyword.lower() in service_name.lower() for keyword in keywords):
                category = cat
                break

        descriptions = {
            'compute': "Enhance your computing capabilities",
            'security': "Strengthen your security posture",
            'storage': "Optimize your data storage solutions",
            'migration': "Improve your cloud migration journey",
            'development': "Accelerate your application development",
            'consulting': "Get expert guidance and best practices",
            'other': "Expand your AWS capabilities"
        }

        return {
            'category': category,
            'description': f"{descriptions.get(category, descriptions['other'])} with {service_name}"
        }

    def get_recommendations(self, user_id: str) -> list:
        """Get personalized recommendations"""
        try:
            logger.info(f"Getting recommendations for user {user_id}")
            response = self.personalize_runtime.get_recommendations(
                campaignArn=self.data_manager.campaign_arn,
                userId=str(user_id),
                numResults=10
            )
            
            logger.info(f"Raw recommendations response: {response}")
            recommendations = []
            
            for item in response['itemList']:
                service_id = str(item['itemId'])
                service_name = self.id_to_service.get(service_id)
                
                if not service_name:
                    logger.warning(f"Unknown service ID: {service_id}")
                    continue
                    
                confidence = float(item.get('score', 0))
                details = self.get_service_details(service_name)
                
                recommendations.append({
                    'service': service_name,
                    'confidence': confidence,
                    'reason': f"{details['description']} ({confidence:.0%} confidence)"
                })
            
            logger.info(f"Processed {len(recommendations)} recommendations")
            return recommendations
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {str(e)}")
            return []

# Initialize the service
personalize_service = None
try:
    personalize_service = PersonalizeService()
except Exception as e:
    logger.error(f"Failed to initialize PersonalizeService: {e}")

@app.route('/')
def home():
    try:
        if not personalize_service:
            return render_template('error.html', 
                                 message="Service not initialized properly"), 500
            
        unique_users = sorted(personalize_service.user_data['User ID'].unique())
        return render_template('index.html', users=unique_users)
        
    except Exception as e:
        logger.error(f"Error in home route: {e}")
        return render_template('error.html', 
                             message="Error loading page"), 500

@app.route('/get_recommendations', methods=['POST'])
def get_recommendations():
    try:
        if not personalize_service:
            return jsonify({
                'status': 'error',
                'message': 'Recommendation service not initialized'
            })
            
        user_id = request.form.get('user_id')
        logger.info(f"Recommendation request for user: {user_id}")
        
        if not user_id:
            return jsonify({
                'status': 'error',
                'message': 'User ID is required'
            })

        history = personalize_service.get_user_history(int(user_id))
        recommendations = personalize_service.get_recommendations(str(user_id))

        logger.info(f"History items: {len(history)}")
        logger.info(f"Recommendations: {len(recommendations)}")

        return jsonify({
            'status': 'success',
            'history': history,
            'recommendations': recommendations
        })
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

if __name__ == '__main__':
    app.run(debug=True, port=5000)