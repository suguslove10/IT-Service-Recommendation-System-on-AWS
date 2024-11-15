from flask import Flask, render_template, request, jsonify
import boto3
import pandas as pd
import os
import logging
from personalize.data_manager import DataManager

app = Flask(__name__)

# Initialize AWS clients
try:
    personalize_runtime = boto3.client('personalize-runtime')
    data_manager = DataManager().load_data_to_json()

    # Load service mapping and user data
    service_mapping_df = pd.read_csv('dataset/service_mapping.csv')
    user_data = pd.read_csv('dataset/aws_synthetic_service_recommendation_data.csv')
    
    # Create bidirectional service mapping
    service_to_id = dict(zip(service_mapping_df['ServiceName'], service_mapping_df['ServiceID']))
    id_to_service = dict(zip(service_mapping_df['ServiceID'], service_mapping_df['ServiceName']))
    
    print("Available services:", id_to_service)  # Debug print

except Exception as e:
    print(f"Error during initialization: {e}")

@app.route('/')
def home():
    try:
        # Get unique users for dropdown
        unique_users = sorted(user_data['User ID'].unique())
        return render_template('index.html', users=unique_users)
    except Exception as e:
        return f"Error: {str(e)}", 500

@app.route('/get_recommendations', methods=['POST'])
def get_recommendations():
    user_id = request.form.get('user_id')
    
    try:
        # Get user history
        user_history = user_data[user_data['User ID'] == int(user_id)].to_dict('records')
        
        # Get recommendations
        response = personalize_runtime.get_recommendations(
            campaignArn=data_manager.campaign_arn,
            userId=str(user_id),
            numResults=10
        )
        
        recommendations = []
        for item in response['itemList']:
            # Get the actual service name from the mapping
            service_id = item['itemId']
            service_name = id_to_service.get(service_id, f"Unknown Service ({service_id})")
            
            recommendations.append({
                'service': service_name,
                'score': float(item.get('score', 0)),
                'id': service_id  # Include ID for debugging
            })
            
        return jsonify({
            'status': 'success',
            'history': user_history,
            'recommendations': recommendations,
            'debug_mapping': id_to_service  # Include mapping for debugging
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

if __name__ == '__main__':
    app.run(debug=True, port=5000)