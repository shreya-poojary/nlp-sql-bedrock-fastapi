#!/usr/bin/env python3
"""
Test Nova model directly
"""

import json
import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_nova():
    try:
        print("Testing Nova model directly...")
        
        client = boto3.client(
            service_name="bedrock-runtime",
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )
        
        # Test with a simple request
        prompt = "Convert this to SQL: Show me all courses"
        
        body = json.dumps({
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ]
        })
        
        print(f"Sending request: {body}")
        
        response = client.invoke_model(
            modelId="amazon.nova-lite-v1:0",
            body=body,
            contentType="application/json"
        )
        
        response_body = json.loads(response["body"].read())
        print(f"Response: {response_body}")
        
        # Try to extract the SQL
        if "outputText" in response_body:
            sql = response_body["outputText"]
            print(f"SQL from outputText: {sql}")
        elif "content" in response_body:
            print(f"Content: {response_body['content']}")
        else:
            print(f"Full response: {response_body}")
            
        return True
        
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    test_nova()
