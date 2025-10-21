#!/usr/bin/env python3
"""
Simple test script for AWS Bedrock Nova model
"""

import json
import boto3
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_nova_model():
    """Test AWS Bedrock Nova model"""
    try:
        print("Testing AWS Bedrock Nova model...")
        
        client = boto3.client(
            service_name="bedrock-runtime",
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )
        
        # Test with a simple request
        prompt = "Convert this to SQL: Show me all users"
        
        body = json.dumps({
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ]
        })
        
        response = client.invoke_model(
            modelId="anthropic.claude-3-5-sonnet-20240620-v1:0",
            body=body,
            contentType="application/json"
        )
        
        response_body = json.loads(response["body"].read())
        
        # Extract SQL from Claude's response
        output = ""
        if "content" in response_body and len(response_body["content"]) > 0:
            output = response_body["content"][0].get("text", "").strip()
        
        print(f"[SUCCESS] Claude model response: {output}")
        return True
        
    except Exception as e:
        print(f"[ERROR] Nova model test failed: {e}")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("AWS Bedrock Nova Model Test")
    print("=" * 50)
    
    success = test_nova_model()
    
    if success:
        print("\n[SUCCESS] Nova model is working!")
        print("You can now update your .env file with:")
        print("BEDROCK_MODEL_ID=amazon.nova-lite-v1:0")
    else:
        print("\n[ERROR] Nova model test failed")
        print("Check your AWS credentials and permissions")
    
    print("=" * 50)
