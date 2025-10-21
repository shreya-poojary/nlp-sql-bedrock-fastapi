#!/usr/bin/env python3
"""
Quick test script for the NLP MCP Server without database dependency
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_aws_bedrock():
    """Test AWS Bedrock connection"""
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        print("Testing AWS Bedrock connection...")
        
        client = boto3.client(
            service_name="bedrock-runtime",
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )
        
        # Test with a simple request
        response = client.invoke_model(
            modelId=os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet-20240229-v1:0"),
            body='{"anthropic_version": "bedrock-2023-05-31", "max_tokens": 10, "messages": [{"role": "user", "content": "Hello"}]}',
            contentType="application/json"
        )
        
        print("[SUCCESS] AWS Bedrock connection successful!")
        return True
        
    except ClientError as e:
        print(f"[ERROR] AWS Bedrock connection failed: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] AWS Bedrock test error: {e}")
        return False

def test_imports():
    """Test if all required modules can be imported"""
    try:
        import fastapi
        import boto3
        import mysql.connector
        from mcp.server import Server
        print("[SUCCESS] All required modules imported successfully!")
        return True
    except ImportError as e:
        print(f"[ERROR] Missing module: {e}")
        return False

def main():
    """Run quick tests"""
    print("=" * 50)
    print("MySQL NLP MCP Server - Quick Test")
    print("=" * 50)
    
    # Test imports
    imports_success = test_imports()
    
    # Test AWS Bedrock
    bedrock_success = test_aws_bedrock()
    
    print("\n" + "=" * 50)
    if imports_success:
        print("[SUCCESS] All modules are available!")
        print("You can now run the servers:")
        print("  python start_fastapi_server.py")
        print("  python start_mcp_server.py")
    else:
        print("[ERROR] Some modules are missing. Please install dependencies:")
        print("  pip install -r requirements.txt")
    
    if bedrock_success:
        print("[SUCCESS] AWS Bedrock is configured and working!")
    else:
        print("[WARNING] AWS Bedrock connection failed. Check your AWS credentials.")
    
    print("=" * 50)
    
    return imports_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
