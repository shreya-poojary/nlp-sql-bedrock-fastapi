#!/usr/bin/env python3
"""
Test script to verify database and AWS Bedrock connectivity
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_database_connection():
    """Test MySQL database connection"""
    try:
        import mysql.connector
        from mysql.connector import Error
        
        config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": int(os.getenv("DB_PORT", 3306)),
            "user": os.getenv("DB_USER", "root"),
            "password": os.getenv("DB_PASSWORD", "password"),
            "database": os.getenv("DB_NAME", "mcpdemo1"),
        }
        
        print("Testing database connection...")
        conn = mysql.connector.connect(**config)
        
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        print(f"[SUCCESS] Database connection successful! MySQL version: {version[0]}")
        
        # Test table listing
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"[SUCCESS] Found {len(tables)} tables in database")
        
        cursor.close()
        conn.close()
        return True
        
    except Error as e:
        print(f"[ERROR] Database connection failed: {e}")
        return False
    except Exception as e:
        print(f"[ERROR] Database test error: {e}")
        return False

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

def main():
    """Run all tests"""
    print("=" * 50)
    print("MySQL NLP MCP Server - Connection Test")
    print("=" * 50)
    
    # Check environment variables
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "AWS_REGION"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"[ERROR] Missing required environment variables: {missing_vars}")
        print("Please configure your .env file")
        return False
    
    print("[SUCCESS] Environment variables configured")
    
    # Test database connection
    db_success = test_database_connection()
    
    # Test AWS Bedrock connection
    bedrock_success = test_aws_bedrock()
    
    print("\n" + "=" * 50)
    if db_success and bedrock_success:
        print("[SUCCESS] All tests passed! You're ready to run the server.")
        print("\nTo start the MCP server: python start_mcp_server.py")
        print("To start the FastAPI server: python start_fastapi_server.py")
    else:
        print("[ERROR] Some tests failed. Please check your configuration.")
        if not db_success:
            print("- Check database credentials and connectivity")
        if not bedrock_success:
            print("- Check AWS credentials and Bedrock permissions")
    
    print("=" * 50)
    
    return db_success and bedrock_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
