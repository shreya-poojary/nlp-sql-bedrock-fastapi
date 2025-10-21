#!/usr/bin/env python3
"""
Test script specifically for AWS RDS MySQL connection
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_rds_connection():
    """Test AWS RDS MySQL connection"""
    try:
        import mysql.connector
        from mysql.connector import Error
        
        config = {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT", 3306)),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_NAME"),
            "connect_timeout": 30,
            "autocommit": True
        }
        
        print("Testing AWS RDS connection...")
        print(f"Host: {config['host']}")
        print(f"Port: {config['port']}")
        print(f"Database: {config['database']}")
        print(f"User: {config['user']}")
        
        conn = mysql.connector.connect(**config)
        
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        print(f"[SUCCESS] Connected to AWS RDS! MySQL version: {version[0]}")
        
        # Test table listing
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print(f"[SUCCESS] Found {len(tables)} tables in database")
        
        if tables:
            print("Tables found:")
            for table in tables:
                print(f"  - {table[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Error as e:
        print(f"[ERROR] RDS connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check your RDS endpoint URL")
        print("2. Verify security group allows your IP")
        print("3. Confirm database credentials")
        print("4. Check if RDS instance is running")
        return False
    except Exception as e:
        print(f"[ERROR] Connection test error: {e}")
        return False

def test_aws_bedrock():
    """Test AWS Bedrock connection"""
    try:
        import boto3
        from botocore.exceptions import ClientError
        
        print("\nTesting AWS Bedrock connection...")
        
        client = boto3.client(
            service_name="bedrock-runtime",
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )
        
        # Test with a simple request
        response = client.invoke_model(
            modelId=os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20240620-v1:0"),
            body='{"messages": [{"role": "user", "content": [{"text": "Hello"}]}]}',
            contentType="application/json"
        )
        
        print("[SUCCESS] AWS Bedrock connection successful!")
        return True
        
    except ClientError as e:
        print(f"[ERROR] AWS Bedrock connection failed: {e}")
        print("\nTroubleshooting tips:")
        print("1. Check AWS credentials")
        print("2. Verify Bedrock access permissions")
        print("3. Confirm model ID is correct")
        return False
    except Exception as e:
        print(f"[ERROR] AWS Bedrock test error: {e}")
        return False

def test_sample_data():
    """Test with sample data if available"""
    try:
        import mysql.connector
        from mysql.connector import Error
        
        config = {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT", 3306)),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "database": os.getenv("DB_NAME"),
            "autocommit": True
        }
        
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor(dictionary=True)
        
        # Try to get sample data from first table
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        if tables:
            first_table = tables[0][list(tables[0].keys())[0]]
            cursor.execute(f"SELECT * FROM `{first_table}` LIMIT 3")
            sample_data = cursor.fetchall()
            
            print(f"\n[SUCCESS] Sample data from '{first_table}':")
            for i, row in enumerate(sample_data, 1):
                print(f"  Row {i}: {row}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"[INFO] Could not fetch sample data: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("AWS RDS MySQL NLP MCP Server - Connection Test")
    print("=" * 60)
    
    # Check environment variables
    required_vars = ["DB_HOST", "DB_USER", "DB_PASSWORD", "DB_NAME", "AWS_REGION"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"[ERROR] Missing required environment variables: {missing_vars}")
        print("Please configure your .env file with AWS RDS details")
        return False
    
    print("[SUCCESS] Environment variables configured")
    
    # Test RDS connection
    rds_success = test_rds_connection()
    
    # Test AWS Bedrock connection
    bedrock_success = test_aws_bedrock()
    
    # Test sample data if RDS is connected
    if rds_success:
        test_sample_data()
    
    print("\n" + "=" * 60)
    if rds_success and bedrock_success:
        print("[SUCCESS] All tests passed! Your AWS RDS setup is working.")
        print("\nYou can now run the servers:")
        print("  python start_fastapi_server.py")
        print("  python start_mcp_server.py")
        print("\nAPI will be available at: http://localhost:8000")
        print("API docs at: http://localhost:8000/docs")
    else:
        print("[ERROR] Some tests failed. Please check your configuration.")
        if not rds_success:
            print("- Check RDS endpoint, credentials, and security group")
        if not bedrock_success:
            print("- Check AWS credentials and Bedrock permissions")
    
    print("=" * 60)
    
    return rds_success and bedrock_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
