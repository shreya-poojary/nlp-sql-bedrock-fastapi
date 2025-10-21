#!/usr/bin/env python3
"""
Simple test to verify the system works without AI
"""

import requests
import json

def test_simple_sql():
    """Test with a simple hardcoded SQL query"""
    try:
        response = requests.post(
            "http://localhost:8000/sql",
            json={"sql": "SELECT * FROM Courses LIMIT 5"}
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ SQL query successful!")
            print(f"Result: {json.dumps(result, indent=2)}")
            return True
        else:
            print(f"❌ SQL query failed: {response.status_code}")
            print(f"Error: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("Testing simple SQL query...")
    test_simple_sql()
