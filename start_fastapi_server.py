#!/usr/bin/env python3
"""
Startup script for the FastAPI MySQL NLP Server
"""

import uvicorn
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

if __name__ == "__main__":
    print("Starting MySQL NLP FastAPI Server...")
    print("Make sure you have:")
    print("1. Configured your .env file with database credentials")
    print("2. AWS credentials configured for Bedrock access")
    print("3. Required Python packages installed")
    print("-" * 50)
    
    # Get configuration from environment
    host = os.getenv("API_HOST", "0.0.0.0")
    port = int(os.getenv("API_PORT", 8000))
    reload = os.getenv("API_RELOAD", "true").lower() == "true"
    
    print(f"Starting server on http://{host}:{port}")
    print("API Documentation available at: http://localhost:8000/docs")
    print("Press Ctrl+C to stop the server")
    
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=reload
    )