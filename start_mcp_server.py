#!/usr/bin/env python3
"""
Startup script for the MCP MySQL NLP Server
"""

import asyncio
import sys
import os
from pathlib import Path

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from mcp_server import main

if __name__ == "__main__":
    try:
        print("Starting MySQL NLP MCP Server...")
        print("Make sure you have:")
        print("1. Configured your .env file with database credentials")
        print("2. AWS credentials configured for Bedrock access")
        print("3. Required Python packages installed")
        print("-" * 50)
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down MCP server...")
    except Exception as e:
        print(f"Error starting MCP server: {e}")
        sys.exit(1)