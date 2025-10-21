#!/usr/bin/env python3
"""
MCP Server for NLP to SQL conversion using AWS Bedrock and MySQL RDS
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Sequence
import mysql.connector
from mysql.connector import Error
import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
from datetime import date, datetime, time
import decimal

from mcp.server import Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    LoggingLevel
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mysql-nlp-mcp")

# Initialize MCP server
server = Server("mysql-nlp-mcp")

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "database": os.getenv("DB_NAME", "mcpdemo1"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "autocommit": True
}

# AWS Bedrock configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20240620-v1:0")

class MySQLBedrockClient:
    """Client for MySQL operations and AWS Bedrock integration"""
    
    def __init__(self):
        self.bedrock_client = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION
        )
    
    def get_connection(self):
        """Get MySQL database connection"""
        try:
            return mysql.connector.connect(**DB_CONFIG)
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            raise
    
    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Execute SQL query and return results"""
        # Security check - only allow SELECT queries
        sql_upper = sql.upper().strip()
        if not sql_upper.startswith('SELECT'):
            raise ValueError("Only SELECT queries are allowed for security")
        
        conn = self.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch results
            rows = cursor.fetchall()
            
            # Serialize the data to handle MySQL data types
            serialized_rows = serialize_mysql_data(rows)
            
            return {
                "columns": columns,
                "rows": serialized_rows,
                "row_count": len(serialized_rows)
            }
        finally:
            cursor.close()
            conn.close()
    
    def get_schema(self) -> Dict[str, Any]:
        """Get database schema information"""
        conn = self.get_connection()
        try:
            cursor = conn.cursor(dictionary=True)
            
            # Get all tables
            cursor.execute("SHOW TABLES")
            tables = [list(row.values())[0] for row in cursor.fetchall()]
            
            schema = {}
            for table in tables:
                # Get table structure
                cursor.execute(f"DESCRIBE `{table}`")
                columns = cursor.fetchall()
                
                # Get sample data (first 3 rows)
                cursor.execute(f"SELECT * FROM `{table}` LIMIT 3")
                sample_data = cursor.fetchall()
                
                schema[table] = {
                    "columns": serialize_mysql_data(columns),
                    "sample_data": serialize_mysql_data(sample_data)
                }
            
            return schema
        finally:
            cursor.close()
            conn.close()
    
    def generate_sql_from_nl(self, question: str, schema_info: str = None) -> str:
        """Generate SQL query from natural language using AWS Bedrock"""
        try:
            # Prepare the prompt
            prompt = f"""You are an expert SQL query generator for MySQL databases.

Question: {question}

Database Schema:
{schema_info if schema_info else "No schema information provided"}

Instructions:
1. Generate a valid MySQL SELECT query only
2. Use proper table and column names from the schema
3. Include appropriate WHERE clauses, JOINs, and aggregations as needed
4. Return ONLY the SQL query, no explanations or markdown formatting
5. Ensure the query is safe and read-only

SQL Query:"""

        # Prepare request body for Claude model
        body = json.dumps({
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ]
        })

            response = self.bedrock_client.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=body,
                contentType="application/json"
            )

        response_body = json.loads(response["body"].read())
        
        # Extract SQL from Claude's response
        sql_query = ""
        
        if "content" in response_body and len(response_body["content"]) > 0:
            sql_query = response_body["content"][0].get("text", "").strip()
            
            # Clean up the SQL query (remove markdown formatting if present)
            if sql_query.startswith("```sql"):
                sql_query = sql_query[6:]
            if sql_query.startswith("```"):
                sql_query = sql_query[3:]
            if sql_query.endswith("```"):
                sql_query = sql_query[:-3]
            
            return sql_query.strip()

        except ClientError as e:
            logger.error(f"AWS Bedrock error: {e}")
            raise Exception(f"Failed to generate SQL query: {e}")
        except Exception as e:
            logger.error(f"Error generating SQL: {e}")
            raise

# Initialize the MySQL Bedrock client
mysql_client = MySQLBedrockClient()

def serialize_mysql_data(data):
    """Convert MySQL data types to JSON-serializable formats"""
    if isinstance(data, dict):
        return {key: serialize_mysql_data(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [serialize_mysql_data(item) for item in data]
    elif isinstance(data, (date, datetime)):
        return data.isoformat()
    elif isinstance(data, time):
        return data.isoformat()
    elif isinstance(data, decimal.Decimal):
        return float(data)
    elif isinstance(data, bytes):
        return data.decode('utf-8')
    else:
        return data

@server.list_tools()
async def handle_list_tools() -> List[Tool]:
    """List available MCP tools"""
    return [
        Tool(
            name="query_database",
            description="Execute a natural language query on the MySQL database using AWS Bedrock to convert to SQL",
            inputSchema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question about the database"
                    }
                },
                "required": ["question"]
            }
        ),
        Tool(
            name="execute_sql",
            description="Execute a raw SQL SELECT query on the MySQL database",
            inputSchema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "SQL SELECT query to execute"
                    }
                },
                "required": ["sql"]
            }
        ),
        Tool(
            name="get_schema",
            description="Get the database schema including tables, columns, and sample data",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="generate_sql",
            description="Generate SQL query from natural language without executing it",
            inputSchema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "Natural language question to convert to SQL"
                    }
                },
                "required": ["question"]
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> Sequence[TextContent]:
    """Handle tool calls"""
    try:
        if name == "query_database":
            question = arguments.get("question")
            if not question:
                return [TextContent(type="text", text="Error: Question is required")]
            
            # Get schema for better SQL generation
            schema = mysql_client.get_schema()
            schema_str = json.dumps(schema, indent=2)
            
            # Generate SQL from natural language
            sql_query = mysql_client.generate_sql_from_nl(question, schema_str)
            
            # Execute the generated SQL
            result = mysql_client.execute_sql(sql_query)
            
            response = {
                "question": question,
                "generated_sql": sql_query,
                "result": result
            }
            
            return [TextContent(type="text", text=json.dumps(response, indent=2))]
        
        elif name == "execute_sql":
            sql = arguments.get("sql")
            if not sql:
                return [TextContent(type="text", text="Error: SQL query is required")]
            
            result = mysql_client.execute_sql(sql)
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        elif name == "get_schema":
            schema = mysql_client.get_schema()
            return [TextContent(type="text", text=json.dumps(schema, indent=2))]
        
        elif name == "generate_sql":
            question = arguments.get("question")
            if not question:
                return [TextContent(type="text", text="Error: Question is required")]
            
            # Get schema for better SQL generation
            schema = mysql_client.get_schema()
            schema_str = json.dumps(schema, indent=2)
            
            # Generate SQL from natural language
            sql_query = mysql_client.generate_sql_from_nl(question, schema_str)
            
            return [TextContent(type="text", text=f"Generated SQL: {sql_query}")]
        
        else:
            return [TextContent(type="text", text=f"Unknown tool: {name}")]
    
    except Exception as e:
        logger.error(f"Error in tool call {name}: {e}")
        return [TextContent(type="text", text=f"Error: {str(e)}")]

async def main():
    """Main entry point for the MCP server"""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="mysql-nlp-mcp",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=None,
                    experimental_capabilities={}
                )
            )
        )

if __name__ == "__main__":
    asyncio.run(main())
