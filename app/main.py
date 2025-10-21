from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import boto3
import mysql.connector
from mysql.connector import Error
import json
import os
from dotenv import load_dotenv
from typing import Dict, Any, List
from datetime import date, datetime, time
import decimal

# Load environment variables from .env
load_dotenv()

app = FastAPI(
    title="MySQL NLP API",
    description="Natural Language Processing API for MySQL databases using AWS Bedrock",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Bedrock client
bedrock = boto3.client(
    service_name="bedrock-runtime",
    region_name=os.getenv("AWS_REGION", "us-east-1")
)

# MySQL config from environment
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "database": os.getenv("DB_NAME", "mcpdemo1"),
    "autocommit": True
}

# Bedrock model ID from .env
MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20240620-v1:0")

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


def get_db_connection():
    """Get MySQL database connection"""
    try:
        return mysql.connector.connect(**DB_CONFIG)
    except Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

def run_sql_query(sql_query: str) -> Dict[str, Any]:
    """Execute SQL query on MySQL and return results."""
    # Security check - only allow SELECT queries
    sql_upper = sql_query.upper().strip()
    if not sql_upper.startswith('SELECT'):
        raise HTTPException(status_code=400, detail=f"Only SELECT queries are allowed for security. Generated SQL: {sql_query}")
    
    conn = get_db_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql_query)
        
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

def get_database_schema() -> Dict[str, Any]:
    """Get database schema information"""
    conn = get_db_connection()
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

def generate_sql_from_nl(question: str, schema_info: str = None) -> str:
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

        response = bedrock.invoke_model(
            modelId=MODEL_ID,
            body=body,
            contentType="application/json"
        )

        response_body = json.loads(response["body"].read())
        
        # Extract SQL from Claude's response
        sql_query = ""
        
        if "content" in response_body and len(response_body["content"]) > 0:
            sql_query = response_body["content"][0].get("text", "").strip()
        
        # Debug: print the response to see what Claude is returning
        print(f"Claude response: {response_body}")
        print(f"Extracted SQL: '{sql_query}'")
        
        # Clean up the SQL query (remove markdown formatting if present)
        if sql_query.startswith("```sql"):
            sql_query = sql_query[6:]
        if sql_query.startswith("```"):
            sql_query = sql_query[3:]
        if sql_query.endswith("```"):
            sql_query = sql_query[:-3]
        
        return sql_query.strip()

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate SQL query: {str(e)}")


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "MySQL NLP API",
        "version": "1.0.0",
        "description": "Natural Language Processing API for MySQL databases using AWS Bedrock",
        "endpoints": {
            "/test-db": "Test database connection",
            "/query": "Process natural language query",
            "/schema": "Get database schema",
            "/sql": "Execute raw SQL query",
            "/generate-sql": "Generate SQL from natural language"
        }
    }

@app.get("/test-db")
async def test_db_connection():
    """Simple endpoint to check DB connectivity."""
    try:
        conn = get_db_connection()
        conn.close()
        return {"status": "success", "message": "Connected to MySQL database!"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/schema")
async def get_schema():
    """Get database schema information"""
    try:
        schema = get_database_schema()
        return {"status": "success", "schema": schema}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting schema: {str(e)}")

@app.post("/query")
async def process_nl_query(request: dict):
    """
    Accepts a natural language query,
    converts it to SQL via Bedrock,
    executes it on MySQL, and returns results.
    """
    nl_query = request.get("query")
    if not nl_query:
        raise HTTPException(status_code=400, detail="Missing 'query' field")

    try:
        # Get schema for better SQL generation
        schema = get_database_schema()
        schema_str = json.dumps(schema, indent=2)
        
        # Generate SQL from natural language
        sql_query = generate_sql_from_nl(nl_query, schema_str)
        
        # Execute the generated SQL
        result = run_sql_query(sql_query)
        
        return {
            "status": "success",
            "nl_query": nl_query,
            "generated_sql": sql_query,
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing query: {str(e)}")

@app.post("/sql")
async def execute_sql(request: dict):
    """
    Execute a raw SQL SELECT query on the database.
    """
    sql_query = request.get("sql")
    if not sql_query:
        raise HTTPException(status_code=400, detail="Missing 'sql' field")
    
    try:
        result = run_sql_query(sql_query)
        return {
            "status": "success",
            "sql_query": sql_query,
            "result": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL execution error: {str(e)}")

@app.post("/generate-sql")
async def generate_sql_only(request: dict):
    """
    Generate SQL query from natural language without executing it.
    """
    question = request.get("question")
    if not question:
        raise HTTPException(status_code=400, detail="Missing 'question' field")
    
    try:
        # Get schema for better SQL generation
        schema = get_database_schema()
        schema_str = json.dumps(schema, indent=2)
        
        # Generate SQL from natural language
        sql_query = generate_sql_from_nl(question, schema_str)
        
        return {
            "status": "success",
            "question": question,
            "generated_sql": sql_query
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating SQL: {str(e)}")
