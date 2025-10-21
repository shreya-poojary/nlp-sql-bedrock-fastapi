from fastapi import FastAPI, HTTPException
import mysql.connector
from mysql.connector import Error
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# MySQL config
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "database": os.getenv("DB_NAME", "mcpdemo1"),
    "autocommit": True
}

@app.post("/test-query")
async def test_query():
    """Test endpoint with hardcoded SQL"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM Courses LIMIT 5")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return {
            "status": "success",
            "sql": "SELECT * FROM Courses LIMIT 5",
            "result": rows
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
