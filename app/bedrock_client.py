import os
import json
import boto3

# Initialize AWS Bedrock client using environment or default config
def get_bedrock_client():
    session = boto3.Session(region_name=os.getenv("AWS_REGION", "us-east-1"))
    return session.client("bedrock-runtime")

# Function to generate SQL query using Bedrock model
def generate_sql_from_nl(question: str, table_schema: str) -> str:
    """
    Converts a natural language question into an SQL query using AWS Bedrock model.
    """
    client = get_bedrock_client()

    prompt = f"""
    You are an expert SQL query generator.
    Convert the following natural language question into an SQL query.

    Question: {question}

    Use this table schema for reference:
    {table_schema}

    Output only the SQL query, no explanation.
    """

    # Prepare Bedrock request body for Anthropic Claude or Titan Text model
    body = json.dumps({
        "inputText": prompt,
        "textGenerationConfig": {
            "maxTokenCount": 500,
            "temperature": 0.1,
            "topP": 0.9
        }
    })

    try:
        response = client.invoke_model(
            modelId=os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-sonnet"),
            body=body
        )

        # Read response
        response_body = json.loads(response["body"].read())
        output_text = response_body.get("outputText", "").strip()

        # Return the SQL query text
        return output_text

    except Exception as e:
        print("Error invoking Bedrock model:", str(e))
        return "SELECT 'Error generating SQL from Bedrock';"
