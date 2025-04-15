import json
import requests
import sys, urllib3
from typing import Any
from logging import getLogger
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)



HOST = "xxxxxx.snowflakecomputing.com"
DATABASE = "CORTEX_AGENTS_DEMO"
SCHEMA = "MAIN"
STAGE = "SEMANTIC_MODELS"
FILE = "sales_orders.yaml"
WAREHOUSE = ""
ROLE = ""
messages = []
logger = getLogger(__name__)


def execute_sql(sql: str) -> tuple[Any, str | None]:
    url = f"https://{HOST}/api/v2/statements"
    headers = {
        "Authorization": f'Bearer {snowflake_token}',
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "cortex-requests/1.0.0",
    #    "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"
    }
    print(f"Executing SQL: {sql}")
    payload = {
        "statement": sql,
        "database": DATABASE,
        "schema": SCHEMA,
        "warehouse": WAREHOUSE,
        "role": ROLE
    }
    try:
        response = requests.post(url, headers=headers, json=payload, verify=False)
        # extract statementHandle from a successful response
        if response.status_code == 200:
            response_json = response.json()
            statement_handle = response_json.get("statementHandle")
            if statement_handle:
                print(f"SQL executed successfully. Statement Handle: {statement_handle}")
                return statement_handle, None
            else:
                print("No statement handle found in the response.")
    except Exception as e:        
        print("SQL execution Unsuccessful")
        logger.error(f"Error executing SQL: {e}")
        return None, str(e)
    

def execute_last_query() -> str:
    last_messages = messages[-1]
    if last_messages["role"] != "analyst":
        raise ValueError("The last message is not a user message.")
    sql = None
    for content in last_messages["content"]:
        if content["type"] == "sql":
            sql = content["statement"]
            break
    query_resp = execute_sql(sql)
    query_id = query_resp[0]
    return str(query_id)

def send_cortex_answers() -> dict[str, Any]:
    query_id = execute_last_query()
    if query_id is None:
        return None

    cortex_answers_request = {
        "role": "user",
        "content": [{"type": "results", "query_id": query_id}],
    }
    messages.append(cortex_answers_request)
    request_body = {
        "messages": messages,
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
        "operation": "answer_generation",
        "warehouse": WAREHOUSE,
    }
    response = send_message(request_body=request_body)
    # extract content within a dictionary 
    if response.get("message") is not None:
        content = response["message"].get("content")
        if content is not None:
            content = content[0].get("text")
            print(f"Content: {content}")
        else:
            print("No content found in the response.")


    

def send_message(request_body: dict[str, Any]) -> dict[str, Any]:
    url = f"https://{HOST}/api/v2/cortex/analyst/message"
    headers = {
        "Authorization": f'Bearer {snowflake_token}',
        "Accept": "*/*",
        "Content-Type": "application/json"
    #    "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT"
    }
    try:
        response = requests.post(url, headers=headers, json=request_body, verify=False)
        request_id = response.headers.get("X-Snowflake-Request-Id")
        return { **response.json(), "request_id": request_id }
    except Exception as e:
        logger.error(f"Error executing SQL: {e}")
        f"API call failed with status code {response.status_code} and request ID {request_id}"
    

def send_text_to_sql(prompt) -> dict[str, Any]:
    user_message =  {"role": "user",  "content": [{"type": "text", "text": prompt}]}
    messages.append(user_message)
    request_body =  {
                         "messages": messages,
                         "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
                         "operation": "sql_generation"
                }
    response = send_message(request_body)
    if response.get("message") is not None:
        messages.append(response["message"])
        return response
    else:
        print(f"API call failed with status code {response.status_code}")
        print("Response:", response.text)

def main():
    #prompt = "How many orders were cancelled with Delta?"
    global snowflake_token 
    snowflake_token = input("Enter your Snowflake JWT token:")
    prompt = input("Enter a quesiton:")
    send_text_to_sql(prompt)
    send_cortex_answers()

if __name__ == "__main__"  :
    main()