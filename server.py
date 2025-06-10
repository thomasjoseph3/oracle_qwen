from flask import Flask, request, jsonify
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import json
import torch
import oracledb
import os
import logging
from dotenv import load_dotenv
import re
import inflect
from typing import Set, Dict, Any
from rapidfuzz import fuzz, process

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Flask app
app = Flask(__name__)

# Load environment variables
load_dotenv()

# Database connection details
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_SERVICE_NAME = os.getenv("DB_SERVICE_NAME")
DB_PRIVILEGE = os.getenv("DB_PRIVILEGE", "").upper()

# Validate environment variables
required_vars = ["DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT", "DB_SERVICE_NAME"]
missing_vars = [var for var in required_vars if not os.getenv(var)]
if missing_vars:
    logging.error(f"Missing required environment variables: {', '.join(missing_vars)}")
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Create DSN
dsn = f"{DB_HOST}:{DB_PORT}/{DB_SERVICE_NAME}"

# Initialize Inflect engine
inflect_engine = inflect.engine()

# Define stop words
STOP_WORDS = {
    "is", "as", "list", "all", "get", "retrieve", "find", "to", "for",
    "on", "by", "in", "and", "of", "the", "from", "assigned"
}

# Load schema from schema.json
try:
    with open("schema.json", "r") as file:
        schema = json.load(file)
    logging.info("Schema loaded successfully from schema.json.")
    logging.info(f"Schema type: {type(schema)}")
    logging.info(f"Schema content: {json.dumps(schema, indent=2)}")
    logging.info(f"Tables found in schema: {list(schema.keys())}")
except FileNotFoundError:
    logging.error("Error: JSON file 'schema.json' not found.")
    raise
except json.JSONDecodeError as e:
    logging.error(f"Error decoding JSON file: {e}")
    raise

# Initialize model
MODEL_PATH = "qwen-oracle-sql-model"
BASE_MODEL = "Qwen/Qwen1.5-0.5B"  # Assuming Qwen 0.6B is based on Qwen1.5-0.5B

logging.info("Loading model...")
try:
    tokenizer_files = ["tokenizer_config.json", "tokenizer.model", "special_tokens_map.json"]
    missing_tokenizer_files = not all(os.path.exists(os.path.join(MODEL_PATH, f)) for f in tokenizer_files)

    if missing_tokenizer_files:
        logging.info("Tokenizer not found in fine-tuned model. Downloading from base model...")
        tokenizer = AutoTokenizer.from_pretrained(BASE_MODEL, use_fast=False)
        tokenizer.save_pretrained(MODEL_PATH)
    else:
        tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH, use_fast=False)

    model = AutoModelForCausalLM.from_pretrained(
        MODEL_PATH,
        device_map="auto",
        torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32
    )

    pipe = pipeline("text-generation", model=model, tokenizer=tokenizer)
    logging.info("Model loaded successfully.")
except Exception as e:
    logging.error(f"Model loading failed: {e}")
    tokenizer = None
    model = None
    pipe = None

# Helper function to generate singular/plural variations
def generate_variations(word: str) -> Set[str]:
    return {word, inflect_engine.singular_noun(word) or word, inflect_engine.plural(word)}

# Recursive function to find related tables
def traverse_relationships(table: str, schema: Dict[str, Any], relevant_tables: Set[str]):
    if table not in schema:
        logging.warning(f"Table {table} not found in schema.")
        return
    foreign_keys = schema[table].get("foreign_keys", {})
    for related_table in foreign_keys.values():
        if related_table not in relevant_tables:
            relevant_tables.add(related_table)
            traverse_relationships(related_table, schema, relevant_tables)

# Function to find relevant tables
def find_relevant_tables(prompt: str, schema: Dict[str, Any]) -> Set[str]:
    if not isinstance(schema, dict):
        logging.error("Schema is not a dictionary. Expected a dictionary with table names as keys.")
        raise ValueError("Schema must be a dictionary with table names as keys.")

    prompt_words = [word for word in re.findall(r'\w+', prompt.lower()) if word not in STOP_WORDS]
    table_map = {}
    column_map = {}

    for table, details in schema.items():
        if not isinstance(details, dict):
            logging.warning(f"Invalid schema entry for table {table}: {details}")
            continue
        for variation in generate_variations(table.lower()):
            table_map[variation] = table
        columns = details.get("columns", [])
        if not isinstance(columns, list):
            logging.warning(f"Columns for table {table} is not a list: {columns}")
            continue
        for column in columns:
            for variation in generate_variations(column.lower()):
                column_map[variation] = table

    matched_tables = set()
    for word in prompt_words:
        table_match = process.extractOne(word, table_map.keys(), scorer=fuzz.token_set_ratio)
        if table_match and table_match[1] > 60:
            matched_tables.add(table_map[table_match[0]])
            logging.info(f"Matched table: {table_map[table_match[0]]} (score: {table_match[1]})")

        column_match = process.extractOne(word, column_map.keys(), scorer=fuzz.token_set_ratio)
        if column_match and column_match[1] > 60:
            matched_tables.add(column_map[column_match[0]])
            logging.info(f"Matched column: {column_match[0]} -> table: {column_map[column_match[0]]} (score: {column_match[1]})")

    relevant_tables = set(matched_tables)
    for table in matched_tables:
        traverse_relationships(table, schema, relevant_tables)

    logging.info(f"Relevant tables: {relevant_tables}")
    return relevant_tables

# Function to validate columns in the query against the schema
def validate_query_columns(query: str, schema: Dict[str, Any], relevant_tables: Set[str]):
    # Simple regex to extract column names (not perfect, but sufficient for basic validation)
    column_pattern = r'\b\w+\b(?=\s*[,)]|\s+FROM\s+|\s*$)'
    columns = re.findall(column_pattern, query, re.IGNORECASE)
    invalid_columns = []

    for table in relevant_tables:
        valid_columns = schema[table].get("columns", [])
        for column in columns:
            if column.lower() not in [c.lower() for c in valid_columns] and column.lower() not in invalid_columns:
                invalid_columns.append(column)

    if invalid_columns:
        logging.warning(f"Invalid columns in query: {invalid_columns}")
    return invalid_columns

# Function to execute SQL query
def execute_query(query: str) -> list:
    # Remove trailing semicolon
    query = query.rstrip(";").strip()
    try:
        logging.info(f"Connecting to Oracle database at {dsn}")
        if DB_PRIVILEGE == "SYSDBA":
            connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn, mode=oracledb.AUTH_MODE_SYSDBA)
        elif DB_PRIVILEGE == "SYSOPER":
            connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn, mode=oracledb.AUTH_MODE_SYSOPER)
        else:
            connection = oracledb.connect(user=DB_USER, password=DB_PASSWORD, dsn=dsn)
        
        cursor = connection.cursor()
        logging.info(f"Executing query: {query}")
        
        cursor.execute(query)
        columns = [col[0] for col in cursor.description] if cursor.description else []
        results = cursor.fetchall()
        
        result_list = [dict(zip(columns, row)) for row in results]
        
        cursor.close()
        connection.close()
        logging.info("Query executed successfully.")
        return result_list
    except oracledb.DatabaseError as e:
        logging.error(f"Database query failed: {e}")
        raise

# API Endpoint to generate and execute SQL query
@app.route("/generate-and-execute-sql", methods=["POST"])
def generate_and_execute_sql():
    if pipe is None:
        return jsonify({"error": "Model not loaded", "generated_query": None}), 500

    data = request.get_json()
    prompt = data.get("prompt")

    if not prompt:
        return jsonify({"error": "Missing 'prompt' in request", "generated_query": None}), 400

    logging.info(f"Input prompt: {prompt}")
    try:
        # Find relevant tables
        relevant_tables = find_relevant_tables(prompt, schema)
        if not relevant_tables:
            return jsonify({"error": "No relevant tables found for the prompt", "generated_query": None}), 400

        # Prepare context with schema information for relevant tables
        context = {table: schema[table] for table in relevant_tables}
        context_str = json.dumps(context)

        # Generate SQL query
        input_text = f"Human: {prompt}\nContext: {context_str}\nAssistant:"
        response = pipe(
            input_text,
            max_new_tokens=200,
            pad_token_id=tokenizer.eos_token_id,
            eos_token_id=tokenizer.eos_token_id,
            do_sample=True,
            temperature=0.2,
            top_p=0.95
        )

        generated_query = response[0]['generated_text'].split("Assistant:")[-1].strip()
        logging.info(f"Generated query: {generated_query}")

        # Validate columns in the query
        invalid_columns = validate_query_columns(generated_query, schema, relevant_tables)
        if invalid_columns:
            logging.warning(f"Generated query contains invalid columns: {invalid_columns}")

        # Execute the generated query
        results = execute_query(generated_query)

        return jsonify({
            "input_prompt": prompt,
            "relevant_tables": list(relevant_tables),
            "generated_query": generated_query,
            "results": results
        })

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return jsonify({
            "error": str(e),
            "input_prompt": prompt,
            "relevant_tables": list(relevant_tables) if 'relevant_tables' in locals() else [],
            "generated_query": generated_query if 'generated_query' in locals() else None
        }), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)