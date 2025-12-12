import copy
import datetime
import json
import logging
from typing import Any, Dict, Set
import pandas as pd

from elasticsearch_config import BASE_QUERY, ES
from utils import log_to_sql_db

# -------------------------
# ✅ GLOBAL CONFIGURATION
# -------------------------
JOB_NAME = "Initial_load_from_elasticsearch"
JOB_ID = 8
EXECUTABLE_CMD = "elasticsearch_data_export"
INDEX_PATTERN = "rta_claim_headers-*"

# -------------------------
# ✅ COMPLETE JSON FLATTENING FUNCTIONS
# -------------------------

def to_pascal_case(s: str) -> str:
    """Convert string to PascalCase"""
    if not s:
        return s
    return s[0].upper() + s[1:] if len(s) > 1 else s.upper()


def to_camel_case(s: str) -> str:
    """Convert string to camelCase"""
    if not s:
        return s
    return s[0].lower() + s[1:] if len(s) > 1 else s.lower()


def flatten_dict_complete(d: Dict[str, Any], parent_key: str = '', sep: str = '_', 
                         columns: Set[str] = None, current_depth: int = 0, 
                         max_depth: int = 20) -> Set[str]:
    
    if columns is None:
        columns = set()
    
    if current_depth > max_depth:
        # Safety: stop recursion at max depth
        columns.add(parent_key)
        return columns
    
    for k, v in d.items():
        pascal_key = to_pascal_case(k)
        new_key = f"{parent_key}{sep}{pascal_key}" if parent_key else pascal_key

        if isinstance(v, dict):
            # Always recurse into nested dictionaries
            flatten_dict_complete(v, new_key, sep, columns, current_depth + 1, max_depth)
            
        elif isinstance(v, list):
            if not v:
                # Empty list - create column
                columns.add(new_key)
            elif isinstance(v[0], dict):
                # List of objects - create indexed columns for EACH element
                for i, item in enumerate(v):
                    indexed_key = f"{new_key}{sep}{i}"
                    flatten_dict_complete(item, indexed_key, sep, columns, current_depth + 1, max_depth)
            elif isinstance(v[0], (str, int, float, bool, type(None))):
                # List of primitives - single column (will be JSON array)
                columns.add(new_key)
            else:
                # Unknown list type - treat as primitive
                columns.add(new_key)
        else:
            # Primitive value (string, number, boolean, null)
            columns.add(new_key)
    
    return columns


def extract_value_by_path_complete(data: Dict[str, Any], path: str, default: Any = '') -> Any:
    
    if not path or not data:
        return default

    parts = path.split('_')
    current = data
    
    i = 0
    while i < len(parts):
        part = parts[i]
        
        if not part:
            i += 1
            continue
        
        # Check if this part is a numeric index
        if part.isdigit():
            idx = int(part)
            if isinstance(current, list):
                if 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return default
            else:
                return default
            i += 1
            continue
        
        # Try to match as a dictionary key
        camel_key = to_camel_case(part)
        possible_keys = [camel_key, part.lower(), part, part.capitalize()]
        found = False
        
        if isinstance(current, dict):
            for key in possible_keys:
                if key in current:
                    current = current[key]
                    found = True
                    break
            
            if not found:
                return default
        elif isinstance(current, list):
            # Hit a list but no index specified - shouldn't happen with complete flattening
            return default
        else:
            # Hit primitive before end of path
            return default
        
        i += 1
    
    # Convert final value to string
    if current is None:
        return default
    elif isinstance(current, (dict, list)):
        return json.dumps(current)
    elif isinstance(current, bool):
        return str(current)
    else:
        return str(current)


def convert_to_string(value: Any) -> str:

    if value is None:
        return ''
    elif isinstance(value, (dict, list)):
        return json.dumps(value)
    elif isinstance(value, bool):
        return str(value)
    else:
        return str(value)


def json_to_tsv_in_memory(data: Dict[str, Any], max_depth: int = 20) -> pd.DataFrame:
 
    # Handle both Elasticsearch response format and direct source documents
    if 'hits' in data and 'hits' in data.get('hits', {}):
        # Elasticsearch format: data['hits']['hits'][i]['_source']
        hits = data['hits']['hits']
        sources = [hit.get('_source', {}) for hit in hits]
    else:
        # Direct source document format
        sources = [data]
    
    if not sources:
        logging.warning("No documents found in JSON data.")
        return pd.DataFrame()

    logging.info(f"Processing {len(sources)} document(s)...")
    
    # First pass: discover ALL columns across ALL documents
    all_columns = set()
    for idx, source in enumerate(sources):
        if source:
            doc_columns = flatten_dict_complete(source, max_depth=max_depth)
            all_columns.update(doc_columns)
            if idx == 0:
                logging.info(f"First document: {len(doc_columns)} columns discovered")
    
    columns = sorted(list(all_columns))
    logging.info(f"Total unique columns: {len(columns)}")
    
    # Second pass: extract values for each column from each document
    rows = []
    for source in sources:
        row = {}
        for col in columns:
            value = extract_value_by_path_complete(source, col)
            row[col] = convert_to_string(value)
        rows.append(row)

    df = pd.DataFrame(rows, columns=columns)
    logging.info(f"Created DataFrame: {df.shape[0]} rows × {df.shape[1]} columns")
    
    return df


# -------------------------
# ✅ FETCH AND EXPORT LOGIC
# -------------------------
def fetch_and_export_documents(output_dir: str = "./output"):

    import os
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    start_ts = datetime.datetime.utcnow()
    pipeline_run_id = start_ts.strftime("%Y%m%d%H%M%S")
    batch_id = pipeline_run_id
    
    try:
        # Count total documents
        total_docs = ES.count(index=INDEX_PATTERN, body={"query": BASE_QUERY["query"]})["count"]
        logging.info(f"Total documents to fetch: {total_docs}")
        
        search_after = None
        records_fetched = 0

        while records_fetched < total_docs:
            # Prepare query with search_after for pagination
            query = copy.deepcopy(BASE_QUERY)
            if search_after:
                query["search_after"] = search_after

            # Execute search
            result = ES.search(index=INDEX_PATTERN, body=query)
            hits = result["hits"]["hits"]
            
            if not hits:
                break

            # Extract source documents
            source_data = [hit["_source"] for hit in hits]
            records_fetched += len(source_data)

            # Convert to flattened TSV DataFrame
            df_tsv = json_to_tsv_in_memory(
                {"hits": {"hits": [{"_source": doc} for doc in source_data]}}
            )
            
            if df_tsv.empty:
                logging.warning("Generated DataFrame is empty, skipping batch")
                continue

            # Generate filename
            claim_id = hits[-1]["_source"].get("claimRequestId", "batch")
            tsv_filename = f"rta_claim_headers_{claim_id}_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.tsv"
            tsv_path = os.path.join(output_dir, tsv_filename)

            # Save to local file
            try:
                df_tsv.to_csv(tsv_path, sep='\t', index=False)
                logging.info(
                    f"Batch {records_fetched}/{total_docs} saved as {tsv_filename} "
                    f"with {len(df_tsv.columns)} columns"
                )
            except Exception as e:
                logging.error(f" Failed to save TSV file: {e}", exc_info=True)
                raise

            # Update search_after for next iteration
            last_doc = hits[-1]
            search_after = [
                last_doc["_source"]["auditProcessedDateTimeUtc"],
                last_doc["_source"]["claimRequestId"],
            ]
        
        # Log success
        end_ts = datetime.datetime.utcnow()
        log_to_sql_db(
            job_name=JOB_NAME,
            start_ts=start_ts,
            end_ts=end_ts,
            job_status="SUCCESS",
            executable_cmd=EXECUTABLE_CMD,
            error_desc=None,
            error_log_file=None,
            batch_id=batch_id,
            table_name="rta_claim_headers",
            record_count_loaded=total_docs,
        )
        
        logging.info(f" Successfully exported {total_docs} documents to {output_dir}")
        
    except Exception as e:
        end_ts = datetime.datetime.utcnow()
        logging.error(f" Error: {e}", exc_info=True)
        log_to_sql_db(
            job_name=JOB_NAME,
            start_ts=start_ts,
            end_ts=end_ts,
            job_status="FAILED",
            executable_cmd=EXECUTABLE_CMD,
            error_desc=str(e),
            error_log_file=None,
            batch_id=batch_id,
            table_name="rta_claim_headers",
        )
        raise


# -------------------------
# ✅ MAIN ENTRY POINT
# -------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    logging.info("Starting Elasticsearch data export...")
    
    # Export to local directory
    fetch_and_export_documents(output_dir="./output")
    
    logging.info("✅ Data export completed!")