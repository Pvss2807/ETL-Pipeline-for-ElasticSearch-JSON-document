# Nested JSON to TSV Flattening Tool

A Python utility to completely flatten deeply nested JSON documents (especially Elasticsearch responses) into TSV format with zero remaining nested structures.

## 🎯 Problem Statement

When exporting complex nested JSON data (e.g., healthcare claims with multiple levels of nesting), standard flattening approaches often:
- Leave arrays as JSON strings
- Miss deeply nested fields
- Create inconsistent column structures
- Fail to handle indexed array elements

This tool was developed to **completely flatten** all nested structures, ensuring every field becomes a separate column.

## ✨ Features

- ✅ **Complete Flattening**: Expands ALL nested objects and arrays into separate columns
- ✅ **Indexed Array Handling**: Creates columns like `Lines_0_Field`, `Lines_1_Field` for array elements
- ✅ **Zero JSON Strings**: No nested JSON objects remain (only primitive arrays like `[2250.0]`)
- ✅ **Elasticsearch Integration**: Built-in support for Elasticsearch pagination
- ✅ **Dual Mode**: Works with both Elasticsearch responses and direct JSON files
- ✅ **Configurable Depth**: Safety limits to prevent infinite recursion
- ✅ **Production Ready**: Comprehensive logging, error handling, and SQL job tracking

## 📊 Example Transformation

### Input JSON (Nested Structure)
```json
{
  "claimRequestId": 3590021,
  "totalCharges": 33934.0,
  "monitoredEditsConfig": {
    "rawClaimOutput": {
      "editOutput": {
        "header": [
          {
            "editMsgText": "ACE C60003 Edit",
            "editId": "C60003",
            "editDisposition": 11
          },
          {
            "editMsgText": "ACE C60007 LINE Edit",
            "editId": "C60007",
            "editDisposition": 11
          }
        ],
        "lines": [
          {
            "lineNumber": 1,
            "messages": [
              {
                "editId": "C60007",
                "editMsgText": "ACE C60007 LINE Edit"
              }
            ]
          }
        ]
      }
    }
  }
}
```

### Output TSV (Flattened Columns)
```
ClaimRequestId | TotalCharges | MonitoredEditsConfig_RawClaimOutput_EditOutput_Header_0_EditMsgText | MonitoredEditsConfig_RawClaimOutput_EditOutput_Header_0_EditId | MonitoredEditsConfig_RawClaimOutput_EditOutput_Header_1_EditMsgText | ...
3590021        | 33934.0      | ACE C60003 Edit                                                      | C60003                                                          | ACE C60007 LINE Edit                                               | ...
```

**Result**: 1 row × 5000+ columns (fully flattened)

## 🚀 Installation

### Prerequisites
```bash
pip install pandas elasticsearch
```

### Dependencies
```python
# Required
import pandas as pd
import json
import logging
from typing import Any, Dict, Set

# Optional (for Elasticsearch integration)
from elasticsearch import Elasticsearch
```

## 📖 Usage

### 1. Basic Usage - Flatten a JSON File

```python
import json
from flatten_json import json_to_tsv_in_memory

# Load JSON file
with open("claim_data.json", "r") as f:
    data = json.load(f)

# Flatten completely
df = json_to_tsv_in_memory(data, max_depth=20)

# Export to TSV
df.to_csv("flattened_output.tsv", sep='\t', index=False)

print(f"Created {df.shape[1]} columns from nested JSON")
```

### 2. Elasticsearch Integration

```python
from flatten_json import fetch_and_export_documents

# Export from Elasticsearch to local TSV files
fetch_and_export_documents(output_dir="./exports")
```

### 3. Jupyter Notebook Analysis

```python
import pandas as pd

# Flatten and view
df = json_to_tsv_in_memory(data)

# View transposed (easier for many columns)
df.T.head(50)

# Search for specific columns
price_cols = [c for c in df.columns if 'Price' in c]
df[price_cols]
```

## 🔧 Configuration

### Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_depth` | 20 | Maximum nesting depth to prevent infinite recursion |
| `sep` | `_` | Separator for nested column names |
| `output_dir` | `./output` | Directory for exported TSV files |

### Column Naming Convention

The tool uses **PascalCase** with underscore separators:
- Nested object: `ParentObject_ChildField`
- Array element: `ArrayName_0_FieldName`, `ArrayName_1_FieldName`
- Deep nesting: `Level1_Level2_Level3_Field`

## 🛠️ Core Functions

### `flatten_dict_complete()`
Recursively discovers all column paths in nested JSON structure.

```python
def flatten_dict_complete(
    d: Dict[str, Any], 
    parent_key: str = '', 
    sep: str = '_',
    columns: Set[str] = None,
    current_depth: int = 0,
    max_depth: int = 20
) -> Set[str]:
    """
    Returns: Set of all discovered column paths
    """
```

### `extract_value_by_path_complete()`
Extracts values from nested structures using underscore-separated paths.

```python
def extract_value_by_path_complete(
    data: Dict[str, Any],
    path: str,
    default: Any = ''
) -> Any:
    """
    Handles indexed paths like "Lines_0_Messages_1_EditId"
    Returns: Extracted value as string
    """
```

### `json_to_tsv_in_memory()`
Main function to convert JSON to flattened DataFrame.

```python
def json_to_tsv_in_memory(
    data: Dict[str, Any],
    max_depth: int = 20
) -> pd.DataFrame:
    """
    Returns: Fully flattened Pandas DataFrame
    """
```

## 📁 Project Structure

```
nested-json-flattening/
├── README.md
├── requirements.txt
├── flatten_json.py              # Main flattening logic
├── elasticsearch_config.py      # Elasticsearch connection config
├── utils.py                     # SQL logging utilities
├── examples/
│   ├── sample_input.json        # Example nested JSON
│   └── sample_output.tsv        # Example flattened output
└── tests/
    └── test_flattening.py       # Unit tests
```

## 🧪 Testing

### Run Unit Tests
```bash
pytest tests/test_flattening.py
```

### Test with Sample Data
```python
# Test with provided sample
sample_data = {
    "claimRequestId": 123,
    "lines": [
        {"lineNumber": 1, "charge": 100.0},
        {"lineNumber": 2, "charge": 200.0}
    ]
}

df = json_to_tsv_in_memory(sample_data)
print(df.columns.tolist())
# Output: ['ClaimRequestId', 'Lines_0_LineNumber', 'Lines_0_Charge', 
#          'Lines_1_LineNumber', 'Lines_1_Charge']
```

## 🐛 Debugging Tips

### Issue: Too Many Columns (>10,000)
**Solution**: Reduce `max_depth` or check for circular references
```python
df = json_to_tsv_in_memory(data, max_depth=15)
```

### Issue: Memory Error
**Solution**: Process in batches
```python
# Process smaller batches from Elasticsearch
BASE_QUERY["size"] = 10  # Reduce batch size
```

### Issue: Column Name Too Long
**Solution**: Use shorter separators or abbreviate parent keys
```python
# Modify to_pascal_case() to abbreviate long names
```

## 📊 Performance Characteristics

| Documents | Columns | Processing Time | Memory Usage |
|-----------|---------|-----------------|--------------|
| 1 | 5,000 | ~2 seconds | ~50 MB |
| 100 | 5,000 | ~15 seconds | ~500 MB |
| 1,000 | 5,000 | ~2 minutes | ~5 GB |

*Tested on: Intel i7, 16GB RAM, Python 3.9*

## 🔄 Comparison: Before vs After

### Original Code Issues
- ❌ Stopped at 'header'/'lines' fields, leaving JSON strings
- ❌ Only sampled first 3 array elements
- ❌ Couldn't handle numeric indices in paths
- ❌ Resulted in ~300 columns with nested JSON

### Rectified Code
- ✅ Complete flattening of ALL nested structures
- ✅ Indexes ALL array elements
- ✅ Handles paths like `Lines_5_Messages_0_EditId`
- ✅ Results in ~5000 columns with zero nested JSON


## 👥 Authors

- **Shravan Pvss** - *Initial work & debugging*

## 🙏 Acknowledgments

- Inspired by healthcare claims processing challenges
- Built for handling deeply nested Elasticsearch documents
- Optimized for RTA (Real-Time Adjudication) claim data structures


## 🔖 Version History

### v2.0.0 (Current)
- ✅ Complete flattening logic implemented
- ✅ Zero remaining nested JSON objects
- ✅ Indexed array element support
- ✅ Comprehensive error handling

### v1.0.0 (Original - Deprecated)
- ⚠️ Partial flattening (stopped at certain fields)
- ⚠️ Arrays left as JSON strings
- ⚠️ Limited to ~300 columns

---

**Made with ❤️ for handling complex nested JSON structures**
