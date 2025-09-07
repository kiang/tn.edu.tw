#!/usr/bin/env python3
import tabula
import pandas as pd
import json
import os

# Read PDF file
print("Reading PDF file...")
dfs = tabula.read_pdf('raw/list.pdf', pages='all', lattice=True, pandas_options={'header': None})

if len(dfs) == 0:
    # Try with stream mode if lattice doesn't work
    print("Trying stream mode...")
    dfs = tabula.read_pdf('raw/list.pdf', pages='all', stream=True, pandas_options={'header': None})

# Combine all dataframes
if len(dfs) > 0:
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Find header row
    header_row_idx = 0
    for idx, row in combined_df.iterrows():
        if '區域' in str(row.values):
            header_row_idx = idx
            break
    
    # Extract headers
    headers = combined_df.iloc[header_row_idx].fillna('').astype(str).tolist()
    
    # Clean headers
    clean_headers = []
    for i, h in enumerate(headers):
        if h and h.strip():
            clean_headers.append(h.strip())
        else:
            clean_headers.append(f'Column_{i}')
    
    # Get data after header row
    data_df = combined_df.iloc[header_row_idx + 1:].reset_index(drop=True)
    
    # Assign cleaned headers
    data_df.columns = clean_headers[:len(data_df.columns)]
    
    # Forward fill to handle merged cells in the first few columns
    fill_columns = ['區域', '機關/單位'] 
    for col in fill_columns:
        if col in data_df.columns:
            data_df[col] = data_df[col].ffill()
    
    # Clean up - remove rows that are mostly NaN
    data_df = data_df.dropna(how='all')
    data_df = data_df[data_df.apply(lambda x: x.notna().sum() > 1, axis=1)]
    
    # Clean up line breaks in cells
    for col in data_df.columns:
        data_df[col] = data_df[col].apply(lambda x: str(x).replace('\r', ' ').replace('\n', ' ').strip() if pd.notna(x) else x)
    
    # Remove duplicate header rows that appear in the data
    # These rows will have the exact same values as the column names
    if '區域' in data_df.columns and '機關/單位' in data_df.columns:
        mask = ~((data_df['區域'] == '區域') & (data_df['機關/單位'] == '機關/單位'))
        data_df = data_df[mask]
        duplicate_count = (~mask).sum()
    else:
        duplicate_count = 0
    
    # Reset index after cleaning
    data_df = data_df.reset_index(drop=True)
    
    # Save to CSV
    data_df.to_csv('raw/list.csv', index=False, encoding='utf-8-sig')
    
    # Generate JSON file for web application
    # Replace NaN with empty strings for JSON
    json_df = data_df.fillna('')
    
    # Convert to list of dictionaries
    json_data = json_df.to_dict('records')
    
    # Create JSON directory if it doesn't exist
    json_dir = 'docs/json'
    if not os.path.exists(json_dir):
        os.makedirs(json_dir)
    
    # Save to JSON file
    json_path = os.path.join(json_dir, 'service_learning_organizations.json')
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    
    # Also generate JavaScript data file for backward compatibility
    js_content = 'const serviceData = ' + json.dumps(json_data, ensure_ascii=False, indent=2) + ';'
    with open('docs/data.js', 'w', encoding='utf-8') as f:
        f.write(js_content)
    
    print(f"\nSuccessfully converted PDF to multiple formats:")
    print(f"  - CSV: raw/list.csv")
    print(f"  - JSON: {json_path}")
    print(f"  - JavaScript: docs/data.js")
    print(f"Total data rows: {len(data_df)}")
    print(f"Removed {duplicate_count} duplicate header rows")
    print(f"Columns: {list(data_df.columns)}")
    print(f"\nSample of the data (first 3 rows):")
    print(data_df.head(3).to_string())
    
else:
    print("No tables found in PDF")