import os
import pandas as pd

class SchemaMapper:
    def __init__(self, mapping_file_path: str = None):
        self.mapping_file_path = mapping_file_path or os.getenv("MAPPING_FILE_PATH")
        self.mapping_dict = {} # "db.schema.table" -> "catalog.schema.table"
        self._load_mapping()

    def _load_mapping(self):
        if not self.mapping_file_path or not os.path.exists(self.mapping_file_path):
            print(f"Warning: Mapping file not found at {self.mapping_file_path}. Using pass-through mapping.")
            return

        try:
            if self.mapping_file_path.endswith('.xlsx'):
                df = pd.read_excel(self.mapping_file_path)
            elif self.mapping_file_path.endswith('.csv') or self.mapping_file_path.endswith('.txt'):
                df = pd.read_csv(self.mapping_file_path)
            else:
                print("Unsupported mapping file format. Use .xlsx, .csv, or .txt")
                return
            
            # Normalize column names
            df.columns = [c.strip().lower() for c in df.columns]
            
            # Expected columns: sql_server_database, sql_server_schema, sql_server_table, databricks_catalog, databricks_schema, databricks_table
            # Alternatively, if it just has 'source' and 'target'
            if 'source' in df.columns and 'target' in df.columns:
                for _, row in df.iterrows():
                    self.mapping_dict[str(row['source']).strip().lower()] = str(row['target']).strip()
            elif ('sql_server_table' in df.columns or 'ms_sql_server_table' in df.columns) and 'databricks_table' in df.columns:
                for _, row in df.iterrows():
                    db = str(row.get('sql_server_database', row.get('ms_sql_server_database', ''))).strip()
                    schema = str(row.get('sql_server_schema', row.get('ms_sql_server_schema', ''))).strip()
                    tbl = str(row.get('sql_server_table', row.get('ms_sql_server_table', ''))).strip()
                    
                    dbx_cat = str(row.get('databricks_catalog', '')).strip()
                    dbx_sch = str(row.get('databricks_schema', '')).strip()
                    dbx_tbl = str(row.get('databricks_table', '')).strip()
                    
                    src_key = f"{db}.{schema}.{tbl}" if db and schema else tbl
                    tgt_val = f"{dbx_cat}.{dbx_sch}.{dbx_tbl}" if dbx_cat and dbx_sch else dbx_tbl
                    self.mapping_dict[src_key.lower()] = tgt_val
            else:
                print("Mapping file missing expected columns. Need 'source'/'target' or detailed db/schema/table columns.")
        except Exception as e:
            print(f"Failed to load mapping file: {e}")

    def get_mapped_name(self, sql_server_name: str) -> str:
        """Returns the mapped Databricks name, or the original if not found."""
        key = sql_server_name.strip().lower()
        return self.mapping_dict.get(key, sql_server_name)
    
    def get_full_mapping_string(self) -> str:
        """Returns a string representation of the mapping rules to feed into the AI prompt."""
        if not self.mapping_dict:
            return "No specific mapping rules provided. Retain original table names but adjust catalog/schema as needed."
        rules = []
        for src, tgt in self.mapping_dict.items():
            rules.append(f"- {src} -> {tgt}")
        return "\n".join(rules)
