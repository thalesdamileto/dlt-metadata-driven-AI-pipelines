import json
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

true = True
false = False

WATERMARK_TABLE = "workspace.default.pipelines_watermark"

TYPE_MAPPING = {
    "string": StringType(),
    "boolean": BooleanType(),

    "byte": ByteType(),
    "short": ShortType(),
    "int": IntegerType(),
    "bigint": LongType(),

    "float": FloatType(),
    "double": DoubleType(),

    "date": DateType(),
    "timestamp": TimestampType(),

    "binary": BinaryType()
}

CONTRACT_FILE = "/Workspace/Users/thalesfmorais94@gmail.com/dlt-metadata-driven-AI-pipelines/pipelines_metadata/2-silver/contracts.json"

def get_data_contract(contract_id: str) -> dict:

    with open(CONTRACT_FILE, "r") as f:
        data = json.load(f)

    for contract in data["contract_list"]:
        if contract["contract_id"] == contract_id:
            return contract

    raise ValueError(f"Contract_id {contract_id} não encontrado")

def get_watermark(contract_id:str , watermark_table: str = WATERMARK_TABLE) -> str:
    watermark_df = spark.read.table(watermark_table)
    try:
        watermark = watermark_df.select(col("watermark_value")).filter(col("contract_id") == contract_id).collect()[0][0]
        return watermark
    
    except:
        print(f"Watermark not found for contract_id {contract_id}, assuming first run with full table processing.")
        return None

def update_watermark(destination_table:str, contract_id:str, watermark_column:str, new_watermark_value:datetime, watermark_table: str = WATERMARK_TABLE):
    try:
        spark.sql(f"""
            MERGE INTO {watermark_table} AS t
            USING (
                SELECT '{destination_table}' AS destination_table, 
                '{contract_id}' AS contract_id, 
                '{watermark_column}' AS watermark_column,
                '{new_watermark_value}' AS watermark_value
            ) AS s
            ON t.contract_id = s.contract_id
            WHEN MATCHED THEN UPDATE SET watermark_value = s.watermark_value
            WHEN NOT MATCHED THEN INSERT (contract_id, watermark_value) VALUES (s.contract_id, s.watermark_value)
        """)   
    except:
        print(f"Error updating watermark for contract_id {contract_id}.")
    