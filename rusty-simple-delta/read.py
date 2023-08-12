from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import os
import json
import pandas as pd

delta_table_path = 'deltaTable'
dt = DeltaTable("file:///Users/ajith.shetty/personal_project/data-engineering-rust-demo/rusty-simple-delta/data/simple-table") 

print(dt.schema)
# Read Data from Delta table
print(dt.to_pandas())

