"""
Generates the shared audience network for a scan
"""

import pandas as pd

pd.read_csv(
    "redtagging-merged-processed.csv",
    usecols=["linker_id","linker_slug","source_name","source_url"],
    dtype=str
)