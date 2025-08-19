import sys

import pandas as pd
import numpy as np
import math
from datetime import datetime
import snowflake.connector
import sys



import creds as MC

def sf_pd_fetchall(query: str) -> pd.DataFrame:
    with snowflake.connector.connect(
            user=MC.USER, password=MC.PASSWORD, account=MC.ACCOUNT,
            warehouse='DATA_ANALYTICS_WH', role='DATA_ANALYST',
            port=443
    ) as conn:
        cur = conn.cursor()
        cur.execute(query)
        df = cur.fetch_pandas_all()
    return df

