import pandas as pd
import logging

log = logging.getLogger(__name__)

def deduplicate(df, engine, table_name, id_column):
    """Remove rows that already exist in the target table by id_column."""
    try:
        existing = pd.read_sql(f"SELECT {id_column} FROM {table_name}", engine)
        before = len(df)
        df = df[~df[id_column].astype(str).isin(existing[id_column].astype(str))]
        skipped = before - len(df)
        log.info(f"  Skipped {skipped:,} existing {id_column}s. New rows: {len(df):,}")
    except Exception as e:
        log.warning(f"  Could not check existing records in {table_name}: {e}")
    return df
