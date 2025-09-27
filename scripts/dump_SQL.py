#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convierte GA4 Parquet -> tabla plana (una columna por event_params.key) -> dump SQL.
Requisitos:
  pip install pandas pyarrow gcsfs
Uso:
    python3 ./scripts/dump_SQL.py
"""

import json
import sqlite3
from pathlib import Path
import pandas as pd
from glob import glob
import numpy as np

# ---------- Helpers ----------

def pick_value(vobj):
  """Coalesce de los campos de valor GA4."""
  if isinstance(vobj, dict):
    for k in ("string_value", "int_value", "float_value", "double_value"):
      if k in vobj and vobj[k] is not None:
        return vobj[k]
    return json.dumps(vobj, ensure_ascii=False)
  return vobj

def params_to_wide(params):
  """Convierte la lista de {key, value:{...}} en dict {key: valor_coalescido}."""
  out = {}
  for p in params:
    try:
      k = p.get("key")
      v = pick_value(p.get("value", {}))
      if k is not None:
        out[k] = v
    except Exception:
      pass
  return out

def sanitize_col(name: str) -> str:
  return (
    str(name)
    .replace(" ", "_").replace(".", "_").replace("-", "_")
    .replace("/", "_").replace(":", "_").replace("(", "_").replace(")", "_")
  )

def flatten_nested_json(df: pd.DataFrame, column: str) -> pd.DataFrame:
  """Flatten a nested JSON column into separate columns."""
  # Convert string representations back to dicts if needed
  nested_data = df[column].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
  # Normalize the nested structure
  flat = pd.json_normalize(nested_data.dropna().tolist(), errors='ignore')
  # Add prefix to avoid column name conflicts
  flat.columns = [f"{column}_{c}" for c in flat.columns]
  return flat

# ---------- Main ----------

def main():
  # --- Definir variables aquí ---
  # source = "./input_DB/events_20201101.parquet"
  source = [
    "./input_DB/events_20201101.parquet",
    "./input_DB/events_20201102.parquet",
    "./input_DB/events_20201103.parquet",
    "./input_DB/events_20201104.parquet",
    "./input_DB/events_20201105.parquet",
    "./input_DB/events_20201106.parquet",
    "./input_DB/events_20201107.parquet",
    "./input_DB/events_20201108.parquet",
    "./input_DB/events_20201109.parquet",
    "./input_DB/events_20201110.parquet",
    "./input_DB/events_20201111.parquet",
    "./input_DB/events_20201112.parquet",
    "./input_DB/events_20201113.parquet",
    "./input_DB/events_20201114.parquet",
    "./input_DB/events_20201115.parquet",
    "./input_DB/events_20201116.parquet",
    "./input_DB/events_20201117.parquet",
    "./input_DB/events_20201118.parquet",
    "./input_DB/events_20201119.parquet",
    "./input_DB/events_20201120.parquet",
    "./input_DB/events_20201121.parquet",
    "./input_DB/events_20201122.parquet",
    "./input_DB/events_20201123.parquet",
    "./input_DB/events_20201124.parquet",
    "./input_DB/events_20201125.parquet",
    "./input_DB/events_20201126.parquet",
    "./input_DB/events_20201127.parquet",
    "./input_DB/events_20201128.parquet",
    "./input_DB/events_20201129.parquet",
    "./input_DB/events_20201130.parquet",
    "./input_DB/events_20201201.parquet",
    "./input_DB/events_20201202.parquet",
    "./input_DB/events_20201203.parquet",
    "./input_DB/events_20201204.parquet",
    "./input_DB/events_20201205.parquet",
    "./input_DB/events_20201206.parquet",
    "./input_DB/events_20201207.parquet",
    "./input_DB/events_20201208.parquet",
    "./input_DB/events_20201209.parquet",
    "./input_DB/events_20201210.parquet",
    "./input_DB/events_20201211.parquet",
    "./input_DB/events_20201212.parquet",
    "./input_DB/events_20201213.parquet",
    "./input_DB/events_20201214.parquet",
    "./input_DB/events_20201215.parquet",
    "./input_DB/events_20201216.parquet",
    "./input_DB/events_20201217.parquet",
    "./input_DB/events_20201218.parquet",
    "./input_DB/events_20201219.parquet",
    "./input_DB/events_20201220.parquet",
    "./input_DB/events_20201221.parquet",
    "./input_DB/events_20201222.parquet",
    "./input_DB/events_20201223.parquet",
    "./input_DB/events_20201224.parquet",
    "./input_DB/events_20201225.parquet",
    "./input_DB/events_20201226.parquet",
    "./input_DB/events_20201227.parquet",
    "./input_DB/events_20201228.parquet",
    "./input_DB/events_20201229.parquet",
    "./input_DB/events_20201230.parquet",
    "./input_DB/events_20201231.parquet",
    "./input_DB/events_20210101.parquet",
    "./input_DB/events_20210102.parquet",
    "./input_DB/events_20210103.parquet",
    "./input_DB/events_20210104.parquet",
    "./input_DB/events_20210105.parquet",
    "./input_DB/events_20210106.parquet",
    "./input_DB/events_20210107.parquet",
    "./input_DB/events_20210108.parquet",
    "./input_DB/events_20210109.parquet",
    "./input_DB/events_20210110.parquet",
    "./input_DB/events_20210111.parquet",
    "./input_DB/events_20210112.parquet",
    "./input_DB/events_20210113.parquet",
    "./input_DB/events_20210114.parquet",
    "./input_DB/events_20210115.parquet",
    "./input_DB/events_20210116.parquet",
    "./input_DB/events_20210117.parquet",
    "./input_DB/events_20210118.parquet",
    "./input_DB/events_20210119.parquet",
    "./input_DB/events_20210120.parquet",
    "./input_DB/events_20210121.parquet",
    "./input_DB/events_20210122.parquet",
    "./input_DB/events_20210123.parquet",
    "./input_DB/events_20210124.parquet",
    "./input_DB/events_20210125.parquet",
    "./input_DB/events_20210126.parquet",
    "./input_DB/events_20210127.parquet",
    "./input_DB/events_20210128.parquet",
    "./input_DB/events_20210129.parquet",
    "./input_DB/events_20210130.parquet",
    "./input_DB/events_20210131.parquet"
  ]
  table = "events_flat"
  sqlite_path = "events_flat.db"
  sql_dump_path = "./dump_SQL/dump.sql"

  # Define nested columns to flatten
  nested_columns = {
    'user_ltv': ['revenue', 'currency'],
    'device': [
      'category', 'mobile_brand_name', 'mobile_model_name', 'mobile_marketing_name',
      'operating_system', 'operating_system_version', 'vendor_id', 'advertising_id',
      'language', 'is_limited_ad_tracking', 'time_zone_offset_seconds',
      'web_info.browser', 'web_info.browser_version'
    ],
    'geo': ['continent', 'sub_continent', 'country', 'region', 'city', 'metro'],
    'ecommerce': [
      'total_item_quantity', 'purchase_revenue_in_usd', 'purchase_revenue',
      'refund_value_in_usd', 'refund_value', 'shipping_value_in_usd',
      'shipping_value', 'tax_value_in_usd', 'tax_value', 'unique_items',
      'transaction_id'
    ]
  }

  base_cols = [
    "event_date",
    "event_timestamp",
    "event_name",
    "user_pseudo_id",
    "user_ltv",
    #     "user_ltv": {
    #       "revenue": "16.0",
    #       "currency": "USD"
    #     },
    "device",
    #     "device": {
    #       "category": "desktop",
    #       "mobile_brand_name": "Apple",
    #       "mobile_model_name": "Safari",
    #       "mobile_marketing_name": "\u003cOther\u003e",
    #       "mobile_os_hardware_model": null,
    #       "operating_system": "Macintosh",
    #       "operating_system_version": "Macintosh Intel 11.1",
    #       "vendor_id": null,
    #       "advertising_id": null,
    #       "language": null,
    #       "is_limited_ad_tracking": "No",
    #       "time_zone_offset_seconds": null,
    #       "web_info": {
    #         "browser": "Chrome",
    #         "browser_version": "87.0"
    #       }
    #     },
    "geo",
    #      "geo": {
    #        "continent": "Americas",
    #        "sub_continent": "Northern America",
    #        "country": "United States",
    #        "region": "Massachusetts",
    #        "city": "Boston",
    #        "metro": "(not set)"
    #      },
    "ecommerce",
    #      "ecommerce": {
    #        "total_item_quantity": "1",
    #        "purchase_revenue_in_usd": "16.0",
    #        "purchase_revenue": "16.0",
    #        "refund_value_in_usd": null,
    #        "refund_value": null,
    #        "shipping_value_in_usd": null,
    #        "shipping_value": null,
    #        "tax_value_in_usd": "0.0",
    #        "tax_value": "0.0",
    #        "unique_items": "1",
    #        "transaction_id": "881281"
    #      },
    "items",
    #      "items": [{
    #        "item_id": "9195912",
    #        "item_name": "Womens Google Striped LS",
    #        "item_brand": "Google",
    #        "item_variant": " MD",
    #        "item_category": "Apparel",
    #        "item_category2": "(not set)",
    #        "item_category3": "(not set)",
    #        "item_category4": "(not set)",
    #        "item_category5": "(not set)",
    #        "price_in_usd": null,
    #        "price": "16.0",
    #        "quantity": "1",
    #        "item_revenue_in_usd": null,
    #        "item_revenue": null,
    #        "item_refund_in_usd": null,
    #        "item_refund": null,
    #        "coupon": "(not set)",
    #        "affiliation": "(not set)",
    #        "location_id": "(not set)",
    #        "item_list_id": "(not set)",
    #        "item_list_name": "Not available in demo dataset",
    #        "item_list_index": "(not set)",
    #        "promotion_id": "(not set)",
    #        "promotion_name": "Complete Your Collection",
    #        "creative_name": "(not set)",
    #        "creative_slot": "(not set)"
    #      }]
    "user_first_touch_timestamp",
    "platform",
    "stream_id",
    "event_bundle_sequence_id"
  ]

  # ------------------------------
  paths = sorted(source) or [source]

  # Leer y concatenar
  dfs = []
  for p in paths:
    df = pd.read_parquet(p)
    dfs.append(df)
  df_all = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

  # Filtrar eventos especificos
  allowed_events = ["add_to_cart", "purchase", "begin_checkout", "session_start", "first_visit", "view_item"]
  df_all = df_all[df_all['event_name'].isin(allowed_events)]

  # Aplanar event_params a columnas
  if "event_params" not in df_all.columns:
    raise ValueError("⚠️ La columna 'event_params' no está en el parquet")
  param_dicts = df_all["event_params"].apply(params_to_wide)
  params_wide = pd.json_normalize(param_dicts).astype(object)

  # Flatten nested objects
  flattened_dfs = []
  for col in nested_columns.keys():
    if col in df_all.columns:
      flattened_dfs.append(flatten_nested_json(df_all, col))

  # Tomar columnas base disponibles y eliminar las que fueron aplanadas
  base_cols = [c for c in base_cols if c in df_all.columns]
  base_df = df_all[base_cols].copy()
  for col in nested_columns.keys():
    if col in base_df.columns:
      base_df = base_df.drop(columns=[col])

  # Concatenar base + event_params + flattened
  flat_df = pd.concat(
    [base_df.reset_index(drop=True), params_wide.reset_index(drop=True)] +
    [df.reset_index(drop=True) for df in flattened_dfs],
    axis=1
  )

  # Sanitizar nombres de columnas
  flat_df.columns = [sanitize_col(c) for c in flat_df.columns]

  # Asegurar que no queden tipos complejos
  for col in flat_df.columns:
    if col == 'items':  # Special handling for items array
      flat_df[col] = flat_df[col].apply(
        lambda x: json.dumps(x.tolist() if isinstance(x, np.ndarray) 
                           else (json.loads(x) if isinstance(x, str) 
                           else x), ensure_ascii=False)
      )
    elif flat_df[col].apply(lambda x: isinstance(x, (dict, list, np.ndarray))).any():
      flat_df[col] = flat_df[col].apply(
        lambda x: json.dumps(x.tolist() if isinstance(x, np.ndarray)
                           else x, ensure_ascii=False) 
                 if isinstance(x, (dict, list, np.ndarray)) else x
      )

  # Guardar en SQLite y hacer dump
  sqlite_path = Path(sqlite_path)
  sql_dump_path = Path(sql_dump_path)
  conn = sqlite3.connect(sqlite_path.as_posix())

  # Define los tipos SQL deseados
  dtype = {
    "event_timestamp": "BIGINT",
    "event_previous_timestamp": "BIGINT",
    "user_first_touch_timestamp": "BIGINT",
    "event_bundle_sequence_id": "BIGINT",
    "stream_id": "BIGINT",
    "user_ltv_revenue": "REAL",
    "ecommerce_purchase_revenue_in_usd": "REAL",
    "ecommerce_purchase_revenue": "REAL",
    "ecommerce_total_item_quantity": "BIGINT",
  }

  # Create table and save data
  flat_df.to_sql(table, conn, if_exists="replace", index=False, dtype=dtype)

  # Create primary key using unique index
  conn.execute(f"CREATE UNIQUE INDEX idx_pk_{table} ON {table} (user_pseudo_id, event_timestamp, event_name)")

  # Generar SQL dump
  with open(sql_dump_path, "w", encoding="utf-8") as f:
    for line in conn.iterdump():
      f.write(f"{line}\n")
  conn.close()

  print(f"✅ Dump generado")
  print(f"Filas: {len(flat_df):,}  Columnas: {len(flat_df.columns):,}")
  print(f"SQLite: {sqlite_path}")
  print(f"Dump SQL: {sql_dump_path}")
  print("Columnas: ", flat_df.columns.tolist())

if __name__ == "__main__":
  main()
