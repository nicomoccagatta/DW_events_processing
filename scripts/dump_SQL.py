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

# ---------- Main ----------

def main():
  # --- Definir variables aquí ---
  source = "./input_DB/events_20201101.parquet"
  table = "events_20201101_flat"
  sqlite_path = "events_flat.db"
  sql_dump_path = "./dump_SQL/dump.sql"
  base_cols = [
    "event_date", "event_timestamp", "event_name", "user_pseudo_id", "user_id",
    "privacy_info", "user_properties", "user_ltv", "device", "geo", "app_info",
    "traffic_source", "event_dimensions", "ecommerce", "items",
    "event_previous_timestamp", "event_value_in_usd", "user_first_touch_timestamp",
    "platform", "stream_id", "event_bundle_sequence_id", "event_server_timestamp_offset"
  ]
  # ------------------------------
  paths = sorted(glob(source)) or [source]

  # Leer y concatenar
  dfs = []
  for p in paths:
    df = pd.read_parquet(p)
    dfs.append(df)
  df_all = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

  # Aplanar event_params a columnas
  if "event_params" not in df_all.columns:
    raise ValueError("⚠️ La columna 'event_params' no está en el parquet")
  param_dicts = df_all["event_params"].apply(params_to_wide)
  params_wide = pd.json_normalize(param_dicts).astype(object)

  # Tomar columnas base disponibles y serializar las que aún sean anidadas
  base_cols = [c for c in base_cols if c in df_all.columns]
  base_df = df_all[base_cols].copy()
  for col in base_df.columns:
    if base_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
      base_df[col] = base_df[col].apply(
        lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
      )

  # Concatenar base + event_params
  flat_df = pd.concat([base_df.reset_index(drop=True), params_wide.reset_index(drop=True)], axis=1)

  # Sanitizar nombres de columnas
  flat_df.columns = [sanitize_col(c) for c in flat_df.columns]

  # Asegurar que no queden tipos complejos
  for col in flat_df.columns:
    if flat_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
      flat_df[col] = flat_df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)

  # Guardar en SQLite y hacer dump
  sqlite_path = Path(sqlite_path)
  sql_dump_path = Path(sql_dump_path)
  conn = sqlite3.connect(sqlite_path.as_posix())

  # Define los tipos SQL deseados solo para ciertas columnas
  dtype = {
    "event_timestamp": "BIGINT",
    "event_previous_timestamp": "BIGINT",
    "user_first_touch_timestamp": "BIGINT",
    "event_bundle_sequence_id": "BIGINT",
    "stream_id": "BIGINT"
    # Agregar aquí más columnas si lo necesitamos
  }
  flat_df.to_sql(table, conn, if_exists="replace", index=False, dtype=dtype)

  with open(sql_dump_path, "w", encoding="utf-8") as f:
    for line in conn.iterdump():
      f.write(f"{line}\n")
  conn.close()

  print(f"✅ Dump generado")
  print(f"Filas: {len(flat_df):,}  Columnas: {len(flat_df.columns):,}")
  print(f"SQLite: {sqlite_path}")
  print(f"Dump SQL: {sql_dump_path}")
  print("Ejemplo de columnas: ", flat_df.columns[:12].tolist())

if __name__ == "__main__":
  main()
