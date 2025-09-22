# !/bin/bash
# Script para exportar eventos de BigQuery (GA4) a Google Cloud Storage en formato Parquet

for t in $(bq ls -n 1000 bigquery-public-data:ga4_obfuscated_sample_ecommerce | grep events_ | awk '{print $1}'); do
  echo "Exportando $t ..."
  bq extract --destination_format=PARQUET \
    "bigquery-public-data:ga4_obfuscated_sample_ecommerce.$t" \
    "gs://nmoccagatta_bukito/dump_sql_parquet/${t}.parquet"
done
