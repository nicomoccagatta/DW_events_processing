### Aca vamos avanzando con el proyecto..

## dump_DB
Hice el dump de eventos en archivos .parquet, y los subi a un bucket de GCloud:
  * Bucket: https://storage.googleapis.com/dump_events/
  * Archivo ejemplo: https://storage.googleapis.com/dump_events/events_20201101.parquet

La idea es poner en esta folder todos los archivos .parquet del bucket?.

Con estos archivos.parquet, tendriamos que generar nuestro dump SQL.

## scripts
  * `dump_SQL.py`: Script para generar el dump SQL a partir de los archivos .parquet
    * Tenemos que modificar este script para generar el dump SQL que necesitamos.
    * Este script se corre con:
```sh
python3 ./scripts/dump_SQL.py
```
  * `fetch_events.sh`: Script para exportar eventos de BigQuery (GA4) a Google Cloud Storage en formato Parquet
  * `fetch_events_2.sh`: Script para exportar eventos de BigQuery (GA3) a Google Cloud Storage en formato Parquet

## dump_SQL
  * Aca tendriamos que generar el/los dump/s SQL necesarios.
