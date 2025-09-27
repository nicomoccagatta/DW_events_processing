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
  * Para correr el script `dump_SQL.py`, pgAdmin no puede cargarlo en memoria, por lo que debemos correrlo en una terminal.
```sh
psql -U postgres -d events_flat -f dump_SQL/dump.sql
```
  * Supongo que luego deberiamos correr el script `olap_final.sql` para generar las vistas OLAP con:
```sh
psql -U postgres -d events_flat -f dump_SQL/olap_final.sql
```

## Data de eventos
```sh
➜  Proyecto git:(main) ✗ python3 ./scripts/dump_SQL.py
✅ Dump generado
Filas: 1,101,492  Columnas: 68
SQLite: events_flat.db
Dump SQL: dump_SQL/dump.sql
Columnas: ['event_date', 'event_timestamp', 'event_name', 'user_pseudo_id', 'items', 'user_first_touch_timestamp', 'platform', 'stream_id', 'event_bundle_sequence_id', 'session_engaged', 'ga_session_number', 'page_location', 'ga_session_id', 'engaged_session_event', 'page_title', 'page_referrer', 'clean_event', 'engagement_time_msec', 'term', 'all_data', 'medium', 'source', 'campaign', 'debug_mode', 'transaction_id', 'value', 'payment_type', 'currency', 'tax', 'gclid', 'coupon', 'promotion_name', 'gclsrc', 'shipping_tier', 'dcclid', 'user_ltv_revenue', 'user_ltv_currency', 'device_category', 'device_mobile_brand_name', 'device_mobile_model_name', 'device_mobile_marketing_name', 'device_mobile_os_hardware_model', 'device_operating_system', 'device_operating_system_version', 'device_vendor_id', 'device_advertising_id', 'device_language', 'device_is_limited_ad_tracking', 'device_time_zone_offset_seconds', 'device_web_info_browser', 'device_web_info_browser_version', 'geo_continent', 'geo_sub_continent', 'geo_country', 'geo_region', 'geo_city', 'geo_metro', 'ecommerce_total_item_quantity', 'ecommerce_purchase_revenue_in_usd', 'ecommerce_purchase_revenue', 'ecommerce_refund_value_in_usd', 'ecommerce_refund_value', 'ecommerce_shipping_value_in_usd', 'ecommerce_shipping_value', 'ecommerce_tax_value_in_usd', 'ecommerce_tax_value', 'ecommerce_unique_items', 'ecommerce_transaction_id']
➜  Proyecto git:(main) ✗ ls -l dump_SQL/dump.sql
-rw-r--r--@ 1 nicomoccagatta  staff  3396285317 27 Sep 00:51 dump_SQL/dump.sql
```
