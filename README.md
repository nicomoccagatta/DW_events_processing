### Aca vamos avanzando con el proyecto..

## input_DB
Hice el dump de eventos en archivos .parquet, y tambien los subi a un bucket de GCloud:
  * Bucket: https://storage.googleapis.com/dump_events/
  * Archivo ejemplo: https://storage.googleapis.com/dump_events/events_20201101.parquet
  * Estos archivos los genere con el script `scripts/fetch_events.sh`, que usa la herramienta de linea de comandos `bq` para exportar los eventos desde BigQuery (GA4) a Google Cloud Storage en formato Parquet.
  * Con estos archivos.parquet, generamos nuestro dump SQL.

## scripts
  * `dump_SQL.py`: Script para generar el dump SQL a partir de los archivos .parquet
    * Este script se corre con:
```sh
➜  Proyecto git:(main) ✗ python3 ./scripts/dump_SQL.py
✅ Dump generado
Filas: 1,101,492  Columnas: 68
SQLite: events_flat.db
Dump SQL: dump_SQL/dump.sql
Columnas: ['event_date', 'event_timestamp', 'event_name', 'user_pseudo_id', 'items', 'user_first_touch_timestamp', 'platform', 'stream_id', 'event_bundle_sequence_id', 'session_engaged', 'ga_session_number', 'page_location', 'ga_session_id', 'engaged_session_event', 'page_title', 'page_referrer', 'clean_event', 'engagement_time_msec', 'term', 'all_data', 'medium', 'source', 'campaign', 'debug_mode', 'transaction_id', 'value', 'payment_type', 'currency', 'tax', 'gclid', 'coupon', 'promotion_name', 'gclsrc', 'shipping_tier', 'dcclid', 'user_ltv_revenue', 'user_ltv_currency', 'device_category', 'device_mobile_brand_name', 'device_mobile_model_name', 'device_mobile_marketing_name', 'device_mobile_os_hardware_model', 'device_operating_system', 'device_operating_system_version', 'device_vendor_id', 'device_advertising_id', 'device_language', 'device_is_limited_ad_tracking', 'device_time_zone_offset_seconds', 'device_web_info_browser', 'device_web_info_browser_version', 'geo_continent', 'geo_sub_continent', 'geo_country', 'geo_region', 'geo_city', 'geo_metro', 'ecommerce_total_item_quantity', 'ecommerce_purchase_revenue_in_usd', 'ecommerce_purchase_revenue', 'ecommerce_refund_value_in_usd', 'ecommerce_refund_value', 'ecommerce_shipping_value_in_usd', 'ecommerce_shipping_value', 'ecommerce_tax_value_in_usd', 'ecommerce_tax_value', 'ecommerce_unique_items', 'ecommerce_transaction_id']
```

  * `fetch_events.sh`: Script para exportar eventos de BigQuery (GA4) a Google Cloud Storage en formato Parquet

## dump_SQL
  * Aca tenemos los dumps SQL para generar nuestra base de datos.
  * `dump_SQL/01-dump.sql`: Script SQL para crear la tabla `events_flat` y cargar los datos.
  * Para correr el script `dump_SQL/01-dump.sql`, pgAdmin no puede cargarlo en memoria, por lo que debemos correrlo en una terminal.

```sh
psql -U postgres -d events_flat -f dump_SQL/01-dump.sql
```

  * Este script genera la tabla `events_flat` con todos los eventos.
  * `dump_SQL/02-olap.sql`: Script SQL para crear vistas OLAP a partir de la tabla `events_flat`.
  * Luego deberiamos correr el script `dump_SQL/02-olap.sql` para generar las vistas OLAP `categoria, dispositivo, eventos, fecha, pagina, producto, usuario, ventas`.

```sh
psql -U postgres -d events_flat -f dump_SQL/02-olap.sql
```

## dump_SQL/schemas_dumps
Aca tenemos los esquemas de las tablas generadas en el dump SQL.
  * `dump_SQL/schemas_dumps/public_schema.sql`: Esquema `public` con la data `events_flat`.
  * `dump_SQL/schemas_dumps/dw_schema.sql`: Esquema `dw` de las vistas OLAP generadas `categoria, dispositivo, eventos, fecha, pagina, producto, usuario, ventas`.

![](https://raw.githubusercontent.com/nicomoccagatta/DW_events_processing/refs/heads/main/dump_SQL/schemas_dumps/public_schema.png?raw=true)

![](https://raw.githubusercontent.com/nicomoccagatta/DW_events_processing/refs/heads/main/dump_SQL/schemas_dumps/dw_schema.png?raw=true)


## queries/analysis.sql
* Aca tenemos queries de analisis de los datos.

## Data de eventos
* En `misc/events.json` tenemos un ejemplo de eventos en formato JSON, que es el formato original en que se encuentran los eventos en GA4.
