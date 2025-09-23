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

## Data de eventos
### base_cols
```sql
"event_date"	"text"
"event_timestamp"	"bigint"
"event_name"	"text"
"user_pseudo_id"	"text"
"user_ltv"	"text"
"device"	"text"
"geo"	"text"
"ecommerce"	"text"
"items"	"text"
"user_first_touch_timestamp"	"bigint"
"platform"	"text"
"stream_id"	"bigint"
"event_bundle_sequence_id"	"bigint"
```

### Hay que separar algunas base_cols
```sql
-- user_ltv
user_ltv.revenue
user_ltv.currency

-- device
device.category
device.mobile_brand_name
device.mobile_model_name
device.mobile_marketing_name
device.mobile_os_hardware_model
device.operating_system
device.operating_system_version
device.vendor_id
device.advertising_id
device.language
device.is_limited_ad_tracking
device.time_zone_offset_seconds
device.web_info.browser
device.web_info.browser_version

-- geo
geo.continent
geo.sub_continent
geo.country
geo.region
geo.city
geo.metro

-- ecommerce
ecommerce.total_item_quantity
ecommerce.purchase_revenue_in_usd
ecommerce.purchase_revenue
ecommerce.refund_value_in_usd
ecommerce.refund_value
ecommerce.shipping_value_in_usd
ecommerce.shipping_value
ecommerce.tax_value_in_usd
ecommerce.tax_value
ecommerce.unique_items
ecommerce.transaction_id

-- items (este es un array, hay que ver como manejarlo)
items[0].item_id
items[0].item_name
items[0].item_brand
items[0].item_variant
items[0].item_category
items[0].item_category2
items[0].item_category3
items[0].item_category4
items[0].item_category5
items[0].price_in_usd
items[0].price
items[0].quantity
items[0].item_revenue_in_usd
items[0].item_revenue
items[0].item_refund_in_usd
items[0].item_refund
items[0].coupon
items[0].affiliation
items[0].location_id
items[0].item_list_id
items[0].item_list_name
items[0].item_list_index
items[0].promotion_id
items[0].promotion_name
items[0].creative_name
items[0].creative_slot
```

### event_params (opcionales - algunos suelen ser nulos)
```sql
"page_title"	"text"
"all_data"	"text"
"page_location"	"text"
"ga_session_number"	"real"
"clean_event"	"text"
"engaged_session_event"	"real"
"dclid"	"text"
"session_engaged"	"text"
"ga_session_id"	"real"
"debug_mode"	"real"
"entrances"	"real"
"engagement_time_msec"	"real"
"source"	"text"
"campaign"	"text"
"page_referrer"	"text"
"medium"	"text"
"term"	"text"
"percent_scrolled"	"real"
"search_term"	"text"
"unique_search_term"	"real"
"currency"	"text"
"outbound"	"text"
"link_domain"	"text"
"link_url"	"text"
"gclid"	"text"
"transaction_id"	"real"
"value"	"real"
"payment_type"	"text"
"tax"	"real"
```
