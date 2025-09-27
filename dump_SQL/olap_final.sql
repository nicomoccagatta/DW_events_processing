-- ===========================================================
-- ESQUEMA
-- ===========================================================
CREATE SCHEMA IF NOT EXISTS dw;
SET search_path = dw, public;

-- Limpieza opcional
DROP VIEW  IF EXISTS v_stg_items;
DROP VIEW  IF EXISTS v_stg_base;

DROP TABLE IF EXISTS ventas;
DROP TABLE IF EXISTS eventos;
DROP TABLE IF EXISTS pagina;
DROP TABLE IF EXISTS producto;
DROP TABLE IF EXISTS categoria;
DROP TABLE IF EXISTS dispositivo;
DROP TABLE IF EXISTS usuario;
DROP TABLE IF EXISTS fecha;

-- ===========================================================
-- 1) STAGING (VISTAS)
-- ===========================================================
CREATE OR REPLACE VIEW v_stg_base AS
SELECT
  e.event_date,                                      -- 'YYYYMMDD' (texto)
  e.event_timestamp::bigint         AS event_timestamp,
  e.event_name,
  e.user_pseudo_id,

  NULLIF(e.platform,'')             AS platform,
  NULLIF(e.page_title,'')           AS page_title,
  NULLIF(e.page_location,'')        AS page_location,
  NULLIF(e.page_referrer,'')        AS page_referrer,

  NULLIF(e.source,'')               AS source,
  NULLIF(e.medium,'')               AS medium,
  NULLIF(e.campaign,'')             AS campaign,

  -- engagement
  COALESCE(NULLIF(e.engagement_time_msec::text,'')::bigint,0) AS engagement_time_msec,
  COALESCE(NULLIF(e.percent_scrolled::text,'')::numeric, NULL) AS percent_scrolled,
  NULLIF(e.search_term,'')          AS search_term,

  -- ventas
  NULLIF(e.transaction_id::text,'') AS transaction_id,
  COALESCE(NULLIF(e.value::text,'')::numeric, NULL) AS value,
  COALESCE(NULLIF(e.tax::text,'')::numeric, NULL)   AS tax,
  NULLIF(e.currency,'')             AS currency,

  -- DEVICE: armamos un JSON desde columnas flat
  jsonb_build_object(
    'category',                 NULLIF(e.device_category,''),
    'mobile_brand_name',        NULLIF(e.device_mobile_brand_name,''),
    'mobile_model_name',        NULLIF(e.device_mobile_model_name,''),
    'operating_system',         NULLIF(e.device_operating_system,''),
    'operating_system_version', NULLIF(e.device_operating_system_version,''),
    'language',                 NULLIF(e.device_language,''),
    'web_info', jsonb_build_object(
        'browser',              NULLIF(e.device_web_info_browser,''),
        'browser_version',      NULLIF(e.device_web_info_browser_version,'')
    )
  )                               AS j_device,

  -- GEO: idem
  jsonb_build_object(
    'continent',     NULLIF(e.geo_continent,''),
    'sub_continent', NULLIF(e.geo_sub_continent,''),
    'country',       NULLIF(e.geo_country,''),
    'region',        NULLIF(e.geo_region,''),
    'city',          NULLIF(e.geo_city,''),
    'metro',         NULLIF(e.geo_metro,'')
  )                               AS j_geo,

  -- ITEMS: usar SOLO si realmente es un array
  CASE
    WHEN e.items IS NOT NULL
         AND btrim(e.items) <> ''
         AND left(btrim(e.items),1) = '['
    THEN e.items::jsonb
    ELSE '[]'::jsonb
  END                             AS j_items

FROM public.events_flat e;

-- 1 fila por ítem (solo para purchase)
CREATE OR REPLACE VIEW v_stg_items AS
SELECT
  b.*,
  itm.value::jsonb AS j_item
FROM v_stg_base b
JOIN LATERAL jsonb_array_elements(b.j_items) itm ON TRUE
WHERE b.event_name = 'purchase';

-- ===========================================================
-- 2) DIMENSIONES
-- ===========================================================

-- Fecha
CREATE TABLE fecha (
  id                   integer PRIMARY KEY,        -- <-- YYYYMMDD
  fecha                date NOT NULL UNIQUE,
  dia                  int,
  mes                  int,
  año                  int,
  dia_mes              int,
  dia_semana           int,      -- 0=domingo..6=sábado
  semana_año           int,
  mes_numero           int,
  numero_cuatrimestre  int,
  fin_de_semana        boolean,
  feriado              boolean
);


-- Usuario (lo que hay: id anónimo + geografía)
CREATE TABLE usuario (
  id         bigserial PRIMARY KEY,
  nombre     text,
  tipo       text,       -- si luego distinguís anon/registrado
  categoria  text,
  direccion  text,
  ciudad     text,
  provincia  text,
  pais       text,
  region     text,
  user_key   text NOT NULL UNIQUE    -- user_pseudo_id
);

-- Dispositivo
CREATE TABLE dispositivo (
  id                bigserial PRIMARY KEY,
  nombre            text,
  tipo              text,            -- device_category
  sistema_operativo text,
  navegador         text,
  canal             text,            -- platform
  canal_nombre      text,            -- browser_version o brand (a gusto)
  brand             text,
  model             text,
  idioma            text,
  UNIQUE (tipo, sistema_operativo, navegador, canal, canal_nombre, brand, model, idioma)
);

-- Categoría
CREATE TABLE categoria (
  id          bigserial PRIMARY KEY,
  nombre      text NOT NULL UNIQUE,
  descripcion text
);

-- Producto (SCD2 light)
CREATE TABLE producto (
  id                      bigserial PRIMARY KEY,
  categoria_id            bigint REFERENCES categoria(id),
  nombre                  text,
  precio                  numeric,
  descripcion             text,
  producto_activo_desde   timestamp NOT NULL,
  producto_activo_hasta   timestamp,
  sku                     text,                      -- item_id (clave de negocio)
  is_current              boolean DEFAULT true,
  flag_cambio_precio      boolean DEFAULT false
);

-- Una sola fila vigente por SKU
CREATE UNIQUE INDEX ux_producto_sku_current
  ON producto (sku)
  WHERE producto_activo_hasta IS NULL;

-- Historia por SKU
CREATE INDEX ix_producto_hist
  ON producto (sku, producto_activo_desde, producto_activo_hasta);

--  evitar duplicar versiones por misma clave de inicio
CREATE UNIQUE INDEX ux_producto_sku_desde
  ON producto (sku, producto_activo_desde);

-- Página
CREATE TABLE pagina (
  id             bigserial PRIMARY KEY,
  nombre         text,
  url            text UNIQUE,
  tipo           text,          -- page tipo (si lo definís más tarde)
  seccion_id     bigint,
  seccion_nombre text
);

-- ===========================================================
-- 3) POBLADO DE DIMENSIONES
-- ===========================================================

-- Fecha desde datos
INSERT INTO fecha (
  id, fecha, dia, mes, año, dia_mes, dia_semana, semana_año,
  mes_numero, numero_cuatrimestre, fin_de_semana, feriado
)
SELECT DISTINCT
  (event_date)::int                                                AS id,          -- YYYYMMDD
  to_date(event_date, 'YYYYMMDD')                                  AS fecha,
  EXTRACT(DAY   FROM to_date(event_date,'YYYYMMDD'))::int          AS dia,
  EXTRACT(MONTH FROM to_date(event_date,'YYYYMMDD'))::int          AS mes,
  EXTRACT(YEAR  FROM to_date(event_date,'YYYYMMDD'))::int          AS año,
  EXTRACT(DAY   FROM to_date(event_date,'YYYYMMDD'))::int          AS dia_mes,
  EXTRACT(DOW   FROM to_date(event_date,'YYYYMMDD'))::int          AS dia_semana,
  EXTRACT(WEEK  FROM to_date(event_date,'YYYYMMDD'))::int          AS semana_año,
  EXTRACT(MONTH FROM to_date(event_date,'YYYYMMDD'))::int          AS mes_numero,
  ( ((EXTRACT(MONTH FROM to_date(event_date,'YYYYMMDD'))::int - 1) / 4) + 1 )::int AS numero_cuatrimestre,
  (EXTRACT(DOW   FROM to_date(event_date,'YYYYMMDD')) IN (0,6))    AS fin_de_semana,
  NULL::boolean                                                    AS feriado
FROM v_stg_base
ON CONFLICT (id) DO NOTHING;

-- Usuario
INSERT INTO usuario (user_key, ciudad, provincia, pais, region, tipo)
SELECT DISTINCT
  b.user_pseudo_id,
  NULLIF(b.j_geo->>'city',''),
  NULLIF(b.j_geo->>'region',''),
  NULLIF(b.j_geo->>'country',''),
  NULLIF(b.j_geo->>'sub_continent',''),
  'anon'
FROM v_stg_base b
WHERE b.user_pseudo_id IS NOT NULL
ON CONFLICT (user_key) DO NOTHING;

-- Dispositivo
TRUNCATE dw.dispositivo RESTART IDENTITY;

INSERT INTO dw.dispositivo
  (nombre, tipo, sistema_operativo, navegador, canal, canal_nombre, brand, model, idioma)
SELECT DISTINCT
  NULL AS nombre,

  -- tipo (desktop/mobile/tablet)
  NULLIF(b.j_device->>'category','') AS tipo,

  -- SO normalizado
  CASE
    WHEN NULLIF(b.j_device->>'operating_system','') IN ('', '<Other>', 'Web') THEN NULL
    WHEN b.j_device->>'operating_system' ILIKE 'Macintosh' THEN 'macOS'
    ELSE b.j_device->>'operating_system'
  END AS sistema_operativo,

  -- navegador limpio
  CASE
    WHEN NULLIF(b.j_device->'web_info'->>'browser','') IN ('', '<Other>') THEN NULL
    ELSE b.j_device->'web_info'->>'browser'
  END AS navegador,

  -- canal/plataforma
  NULLIF(b.platform,'') AS canal,

  -- versión mayor del navegador
  NULLIF(split_part(b.j_device->'web_info'->>'browser_version','.',1),'') AS canal_nombre,

  -- brand limpio
  CASE
    WHEN NULLIF(b.j_device->>'mobile_brand_name','') IN ('', '<Other>') THEN NULL
    ELSE b.j_device->>'mobile_brand_name'
  END AS brand,

  -- model limpio (evita valores que en realidad son navegadores)
  CASE
    WHEN NULLIF(b.j_device->>'mobile_model_name','') IN ('', '<Other>') THEN NULL
    WHEN b.j_device->>'mobile_model_name' IN ('Chrome','Safari','Edge','Firefox') THEN NULL
    ELSE b.j_device->>'mobile_model_name'
  END AS model,

  -- idioma a 2 letras
  CASE
    WHEN NULLIF(b.j_device->>'language','') IN ('', '<Other>') THEN NULL
    ELSE left(lower(b.j_device->>'language'),2)
  END AS idioma
FROM dw.v_stg_base b;


-- Categoría (desde items)
INSERT INTO categoria (nombre)
SELECT DISTINCT
  NULLIF(j_item->>'item_category','')
FROM v_stg_items
WHERE NULLIF(j_item->>'item_category','') IS NOT NULL
ON CONFLICT (nombre) DO NOTHING;

-- Producto (SCD2 por precio: historia completa)
WITH items AS (
  SELECT
    (s.j_item->>'item_id')::text                  AS sku,
    s.j_item->>'item_name'                        AS nombre,
    NULLIF(s.j_item->>'item_category','')         AS categoria_nombre,
    s.j_item->>'item_brand'                       AS marca_desc,   -- va a descripcion
    NULLIF(s.j_item->>'price','')::numeric        AS precio,
    to_timestamp(s.event_timestamp/1000000.0)     AS ts
  FROM v_stg_items s
  WHERE s.event_name = 'purchase'
    AND NULLIF(s.j_item->>'price','') IS NOT NULL
),
ordenado AS (
  SELECT
    i.*,
    LAG(precio) OVER (PARTITION BY sku ORDER BY ts) AS prev_precio
  FROM items i
),
cambios AS (
  -- primera aparición de cada SKU + cada cambio de precio
  SELECT *
  FROM ordenado
  WHERE prev_precio IS DISTINCT FROM precio
),
versiones AS (
  -- ventanas [desde, hasta_siguiente)
  SELECT
    sku, nombre, categoria_nombre, marca_desc, precio,
    ts AS desde,
    LEAD(ts) OVER (PARTITION BY sku ORDER BY ts) AS hasta_siguiente,
    CASE WHEN LAG(precio) OVER (PARTITION BY sku ORDER BY ts) IS NULL
         THEN false ELSE true END AS flag_cambio_precio
  FROM cambios
),
final AS (
  SELECT
    sku, nombre, categoria_nombre, marca_desc, precio,
    desde                                        AS producto_activo_desde,
    (hasta_siguiente - interval '1 microsecond') AS producto_activo_hasta,
    flag_cambio_precio,
    (hasta_siguiente IS NULL)                    AS is_current
  FROM versiones
)
INSERT INTO producto (
  categoria_id, nombre, precio, descripcion,
  producto_activo_desde, producto_activo_hasta,
  sku, is_current, flag_cambio_precio
)
SELECT
  c.id,
  f.nombre,
  f.precio,
  f.marca_desc,
  f.producto_activo_desde,
  f.producto_activo_hasta,
  f.sku,
  f.is_current,
  f.flag_cambio_precio
FROM final f
LEFT JOIN categoria c ON c.nombre = f.categoria_nombre;

-- Página (URL canónica, sin producto_id)
INSERT INTO pagina (nombre, url, tipo, seccion_id, seccion_nombre)
WITH base AS (
  SELECT
    NULLIF(page_title,'')    AS page_title_raw,
    NULLIF(page_location,'') AS url_raw
  FROM v_stg_base
  WHERE NULLIF(page_location,'') IS NOT NULL
),
canon AS (
  SELECT
    lower(
      regexp_replace(
        split_part(regexp_replace(url_raw, '^https?://(www\.)?', ''), '?', 1),
        '/+$|\\.+$',''
      )
    ) AS url_can,
    page_title_raw AS page_title
  FROM base
),
agg AS (
  SELECT
    url_can                                                AS url,
    NULLIF( mode() WITHIN GROUP (ORDER BY page_title), '' ) AS nombre
  FROM canon
  GROUP BY url_can
)
SELECT
  a.nombre,
  a.url,
  NULL::text,
  NULL::bigint,
  NULL::text
FROM agg a
ON CONFLICT (url) DO UPDATE
  SET nombre = COALESCE(EXCLUDED.nombre, pagina.nombre);

-- ===========================================================
-- 4) TABLAS DE HECHOS
-- ===========================================================

-- Eventos (grano: 1 fila por evento)
CREATE TABLE eventos (
  evento_id   bigserial PRIMARY KEY,
  venta_id    bigint,                          -- FK a ventas si querés linkear después
  dispositivo_id bigint REFERENCES dispositivo(id),
  fecha_id    int    REFERENCES fecha(id),
  pagina_id   bigint REFERENCES pagina(id),
  producto_id bigint REFERENCES producto(id),
  usuario_id  bigint REFERENCES usuario(id),
  tipo        text,    -- event_name
  cantidad    numeric
);

-- Ventas (grano: 1 fila por ÍTEM en purchase)
CREATE TABLE ventas (
  venta_id      bigserial PRIMARY KEY,
  dispositivo_id bigint REFERENCES dispositivo(id),
  fecha_id      int    REFERENCES fecha(id),
  producto_id   bigint REFERENCES producto(id),
  usuario_id    bigint REFERENCES usuario(id),
  orden_id      text,            -- transaction_id
  cantidad      numeric,
  monto         numeric,
  transaccion_exitosa boolean,
  descuento     numeric
);

-- ===========================================================
-- 5) CARGA HECHOS
-- ===========================================================

-- Ventas (a nivel ítem)
INSERT INTO ventas (dispositivo_id, fecha_id, producto_id, usuario_id, orden_id, cantidad, monto, transaccion_exitosa, descuento)
SELECT
  d.id                                                AS dispositivo_id,
  f.id                                                AS fecha_id,
  pr.id                                               AS producto_id,
  u.id                                                AS usuario_id,
  b.transaction_id                                    AS orden_id,
  NULLIF(b.j_item->>'quantity','')::numeric          AS cantidad,
  COALESCE(NULLIF(b.j_item->>'item_revenue','')::numeric,
           NULLIF(b.j_item->>'price','')::numeric)    AS monto,
  TRUE                                                AS transaccion_exitosa,
  0::numeric                                                   AS descuento
FROM v_stg_items b
CROSS JOIN LATERAL (
  SELECT
    NULLIF(b.j_device->>'category','') AS tipo,
    CASE
      WHEN NULLIF(b.j_device->>'operating_system','') IN ('', '<Other>', 'Web') THEN NULL
      WHEN b.j_device->>'operating_system' ILIKE 'Macintosh' THEN 'macOS'
      ELSE b.j_device->>'operating_system'
    END AS sistema_operativo,
    CASE
      WHEN NULLIF(b.j_device->'web_info'->>'browser','') IN ('', '<Other>') THEN NULL
      ELSE b.j_device->'web_info'->>'browser'
    END AS navegador,
    NULLIF(b.platform,'') AS canal,
    NULLIF(split_part(b.j_device->'web_info'->>'browser_version','.',1),'') AS canal_nombre,
    CASE
      WHEN NULLIF(b.j_device->>'mobile_brand_name','') IN ('', '<Other>') THEN NULL
      ELSE b.j_device->>'mobile_brand_name'
    END AS brand,
    CASE
      WHEN NULLIF(b.j_device->>'mobile_model_name','') IN ('', '<Other>') THEN NULL
      WHEN b.j_device->>'mobile_model_name' IN ('Chrome','Safari','Edge','Firefox') THEN NULL
      ELSE b.j_device->>'mobile_model_name'
    END AS model,
    CASE
      WHEN NULLIF(b.j_device->>'language','') IN ('', '<Other>') THEN NULL
      ELSE left(lower(b.j_device->>'language'),2)
    END AS idioma
) nd
LEFT JOIN dispositivo d
  ON d.tipo = nd.tipo
 AND d.sistema_operativo = nd.sistema_operativo
 AND d.navegador = nd.navegador
 AND d.canal = nd.canal
 AND d.canal_nombre = nd.canal_nombre
 AND d.brand = nd.brand
 AND d.model = nd.model
 AND d.idioma = nd.idioma
LEFT JOIN fecha f   ON f.fecha = to_date(b.event_date,'YYYYMMDD')
LEFT JOIN usuario u ON u.user_key = b.user_pseudo_id
LEFT JOIN producto pr ON pr.sku = b.j_item->>'item_id'
                      AND pr.is_current = true
WHERE b.event_name = 'purchase'
  AND b.transaction_id IS NOT NULL;
  
-- Eventos (con link a ventas cuando hay orden)
INSERT INTO eventos (venta_id, dispositivo_id, fecha_id, pagina_id, producto_id, usuario_id, tipo, cantidad)
WITH map_venta AS (
  -- una venta_id representativa por orden (si hay varias líneas, tomo la mínima)
  SELECT orden_id, MIN(venta_id) AS venta_id
  FROM ventas
  GROUP BY orden_id
)
SELECT
  mv.venta_id,                                         -- << se completa si hubo venta
  d.id                         AS dispositivo_id,
  f.id                         AS fecha_id,
  p.id                         AS pagina_id,
  NULL::bigint                 AS producto_id,
  u.id                         AS usuario_id,
  b.event_name                 AS tipo,
  COALESCE(NULLIF(b.percent_scrolled,0), NULL)::numeric AS cantidad
FROM v_stg_base b
CROSS JOIN LATERAL (
  SELECT
    NULLIF(b.j_device->>'category','') AS tipo,
    CASE
      WHEN NULLIF(b.j_device->>'operating_system','') IN ('', '<Other>', 'Web') THEN NULL
      WHEN b.j_device->>'operating_system' ILIKE 'Macintosh' THEN 'macOS'
      ELSE b.j_device->>'operating_system'
    END AS sistema_operativo,
    CASE
      WHEN NULLIF(b.j_device->'web_info'->>'browser','') IN ('', '<Other>') THEN NULL
      ELSE b.j_device->'web_info'->>'browser'
    END AS navegador,
    NULLIF(b.platform,'') AS canal,
    NULLIF(split_part(b.j_device->'web_info'->>'browser_version','.',1),'') AS canal_nombre,
    CASE
      WHEN NULLIF(b.j_device->>'mobile_brand_name','') IN ('', '<Other>') THEN NULL
      ELSE b.j_device->>'mobile_brand_name'
    END AS brand,
    CASE
      WHEN NULLIF(b.j_device->>'mobile_model_name','') IN ('', '<Other>') THEN NULL
      WHEN b.j_device->>'mobile_model_name' IN ('Chrome','Safari','Edge','Firefox') THEN NULL
      ELSE b.j_device->>'mobile_model_name'
    END AS model,
    CASE
      WHEN NULLIF(b.j_device->>'language','') IN ('', '<Other>') THEN NULL
      ELSE left(lower(b.j_device->>'language'),2)
    END AS idioma
) nd
LEFT JOIN dispositivo d
  ON d.tipo = nd.tipo
 AND d.sistema_operativo = nd.sistema_operativo
 AND d.navegador = nd.navegador
 AND d.canal = nd.canal
 AND d.canal_nombre = nd.canal_nombre
 AND d.brand = nd.brand
 AND d.model = nd.model
 AND d.idioma = nd.idioma
LEFT JOIN fecha  f ON f.fecha = to_date(b.event_date,'YYYYMMDD')
CROSS JOIN LATERAL (
  SELECT lower(
           regexp_replace(
             split_part(regexp_replace(b.page_location, '^https?://(www\.)?', ''), '?', 1),
             '/+$|\\.+$',''
           )
         ) AS url_can
) urln
LEFT JOIN pagina p ON p.url = urln.url_can
LEFT JOIN usuario u ON u.user_key = b.user_pseudo_id
LEFT JOIN map_venta mv ON mv.orden_id = b.transaction_id;
-- =====================================================================
-- 6) CHEQUEOS RÁPIDOS
-- =====================================================================
-- Cantidad de filas cargadas
SELECT 'dim_fecha'    AS tabla, COUNT(*) FROM Fecha
UNION ALL SELECT 'dim_usuario', COUNT(*) FROM Usuario
UNION ALL SELECT 'dim_dispositivo', COUNT(*) FROM Dispositivo
UNION ALL SELECT 'dim_categoria', COUNT(*) FROM Categoria
UNION ALL SELECT 'dim_producto', COUNT(*) FROM  Producto
UNION ALL SELECT 'dim_pagina', COUNT(*) FROM Pagina
UNION ALL SELECT 'fact_eventos', COUNT(*) FROM Eventos
UNION ALL SELECT 'fact_ventas', COUNT(*) FROM Ventas;
select * from pagina order by nombre desc;
SELECT *
FROM producto;
select * from ventas;
select * from dispositivo;
select * from fecha;