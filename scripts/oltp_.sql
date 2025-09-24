CREATE SCHEMA IF NOT EXISTS web_oltp;
SET search_path = web_oltp, public;


CREATE OR REPLACE VIEW v_events_stg AS
SELECT
  -- claves base
  e.event_timestamp,
  e.event_name,
  e.user_pseudo_id,

  -- sesión (en GA4 viene como number/float/string a veces)
  ((NULLIF(e.ga_session_id::text,''))::float8)::bigint              AS ga_session_id,
  ((NULLIF(e.ga_session_number::text,''))::float8)::int             AS ga_session_number,

  -- página / website
  e.page_title,
  e.page_location,
  e.page_referrer,
  -- dominio (website) desde la URL
  NULLIF(regexp_replace(e.page_location,
         '^https?://([^/]+).*$', '\1'), '')          AS website_domain,

  -- tráfico
  NULLIF(e.source,'')                                AS source,
  NULLIF(e.medium,'')                                AS medium,
  NULLIF(e.campaign,'')                              AS campaign,
  NULLIF(e.term,'')                                  AS term,
  NULLIF(e.gclid,'')                                 AS gclid,
  NULLIF(e.dclid,'')                                 AS dclid,
  NULLIF(e.outbound,'')                              AS outbound,

  -- engagement / métricas de evento
  NULLIF(e.clean_event,'')                           AS clean_event,
  COALESCE((((NULLIF(e.engagement_time_msec::text,''))::float8)::bigint), 0) AS engagement_time_msec,
  COALESCE(NULLIF(e.percent_scrolled::text,'' )::numeric, NULL)::numeric AS percent_scrolled,
  NULLIF(e.search_term,'')                           AS search_term,

  -- orden/venta
  NULLIF(e.transaction_id::text,'')                  AS transaction_id,
  NULLIF(e.currency,'')                              AS currency,
  NULLIF(e.payment_type,'')                          AS payment_type,
  COALESCE(NULLIF(e.value::text,'')::numeric, NULL)  AS value,
  COALESCE(NULLIF(e.tax::text,'')::numeric, NULL)    AS tax,

  -- DEVICE (json)
  (e.device::jsonb)                                  AS j_device,
  -- GEO (json)
  (e.geo::jsonb)                                     AS j_geo

FROM public.events_20201101_flat e;

-- Usuarios
CREATE TABLE IF NOT EXISTS user_account (
  user_id           bigserial PRIMARY KEY,
  user_pseudo_id    text NOT NULL UNIQUE,
  user_type         text  -- opcional si más tarde distinguís registrado/anon
);

-- Websites
CREATE TABLE IF NOT EXISTS website (
  website_id     bigserial PRIMARY KEY,
  domain         text NOT NULL UNIQUE,
  name           text
);

-- Páginas
CREATE TABLE IF NOT EXISTS page (
  page_id        bigserial PRIMARY KEY,
  website_id     bigint NOT NULL REFERENCES website(website_id),
  title          text,
  url            text NOT NULL UNIQUE,
  referrer       text
);

-- Dispositivos
CREATE TABLE IF NOT EXISTS device (
  device_id      bigserial PRIMARY KEY,
  category       text,
  brand          text,
  model          text,
  os             text,
  os_version     text,
  browser        text,
  browser_ver    text,
  language       text,
  UNIQUE(category, brand, model, os, os_version, browser, browser_ver, language)
);

-- Geo
CREATE TABLE IF NOT EXISTS geo (
  geo_id         bigserial PRIMARY KEY,
  continent      text,
  sub_continent  text,
  country        text,
  region         text,
  city           text,
  metro          text,
  UNIQUE(continent, sub_continent, country, region, city, metro)
);

-- Fuente / Tráfico
CREATE TABLE IF NOT EXISTS traffic_source (
  traffic_id     bigserial PRIMARY KEY,
  source         text,
  medium         text,
  campaign       text,
  term           text,
  gclid          text,
  dclid          text,
  outbound       text,
  UNIQUE(source, medium, campaign, term, gclid, dclid, outbound)
);

-- Categoría de Evento (catálogo simple, si querés mapear por nombre)
CREATE TABLE IF NOT EXISTS event_category (
  event_category_id bigserial PRIMARY KEY,
  name              text NOT NULL UNIQUE
);

-- Usuarios
INSERT INTO user_account (user_pseudo_id)
SELECT DISTINCT user_pseudo_id
FROM v_events_stg
WHERE user_pseudo_id IS NOT NULL
ON CONFLICT (user_pseudo_id) DO NOTHING;

-- Websites
INSERT INTO website (domain)
SELECT DISTINCT website_domain
FROM v_events_stg
WHERE website_domain IS NOT NULL
ON CONFLICT (domain) DO NOTHING;

-- Páginas
INSERT INTO page (website_id, title, url, referrer)
SELECT DISTINCT w.website_id, s.page_title, s.page_location, s.page_referrer
FROM v_events_stg s
JOIN website w ON w.domain = s.website_domain
WHERE s.page_location IS NOT NULL
ON CONFLICT (url) DO NOTHING;

-- Dispositivos (parseo JSON)
INSERT INTO device (category, brand, model, os, os_version, browser, browser_ver, language)
SELECT DISTINCT
  s.j_device->>'category',
  s.j_device->>'mobile_brand_name',
  s.j_device->>'mobile_model_name',
  s.j_device->>'operating_system',
  s.j_device->>'operating_system_version',
  s.j_device->'web_info'->>'browser',
  s.j_device->'web_info'->>'browser_version',
  s.j_device->>'language'
FROM v_events_stg s
WHERE s.j_device IS NOT NULL
ON CONFLICT (category, brand, model, os, os_version, browser, browser_ver, language) DO NOTHING;

-- Geo
INSERT INTO geo (continent, sub_continent, country, region, city, metro)
SELECT DISTINCT
  s.j_geo->>'continent',
  s.j_geo->>'sub_continent',
  s.j_geo->>'country',
  s.j_geo->>'region',
  s.j_geo->>'city',
  s.j_geo->>'metro'
FROM v_events_stg s
WHERE s.j_geo IS NOT NULL
ON CONFLICT (continent, sub_continent, country, region, city, metro) DO NOTHING;

-- Tráfico
INSERT INTO traffic_source (source, medium, campaign, term, gclid, dclid, outbound)
SELECT DISTINCT source, medium, campaign, term, gclid, dclid, outbound
FROM v_events_stg
ON CONFLICT (source, medium, campaign, term, gclid, dclid, outbound) DO NOTHING;

-- Categoría de evento (si querés 1:1 con el nombre del evento)
INSERT INTO event_category (name)
SELECT DISTINCT event_name FROM v_events_stg
ON CONFLICT (name) DO NOTHING;

-- Sesiones
CREATE TABLE IF NOT EXISTS session (
  session_pk        bigserial PRIMARY KEY,
  user_id           bigint NOT NULL REFERENCES user_account(user_id),
  ga_session_id     bigint NOT NULL,
  ga_session_number int,
  engaged_flag      boolean,
  entrances         int,
  debug_mode        boolean,
  total_eng_time_ms bigint,
  UNIQUE(user_id, ga_session_id, ga_session_number)
);

-- Cargar sesiones (agrego engagement/entrances agregando por sesión)
WITH base AS (
  SELECT
    u.user_id,
    s.ga_session_id,
    s.ga_session_number,
    bool_or(COALESCE(s.engagement_time_msec,0) > 0) AS engaged_flag,
    max(CASE WHEN s.clean_event = 'gtm.js' OR s.event_name='session_start'
             THEN 1 ELSE 0 END)                     AS entrances,
    bool_or(FALSE)                                   AS debug_mode, -- si tenés flag
    sum(COALESCE(s.engagement_time_msec,0))          AS total_eng_time_ms
  FROM v_events_stg s
  JOIN user_account u ON u.user_pseudo_id = s.user_pseudo_id
  WHERE s.ga_session_id IS NOT NULL
  GROUP BY u.user_id, s.ga_session_id, s.ga_session_number
)
INSERT INTO session (user_id, ga_session_id, ga_session_number, engaged_flag, entrances, debug_mode, total_eng_time_ms)
SELECT * FROM base
ON CONFLICT (user_id, ga_session_id, ga_session_number) DO NOTHING;

-- Eventos (cada fila del flat normalizada y referenciada)
CREATE TABLE IF NOT EXISTS event (
  event_id          bigserial PRIMARY KEY,
  session_pk        bigint REFERENCES session(session_pk),
  user_id           bigint NOT NULL REFERENCES user_account(user_id),
  event_name        text   NOT NULL,
  event_timestamp   bigint NOT NULL,
  page_id           bigint REFERENCES page(page_id),
  device_id         bigint REFERENCES device(device_id),
  geo_id            bigint REFERENCES geo(geo_id),
  traffic_id        bigint REFERENCES traffic_source(traffic_id),
  event_category_id bigint REFERENCES event_category(event_category_id),

  clean_event       text,
  engagement_time_msec bigint,
  percent_scrolled  numeric,
  search_term       text
);

INSERT INTO event (
  session_pk, user_id, event_name, event_timestamp,
  page_id, device_id, geo_id, traffic_id, event_category_id,
  clean_event, engagement_time_msec, percent_scrolled, search_term
)
SELECT
  s2.session_pk,
  u.user_id,
  stg.event_name,
  stg.event_timestamp,
  p.page_id,
  d.device_id,
  g.geo_id,
  t.traffic_id,
  ec.event_category_id,
  stg.clean_event,
  stg.engagement_time_msec,
  stg.percent_scrolled,
  stg.search_term
FROM v_events_stg stg
JOIN user_account u  ON u.user_pseudo_id = stg.user_pseudo_id
LEFT JOIN session s2 ON s2.user_id = u.user_id
                    AND s2.ga_session_id = stg.ga_session_id
                    AND s2.ga_session_number = stg.ga_session_number
LEFT JOIN website w  ON w.domain = stg.website_domain
LEFT JOIN page p     ON p.url = stg.page_location
LEFT JOIN device d   ON d.category   = stg.j_device->>'category'
                    AND d.brand      = stg.j_device->>'mobile_brand_name'
                    AND d.model      = stg.j_device->>'mobile_model_name'
                    AND d.os         = stg.j_device->>'operating_system'
                    AND d.os_version = stg.j_device->>'operating_system_version'
                    AND d.browser    = stg.j_device->'web_info'->>'browser'
                    AND d.browser_ver= stg.j_device->'web_info'->>'browser_version'
                    AND d.language   = stg.j_device->>'language'
LEFT JOIN geo g      ON g.continent     = stg.j_geo->>'continent'
                    AND g.sub_continent = stg.j_geo->>'sub_continent'
                    AND g.country       = stg.j_geo->>'country'
                    AND g.region        = stg.j_geo->>'region'
                    AND g.city          = stg.j_geo->>'city'
                    AND g.metro         = stg.j_geo->>'metro'
LEFT JOIN traffic_source t
                  ON t.source = stg.source AND t.medium = stg.medium
                 AND t.campaign = stg.campaign AND t.term = stg.term
                 AND t.gclid = stg.gclid AND t.dclid = stg.dclid
                 AND t.outbound = stg.outbound
LEFT JOIN event_category ec ON ec.name = stg.event_name;

--CHEKSS
-- Total por tabla
SELECT 'user_account'    AS tabla, COUNT(*) FROM web_oltp.user_account
UNION ALL SELECT 'website',        COUNT(*) FROM web_oltp.website
UNION ALL SELECT 'page',           COUNT(*) FROM web_oltp.page
UNION ALL SELECT 'device',         COUNT(*) FROM web_oltp.device
UNION ALL SELECT 'geo',            COUNT(*) FROM web_oltp.geo
UNION ALL SELECT 'traffic_source', COUNT(*) FROM web_oltp.traffic_source
UNION ALL SELECT 'event_category', COUNT(*) FROM web_oltp.event_category
UNION ALL SELECT 'session',        COUNT(*) FROM web_oltp.session
UNION ALL SELECT 'event',          COUNT(*) FROM web_oltp.event;

SELECT * FROM web_oltp.user_account    LIMIT 10;
SELECT * FROM web_oltp.website         LIMIT 10;
SELECT * FROM web_oltp.page            LIMIT 10;
SELECT * FROM web_oltp.device          LIMIT 10;
SELECT * FROM web_oltp.geo             LIMIT 10;
SELECT * FROM web_oltp.traffic_source  LIMIT 10;
SELECT * FROM web_oltp.event_category  LIMIT 10;
SELECT * FROM web_oltp.session         LIMIT 10;
SELECT * FROM web_oltp.event           LIMIT 10;


-- Top páginas por cantidad de eventos
SELECT p.url, COUNT(*) AS evs
FROM web_oltp.event e
JOIN web_oltp.page p ON p.page_id = e.page_id
GROUP BY p.url
ORDER BY evs DESC
LIMIT 10;

-- Top dispositivos
SELECT d.category, d.brand, d.model, d.os, d.browser, COUNT(*) AS evs
FROM web_oltp.event e
JOIN web_oltp.device d ON d.device_id = e.device_id
GROUP BY d.category, d.brand, d.model, d.os, d.browser
ORDER BY evs DESC
LIMIT 10;

-- Top países
SELECT g.country, COUNT(*) AS evs
FROM web_oltp.event e
JOIN web_oltp.geo g ON g.geo_id = e.geo_id
GROUP BY g.country
ORDER BY evs DESC
LIMIT 10;

-- Eventos por nombre (categoría)
SELECT ec.name AS event_name, COUNT(*) AS evs
FROM web_oltp.event e
JOIN web_oltp.event_category ec ON ec.event_category_id = e.event_category_id
GROUP BY ec.name
ORDER BY evs DESC
LIMIT 10;
