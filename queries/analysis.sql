-- Mas vendidos
SELECT 
p.id AS producto_id, p.nombre AS producto_nombre, SUM(v.cantidad) AS total_vendido
FROM dw.ventas v
JOIN dw.producto p 
ON v.producto_id = p.id
GROUP BY p.id, p.nombre
ORDER BY total_vendido DESC;



-- Menos vendidos
SELECT 
    p.id AS producto_id,
    p.nombre AS producto_nombre,
    SUM(v.cantidad) AS total_vendido
FROM dw.ventas v
JOIN dw.producto p 
    ON v.producto_id = p.id
GROUP BY p.id, p.nombre
ORDER BY total_vendido ASC;



-- Total vendidos por categoria
SELECT 
    c.id AS categoria_id,
    c.nombre AS categoria_nombre,
    SUM(v.cantidad) AS total_items_vendidos,
    SUM(v.monto) AS total_monto_vendido
FROM dw.ventas v
JOIN dw.producto p 
    ON v.producto_id = p.id
JOIN dw.categoria c 
    ON p.categoria_id = c.id
GROUP BY c.id, c.nombre
ORDER BY total_monto_vendido DESC;



-- add_to_cart mas abandonados
WITH add_to_cart AS (
    SELECT 
        p.id AS producto_id,
        p.nombre,
        SUM(e.cantidad) AS agregado
    FROM dw.eventos e
    JOIN dw.producto p ON e.producto_id = p.id
    WHERE e.tipo = 'add_to_cart'
    GROUP BY p.id, p.nombre
),
purchased AS (
    SELECT 
        p.id AS producto_id,
        SUM(v.cantidad) AS comprado
    FROM dw.ventas v
    JOIN dw.producto p ON v.producto_id = p.id
    GROUP BY p.id
)
SELECT 
    a.producto_id,
    a.nombre,
    a.agregado,
    COALESCE(p.comprado,0) AS comprado,
    (a.agregado - COALESCE(p.comprado,0)) AS abandonados
FROM add_to_cart a
LEFT JOIN purchased p ON a.producto_id = p.producto_id
WHERE (a.agregado - COALESCE(p.comprado,0)) > 0
ORDER BY abandonados DESC



-- Ticket promedio
WITH ordenes AS (
    SELECT 
        v.orden_id,
        SUM(v.monto) AS monto_total
    FROM dw.ventas v
    GROUP BY v.orden_id
)
SELECT 
    AVG(monto_total) AS ticket_promedio
FROM ordenes;



-- Productos comprados juntos
SELECT 
    LEAST(p1.nombre, p2.nombre) AS producto_a,
    GREATEST(p1.nombre, p2.nombre) AS producto_b,
    COUNT(DISTINCT v1.orden_id) AS veces_comprados_juntos
FROM dw.ventas v1
JOIN dw.ventas v2 
    ON v1.orden_id = v2.orden_id 
   AND v1.producto_id < v2.producto_id   -- evita duplicar pares y self-joins
JOIN dw.producto p1 ON v1.producto_id = p1.id
JOIN dw.producto p2 ON v2.producto_id = p2.id
GROUP BY producto_a, producto_b
ORDER BY veces_comprados_juntos DESC
LIMIT 20;



-- Conversion rate por fuente de tráfico de usuarios
WITH visitas AS (
    SELECT DISTINCT 
        b.user_pseudo_id,
        NULLIF(b.source,'') AS source
    FROM dw.v_stg_base b
    WHERE b.user_pseudo_id IS NOT NULL
),
compradores AS (
    SELECT DISTINCT
        v.usuario_id,
        u.user_key,
        NULLIF(b.source,'') AS source
    FROM dw.ventas v
    JOIN dw.usuario u ON v.usuario_id = u.id
    JOIN dw.v_stg_base b ON b.user_pseudo_id = u.user_key
    WHERE v.transaccion_exitosa = TRUE
)
SELECT 
    v.source,
    COUNT(DISTINCT v.user_pseudo_id) AS usuarios_visitantes,
    COUNT(DISTINCT c.user_key) AS usuarios_compradores,
    ROUND(
        COUNT(DISTINCT c.user_key)::numeric 
        / NULLIF(COUNT(DISTINCT v.user_pseudo_id),0) * 100, 2
    ) AS conversion_rate_pct
FROM visitas v
LEFT JOIN compradores c 
    ON v.user_pseudo_id = c.user_key
   AND v.source = c.source
GROUP BY v.source
ORDER BY conversion_rate_pct DESC;



-- Ticket promedio por fuente de tráfico
WITH ordenes AS (
    -- monto total por orden
    SELECT 
        v.orden_id,
        SUM(v.monto) AS monto_total
    FROM dw.ventas v
    GROUP BY v.orden_id
),
ordenes_fuente AS (
    -- asignamos fuente a cada orden usando transaction_id
    SELECT DISTINCT
        b.transaction_id AS orden_id,
        NULLIF(b.source,'') AS source
    FROM dw.v_stg_base b
    WHERE b.transaction_id IS NOT NULL
)
SELECT 
    f.source,
    ROUND(AVG(o.monto_total),2) AS ticket_promedio
FROM ordenes o
JOIN ordenes_fuente f ON o.orden_id = f.orden_id
GROUP BY f.source
ORDER BY ticket_promedio DESC;



-- Porcentaje de abandono por dispositivo
SELECT 
    d.tipo AS dispositivo,
    COUNT(DISTINCT e.usuario_id) AS usuarios_checkout,
    COUNT(DISTINCT e.usuario_id) FILTER (WHERE e.venta_id IS NOT NULL) AS usuarios_con_compra,
    COUNT(DISTINCT e.usuario_id) FILTER (WHERE e.venta_id IS NULL) AS usuarios_abandono,
    ROUND(
        COUNT(DISTINCT e.usuario_id) FILTER (WHERE e.venta_id IS NULL)::numeric
        / NULLIF(COUNT(DISTINCT e.usuario_id),0) * 100, 2
    ) AS abandono_pct
FROM dw.eventos e
JOIN dw.dispositivo d ON e.dispositivo_id = d.id
WHERE e.tipo = 'begin_checkout'
GROUP BY d.tipo
ORDER BY abandono_pct DESC;



-- Porcentaje de abandono total
SELECT
    COUNT(DISTINCT usuario_id) AS usuarios_checkout,
    COUNT(DISTINCT usuario_id) FILTER (WHERE venta_id IS NOT NULL) AS usuarios_con_compra,
    COUNT(DISTINCT usuario_id) FILTER (WHERE venta_id IS NULL) AS usuarios_abandono,
    ROUND(
        COUNT(DISTINCT usuario_id) FILTER (WHERE venta_id IS NULL)::numeric 
        / NULLIF(COUNT(DISTINCT usuario_id),0) * 100, 2
    ) AS abandono_pct
FROM dw.eventos
WHERE tipo = 'begin_checkout';



-- Conversion rate por país
WITH visitantes AS (
    SELECT 
        u.id AS usuario_id,
        u.pais
    FROM dw.usuario u
    WHERE u.pais IS NOT NULL
),
compradores AS (
    SELECT DISTINCT
        v.usuario_id,
        u.pais
    FROM dw.ventas v
    JOIN dw.usuario u ON v.usuario_id = u.id
    WHERE v.transaccion_exitosa = TRUE
)
SELECT 
    v.pais,
    COUNT(DISTINCT v.usuario_id) AS usuarios_visitantes,
    COUNT(DISTINCT c.usuario_id) AS usuarios_compradores,
    COUNT(*) FILTER (WHERE c.usuario_id IS NOT NULL) AS ventas_totales,
    ROUND(
        COUNT(DISTINCT c.usuario_id)::numeric / NULLIF(COUNT(DISTINCT v.usuario_id),0) * 100, 2
    ) AS conversion_rate_pct
FROM visitantes v
LEFT JOIN compradores c 
    ON v.usuario_id = c.usuario_id
GROUP BY v.pais
ORDER BY ventas_totales DESC;



-- Revenue por dispositivo
SELECT 
    d.tipo AS dispositivo,
    COUNT(DISTINCT v.usuario_id) AS usuarios,
    SUM(v.monto) AS revenue_total,
    ROUND(SUM(v.monto)::numeric * 100 / SUM(SUM(v.monto)) OVER (), 2) AS pct_revenue
FROM dw.ventas v
JOIN dw.dispositivo d ON v.dispositivo_id = d.id
GROUP BY d.tipo
ORDER BY revenue_total DESC;
