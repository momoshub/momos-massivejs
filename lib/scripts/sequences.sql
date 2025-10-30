-- Load non-pk sequences.

-- Try to read from materialized view first, fall back to full query if it doesn't exist
SELECT * FROM (
  SELECT 
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM pg_matviews 
        WHERE schemaname = 'public' AND matviewname = 'massive_sequences_cache'
      ) THEN 'cached'
      ELSE 'fresh'
    END AS query_type
) AS check_cache,
LATERAL (
  SELECT * FROM massive_sequences_cache
  WHERE check_cache.query_type = 'cached'
  
  UNION ALL
  
  SELECT
    s.sequence_schema AS schema,
    s.sequence_name AS name
  FROM information_schema.sequences AS s
  WHERE check_cache.query_type = 'fresh'
    AND NOT EXISTS (
      SELECT 1
      FROM pg_class AS c
      JOIN pg_attribute AS a ON a.attrelid = c.oid
      JOIN pg_constraint AS pks ON pks.conrelid = c.oid
      JOIN information_schema._pg_expandarray(pks.conkey) AS ordinals ON ordinals.n = a.attnum
      JOIN pg_attrdef AS ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
      WHERE pg_get_expr(ad.adbin, ad.adrelid) = 'nextval(''' || s.sequence_name || '''::regclass)'
        OR pg_get_expr(ad.adbin, ad.adrelid) = 'nextval(''"' || s.sequence_name || '"''::regclass)' -- casing!
    )
) AS sequences;
