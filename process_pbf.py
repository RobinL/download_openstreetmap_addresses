# ref https://wiki.openstreetmap.org/wiki/Addresses_in_the_United_Kingdom

# Download the latest OSM data for Great Britain from https://download.geofabrik.de/europe/great-britain.html

# Extract OSM "postal-ish" address features for Great Britain → NDJSONL (DuckDB-ready)

# # Inputs/outputs
# PBF="united-kingdom-250915.osm.pbf"
# OUT="gb_addr"
# PATTERN='nwr/addr:*'                 # quoted so zsh won't glob the *

# # 1) Keep only objects carrying any addr:* tag (nodes/ways/relations)
# osmium tags-filter -O "$PBF" "$PATTERN" -o "${OUT}.osm.pbf"

# # 2) Attach node locations to ways so polygons can be exported later
# osmium add-locations-to-ways "${OUT}.osm.pbf" -o "${OUT}_loc.osm.pbf"

# # 3) Re-filter and omit referenced objects (drops gates/crossings pulled as refs)
# osmium tags-filter -O -R "${OUT}_loc.osm.pbf" "$PATTERN" -o "${OUT}_only.osm.pbf"

# # 4) Export to GeoJSON Text Sequence (includes id/type/timestamp)
# osmium export "${OUT}_only.osm.pbf" \
#   -f geojsonseq --attributes=id,type,timestamp \
#   -o "${OUT}.geojsonseq"

# # 5) Strip the RFC8142 RS (0x1E) so it becomes plain NDJSONL for DuckDB
# tr -d '\036' < "${OUT}.geojsonseq" > "${OUT}.ndjsonl"

# # 6) (optional quick sanity check)
# # grep -n '"addr:' "${OUT}.ndjsonl" | head

# # 7) Delete intermediates (leave only ${OUT}.ndjsonl)
# rm -f "${OUT}.osm.pbf" "${OUT}_loc.osm.pbf" "${OUT}_only.osm.pbf" "${OUT}.geojsonseq"


import duckdb

# Enable spatial extension in DuckDB
con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")


# 1) read NDJSON (one Feature per line)
raw = con.sql("""

SELECT * FROM read_json_auto('gb_addr.ndjsonl', format='newline_delimited');
""")
raw.limit(2).show(max_width=10000)


# 2) build geometry + keep all tags
features = con.sql("""

SELECT
  CASE
    WHEN geometry.type = 'Point' THEN
      ST_Point(
        geometry.coordinates[1]::DOUBLE,
        geometry.coordinates[2]::DOUBLE
      )
    ELSE
      -- for Polygon/MultiPolygon/etc: serialize STRUCT -> JSON string
      ST_GeomFromGeoJSON(CAST(to_json(geometry) AS VARCHAR))
  END AS geom,
  properties
FROM raw;
""")

features.limit(2).show(max_width=10000)

# 3) representative point for polygons (point-on-surface), original for points
features_pt = con.sql("""
SELECT
  CASE WHEN ST_GeometryType(geom)::varchar LIKE '%POLYGON%'
       THEN ST_PointOnSurface(geom)
       ELSE geom
  END AS geom,
  properties
FROM features;
""")
features_pt.limit(2).show(max_width=10000)

# 4) “postal-ish” filter:
# require (housenumber OR housename OR unit) AND (street OR place)
gb_osm_addresses = con.sql("""

SELECT
  properties->>'@id'              AS osm_id,
  properties->>'@type'            AS osm_type,          -- node/way/relation
  NULLIF(properties->>'addr:housenumber','') AS housenumber,
  NULLIF(properties->>'addr:housename','')  AS housename,
  NULLIF(properties->>'addr:unit','')       AS unit,
  COALESCE(NULLIF(properties->>'addr:street',''),
           NULLIF(properties->>'addr:place',''))       AS thoroughfare,
  NULLIF(properties->>'addr:suburb','')     AS suburb,
  COALESCE(NULLIF(properties->>'addr:city',''),
           NULLIF(properties->>'addr:town',''),
           NULLIF(properties->>'addr:village',''),
           NULLIF(properties->>'addr:hamlet',''))      AS city_like,
  NULLIF(properties->>'addr:district','')   AS district,
  NULLIF(properties->>'addr:county','')     AS county,
  NULLIF(properties->>'addr:postcode','')   AS postcode,
  properties->>'building'                   AS building_tag,
  properties->>'name'                       AS name_tag,

  ST_X(geom) AS lon,
  ST_Y(geom) AS lat
FROM features_pt

""")

gb_osm_addresses.limit(5).show(max_width=10000)

full_addresses = con.sql("""
SELECT
    osm_id,
    CONCAT_WS(', ',
        NULLIF(name_tag, ''),
        NULLIF(unit, ''),
        NULLIF(CONCAT_WS(' ',
            NULLIF(housename, ''),
            NULLIF(housenumber, ''),
            NULLIF(thoroughfare, '')
        ), ''),
        NULLIF(suburb, ''),
        NULLIF(city_like, ''),
        NULLIF(county, '')
    ) AS full_address,
    postcode,
    lat, lon
FROM gb_osm_addresses
WHERE
    (housenumber IS NOT NULL OR housename IS NOT NULL OR unit IS NOT NULL)
    AND (thoroughfare IS NOT NULL)
limit 100
""")


full_addresses.limit(10).show(max_width=10000)

# 5) dump final addresses to parquet
# note: DuckDB writes Parquet natively; no extra dependencies required
con.sql("""
COPY full_addresses TO 'all_uk_addresses_osm.parquet' (FORMAT PARQUET);
""")
