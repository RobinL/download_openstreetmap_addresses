import duckdb

OS_PARQUET_PATH = "/Users/robin.linacre/Documents/data_linking/uk_address_matcher/secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet"
OSM_PATH = "./all_uk_addresses_osm.parquet"

# Enable spatial functions for geodesic distance calculations.
duckdb.install_extension("spatial")
duckdb.load_extension("spatial")

os_sql = f"""
    SELECT
        uprn,
        fulladdress.regexp_replace(postcode, '', 'gi').trim() AS fulladdressnopc,
        postcode AS os_postcode,
        latitude AS os_lat,
        longitude AS os_lon,
        ST_Transform(ST_Point(longitude, latitude), 'EPSG:4326', 'EPSG:3857') AS os_geom
    FROM read_parquet('{OS_PARQUET_PATH}')
"""

osm_sql = f"""
    SELECT
        *,
        ST_Transform(ST_Point(lon, lat), 'EPSG:4326', 'EPSG:3857') AS osm_geom
    FROM read_parquet('{OSM_PATH}')
"""

join_sql = f"""
    WITH os AS ({os_sql}),
         osm AS ({osm_sql}),
         matches AS (
             SELECT
                 osm.osm_id,
                 osm.full_address,
                 osm.postcode AS osm_postcode,
                 osm.lat AS osm_lat,
                 osm.lon AS osm_lon,
                 os.uprn,
                 os.fulladdressnopc,
                 os.os_postcode,
                 os.os_lat,
                 os.os_lon,
                 ST_Distance(os.os_geom, osm.osm_geom) AS distance_m
             FROM osm
             JOIN os
               ON ST_DWithin(os.os_geom, osm.osm_geom, 5)
         ),
         unique_matches AS (
             SELECT
                 osm_id
             FROM matches
             GROUP BY osm_id
             HAVING COUNT(*) = 1
         )
    SELECT
        m.*
    FROM matches m
    JOIN unique_matches u USING (osm_id)
"""

duckdb.sql(join_sql).show(max_width=10000)
