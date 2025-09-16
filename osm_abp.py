# Aim: Get a list of messy addresses from OSM and their corresponding canonical addresses to measure performance of address matching.
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
               ON ST_DWithin(os.os_geom, osm.osm_geom, 10)
         ),
         unique_matches AS (
             SELECT
                 osm_id
             FROM matches
             GROUP BY osm_id
             HAVING COUNT(*) = 1 AND MIN(distance_m) <= 2
         )
    SELECT
        m.osm_id,
        m.uprn,
        m.full_address AS full_address_osm,
        m.fulladdressnopc AS full_address_os,
        m.osm_postcode AS postcode_osm,
        m.os_postcode AS postcode_os,
        m.osm_lat AS lat_osm,
        m.os_lat AS lat_os,
        m.osm_lon AS lon_osm,
        m.os_lon AS lon_os
    FROM matches m
    JOIN unique_matches u USING (osm_id)
"""

result = duckdb.sql(join_sql)

output_path = "os_vs_osm_matched.parquet"


result.to_parquet(output_path)

duckdb.sql(f"SELECT COUNT(*) AS row_count FROM read_parquet('{output_path}')").show()
duckdb.sql(
    f"SELECT * FROM read_parquet('{output_path}') ORDER BY random() LIMIT 10"
).show(max_width=10000)

# Use our exact matcher to skim off the exact ones, then human/LLM review to decide the rest?
