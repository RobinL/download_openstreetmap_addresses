import duckdb

OS_PARQUET_PATH = "/Users/robin.linacre/Documents/data_linking/uk_address_matcher/secret_data/ord_surv/raw/add_gb_builtaddress_sorted_zstd.parquet"

sql = f"""
select
uprn,
fulladdress.regexp_replace(postcode, '', 'gi').trim() as fulladdressnopc,
postcode, latitude as lat, longitude as lon
from read_parquet('{OS_PARQUET_PATH}')
"""

duckdb.sql(sql).show(max_width=10000)

OSM_PATH = "./all_uk_addresses_osm.parquet"
sql = f"""
select *
from read_parquet('{OSM_PATH}')"""
duckdb.sql(sql).show(max_width=10000)
