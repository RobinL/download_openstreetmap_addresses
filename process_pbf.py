# ref https://wiki.openstreetmap.org/wiki/Addresses_in_the_United_Kingdom

# Download the latest OSM data for Great Britain from https://download.geofabrik.de/europe/great-britain.html
# osmium tags-filter --overwrite -o england-addresses.osm.pbf great-britain-latest.osm.pbf nwr/addr:housenumber
# osmium export england-addresses.osm.pbf -o england-addresses.geojsonseq --output-format=geojsonseq

import duckdb
import pandas as pd

# Load the spatial extension

duckdb.sql("INSTALL spatial")
duckdb.sql("LOAD spatial")

# Read the GeoJSON file into DuckDB
geojson_path = "./england-addresses.geojsonseq"

df_addresses = duckdb.sql(
    f"""
    SELECT * FROM ST_Read('{geojson_path}')
"""
)

# Verify the table creation and view the first few records
pd.options.display.max_columns = 1000
pd.options.display.max_rows = 1000

duckdb.sql("SELECT * FROM df_addresses LIMIT 5").df()


# Extract the address data
query = """
SELECT
    "addr:unit" AS unit,
    "addr:flats" AS flats,
    "addr:housename" AS housename,
    "addr:housenumber" AS housenumber,
    "addr:substreet" AS substreet,
    "addr:street" AS street,
    "addr:parentstreet" AS parentstreet,
    "addr:suburb" as suburb,
    "addr:city" AS city,
    "addr:postcode" AS postcode,
    "building" AS building,

FROM
    df_addresses
where (
"addr:housename" IS NOT NULL OR
"addr:housenumber" IS NOT NULL
)
and
"addr:postcode" IS NOT NULL
and
 ("building" IN (
        'apartments', 'detached', 'terrace', 'semidetached_house', 'hut',
        'ger', 'houseboat', 'static_caravan', 'house', 'dwelling_house',
        'residences', 'residence', 'residental'
    ))



"""
open_streetmap_addresess = duckdb.sql(query)

# Write out to parquet
sql = """
COPY open_streetmap_addresess
TO 'open_streetmap_addresess.parquet' (FORMAT PARQUET);

"""
duckdb.sql(sql)

sql = """

select count(*)
from read_parquet('open_streetmap_addresess.parquet')
"""

n = duckdb.sql(sql).df().iloc[0, 0]
print(f"Number of addresses: {n:,}")

pd.options.display.max_colwidth = 1000

sql = """
select COALESCE(unit, '') || ' ' ||
    COALESCE(flats, '') || ' ' ||
    COALESCE(housename, '') || ' ' ||
    COALESCE(housenumber, '') || ' ' ||
    COALESCE(substreet, '') || ' ' ||
    COALESCE(street, '') || ' ' ||
    COALESCE(parentstreet, '') || ' ' ||
    COALESCE(suburb, '') || ' ' ||
    COALESCE(city, '')  as full_address
    , *

from read_parquet('open_streetmap_addresess.parquet')
-- where housename is not null and housenumber is not null
where building = 'apartments'

"""

ddff = duckdb.sql(sql).df()
len(ddff)
