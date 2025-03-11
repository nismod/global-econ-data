import itertools as iter
import json
# import shapely
from scalenav.plotting import cmap
import scalenav.oop as snoo

import ibis as ib
from ibis import _

ib.options.interactive = True
ib.options.graphviz_repr = True

from parameters import *

# the spatial extension
# https://duckdb.org/docs/extensions/spatial/functions



# the h3 extension in duckdb
# https://github.com/isaacbrodsky/h3-duckdb?tab=readme-ov-file
conn = snoo.sn_connect(database=overture_db_filename,interactive=True, memory_limit="100GB")

### Geo Boundaries
conn.raw_sql("CREATE OR REPLACE TABLE boundaries as SELECT * FROM ST_Read('/Users/cenv1069/Documents/data/datasets/boundaries/light/ne_110m_admin_0_countries/ne_110m_admin_0_countries.shp');")
boundaries = conn.table("boundaries")

# making boundaries for continents
limit_countries = {
        "europe" : ["ISL","NOR","UKR","GRC",], # europe
        "north america" : ["CAN","PAN","USA",],
        "south america" : ["PER","COL","BRA","CHL",],
        "oceania" : ["AUS","NZL",],
        "asia" : ["KAZ","Turkey","PNG","JPN","TWN"],
        "africa" : ["TUN","SEN","ZAF","SOM"], 
}

country_to_cont = {v:k for k,l in limit_countries.items() for v in l} 
country_to_cont
country_values = [x for x in iter.chain(*limit_countries.values())]
boundaries_selected = (
    boundaries
    .filter(_.ADM0_A3.isin(country_values))
    .mutate(geom=_.geom.buffer(0.5))
    )
# getting continents
continents = boundaries_selected.select("ADMIN","ADM0_A3","SU_A3","geom")
ib.to_sql(continents)
continents = continents.alias("b").sql('''
SELECT 
    ADM0_A3, 
    geom::GEOMETRY as geom
from b;
''')

### Generating bboxs
continents_df_ = continents.execute()
type(continents_df_)
continents_df_["continent"]=continents_df_["ADM0_A3"].map(country_to_cont)
continents_df_.set_geometry("geom",inplace=True)
continents_df_.head()
continents_df = continents_df_[["continent","geom"]].dissolve(by = "continent").reset_index()

continents_df["bounds"] = continents_df["geom"].apply(lambda x : list(x.bounds))
bboxs = {x[0]:y[0] for x,y in continents_df[["continent","bounds"]].apply(zip,axis=1)}
bboxs["global"] = map_limits

# bboxs
with open("notebooks/data/bboxs.json", "w") as out_file:
    json.dump(bboxs, out_file)

