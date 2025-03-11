
# %%
import os

# data
import pandas as pd
import scalenav.oop as snoo

import ibis as ib
from ibis import _
import ibis.selectors as s

ib.options.interactive = True
ib.options.graphviz_repr = True

## Params
from parameters import *
from boundaries import bboxs,overture_db_filename
## Guide on working with Overture

### https://docs.overturemaps.org/guides/

### https://github.com/OvertureMaps/data?tab=readme-ov-file 
## Analysis
# the spatial extension
# https://duckdb.org/docs/extensions/spatial/functions

# the h3 extension in duckdb
# https://github.com/isaacbrodsky/h3-duckdb?tab=readme-ov-file

conn = snoo.sn_connect(database=overture_db_filename,interactive=True, memory_limit="100GB",threads = 8)

## Reading in the data
## Selecting an area of interest
bbox_name = 'global'
bbox = bboxs[bbox_name]

### Places
conn.raw_sql(f"""
CREATE or replace table places as (
    select * from read_parquet('datasets/overture/raw/places/*')
        where ST_X(geometry)>{bbox[0]} and
             ST_X(geometry)<{bbox[2]} and
             ST_Y(geometry)>{bbox[1]} and
             ST_Y(geometry)<{bbox[3]}
);
""")

places = conn.table(name="places")

# places = snoo.sn_table(conn,"places",'datasets/overture/raw/places/*')

### Reading landuse
conn.raw_sql(f"""
Create or replace table overture_landuse AS (
    select * from read_parquet('datasets/overture/raw/land_use/*')
         where bbox.xmax>{bbox[0]} and
            bbox.xmin<{bbox[2]} and
            bbox.ymax>{bbox[1]} and
            bbox.ymin<{bbox[3]}                     
);""")
landuse = conn.table(name="overture_landuse")

# landuse = snoo.sn_table(conn,"overture_landuse",'./datasets/overture/raw/land_use/*')

## Places
places = (
    places
    .mutate(x=_.bbox["xmin"],
            y=_.bbox["ymin"],
            name=_.names.primary,
            main=_.categories["primary"],
            sec = _.categories["alternate"],
            )
    .select("id","geometry","x","y","name","confidence","main","sec")
    )

# Selecting a starting resolution to project on H3.
h3_res = 11

### Working with places tags 

### Aggregating categories

overture_places_types = "https://raw.githubusercontent.com/OvertureMaps/schema/refs/heads/main/docs/schema/concepts/by-theme/places/overture_categories.csv"

places_types = pd.read_csv(overture_places_types,sep="; ",dtype={"Category code" : str, ' Overture Taxonomy' : list},engine='python')

places_types.rename(columns={"Category code" : "category", 'Overture Taxonomy' : "taxonomy"},inplace=True)

places_types["taxonomy"] = places_types["taxonomy"].apply(lambda x : x.strip().replace("]","").replace("[","").split(","))
places_types["main_cat"] = places_types["taxonomy"].apply(lambda x: x[0])
places_types["sec_cat"] = places_types["taxonomy"].apply(lambda x: x[1] if len(x)>1  else x[0])
places_types["raw_cat"] = places_types["taxonomy"].apply(lambda x: x[len(x)-1])

overture_place_taxonomy_filename = "notebooks/data/overture_place_types.csv"

if not os.path.exists(overture_place_taxonomy_filename):
    places_types[["main_cat","sec_cat","raw_cat"]].to_csv(overture_place_taxonomy_filename,index=False)
else : 
    print("File exists")

conn.create_table(obj=places_types[["raw_cat","main_cat","sec_cat"]],
                           name="places_types",
                           overwrite=True,
                           schema={
                               "raw_cat" : ib.dtype("string"),
                               "main_cat" : ib.dtype("string"),
                               "sec_cat" : ib.dtype("string"),
})

places_types = conn.table("places_types")

#%%

places = places.join(right=places_types,predicates=places.main==places_types.raw_cat,how="left")
places_exclude = ['structure_and_geography','religious_organization','health_and_medical','public_service_and_government',]
places_poi = places[~places["main_cat"].isin(places_exclude)]

## Landuse
landuse = landuse.mutate(centroid = _.geometry.centroid())

landuse_class = landuse.select("class").distinct().execute().get("class").tolist()
landuse_subtype = landuse.select("subtype").distinct().execute().get("subtype").tolist()

landuse_class_type = landuse.select("class","subtype").distinct().execute().apply(lambda x: " ".join(x),axis=1)

landuse = (
    landuse
    .mutate(x=_.centroid.x(),y=_.centroid.y())
    .select("id","class","subtype","x","y")
    )

landuse_class_type.to_csv("notebooks/data/overture_landuse_type.csv",index=False)

## POI to ISIC
poi_to_isic = conn.read_csv("notebooks/" + place_types_filename)
# poi_to_isic = poi_to_isic.filter(_.match_score > 0.4)

## Combining landuse and places 

# the ibis way
overture_data_ = ib.union(
    (landuse
     .select("id", "class","x","y")
     .rename({"sec_cat" : "class"})
     .cast({"x" : "float32",
            "y" : "float32",})
            ),
    (places_poi
     .select("id","sec_cat","x","y")
     .cast({"x" : "float32",
            "y" : "float32",})
            ),
)

overture_remove = ["residential","farmyard","meadow","orchard","military","medical","cemetery","base",
                   "trench","recreation_ground","clinic",'dog_park','animal_keeping','strict_nature_reserve',
                   'pedestrian','national_park','training_area','state_park']

overture_data = overture_data_.filter(~_.sec_cat.isin(overture_remove))

overture_data = overture_data.join(poi_to_isic[["sec_cat","section","isic_embed","isic_descr","dose","match_score"]],predicates="sec_cat",how="left")

overture_data = snoo.sn_project(overture_data,res=h3_res)

conn.create_table(obj=overture_data,name="overture_pois")

## saving full file

# if not os.path.exists(overture_places_landuses_filename):
overture_data.to_parquet(overture_places_landuses_filename,overwrite=True)
# else :
#     print("File exists")