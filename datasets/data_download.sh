# download process
micromamba activate global-data

# GHSL layers

## S Total 10m
wget https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_S_GLOBE_R2023A/GHS_BUILT_S_E2030_GLOBE_R2023A_54009_100/V1-0/GHS_BUILT_S_E2030_GLOBE_R2023A_54009_100_V1_0.zip \
    --directory-prefix JRC/raw/

## S NRES 10m
wget https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_S_GLOBE_R2023A/GHS_BUILT_S_NRES_E2030_GLOBE_R2023A_54009_100/V1-0/GHS_BUILT_S_NRES_E2030_GLOBE_R2023A_54009_100_V1_0.zip \
    --directory-prefix JRC/raw/

## H AGBH

wget https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_H_GLOBE_R2023A/GHS_BUILT_H_AGBH_E2018_GLOBE_R2023A_54009_100/V1-0/GHS_BUILT_H_AGBH_E2018_GLOBE_R2023A_54009_100_V1_0.zip \
    --directory-prefix JRC/raw/

## MSZ 
wget https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/GHSL/GHS_BUILT_C_GLOBE_R2023A/GHS_BUILT_C_MSZ_E2018_GLOBE_R2023A_54009_10/V1-0/GHS_BUILT_C_MSZ_E2018_GLOBE_R2023A_54009_10_V1_0.zip \
    --directory-prefix JRC/raw/

# Overture data

## Places
### With overture CL tool :
overturemaps download -f geoparquet --type=place -o overture/raw/places.geoparquet
# aws s3 cp --region us-west-2 --no-sign-request --recursive s3://overturemaps-us-west-2/release/2024-10-23.0/theme=places/type=place/ documents/data/datasets/overture/raw

## land use
overturemaps download -f geoparquet --type=land_use -o overture/raw/landuse.geoparquet
# aws s3 cp --region us-west-2 --no-sign-request --recursive s3://overturemaps-us-west-2/release/2024-10-23.0/theme=base/type=land_use/ documents/data/datasets/overture/raw

# Foursquare
aws s3 cp --region us-east-1 --no-sign-request --recursive s3://fsq-os-places-us-east-1/release/dt=2024-11-19/places/parquet/ foursquare/places


# Kummu 2025
wget https://zenodo.org/api/records/13943886/files-archive  --directory-prefix=kummu_2025/

# AgGDP

wget https://datacatalogfiles.worldbank.org/ddh-published-v2/0061507/3/DR0090771/aggdp2010_ls_prior.tif --directory-prefix=AgGDP/raw

wget https://datacatalogfiles.worldbank.org/ddh-published-v2/0061507/3/DR0090770/aggdp2010_crop_prior.tif --directory-prefix=AgGDP/raw

wget https://datacatalogfiles.worldbank.org/ddh-published-v2/0061507/3/DR0090772/aggdp2010_fish_prior.tif --directory-prefix=AgGDP/raw

wget https://datacatalogfiles.worldbank.org/ddh-published-v2/0061507/3/DR0090773/aggdp2010_forest_prior.tif --directory-prefix=AgGDP/raw

wget https://datacatalogfiles.worldbank.org/ddh-published-v2/0061507/3/DR0089195/aggdp2010.tif --directory-prefix=AgGDP/raw