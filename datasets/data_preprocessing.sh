micromamba activate global-data

MAX_WORKERS = 10

#########
# JRC DATA

rastapar JRC/raw/GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0/GHS_POP_E2020_GLOBE_R2023A_54009_100_V1_0.tif -o_c=EPSG:4326 -o_p=JRC/processed/POP_100

rastapar JRC/raw/GHS_BUILT_S_NRES_E2018_GLOBE_R2023A_54009_10_V1_0/GHS_BUILT_S_NRES_E2018_GLOBE_R2023A_54009_10_V1_0.tif -o_c=EPSG:4326 -o_p=JRC/processed/S_NRES_10 -w=$MAX_WORKERS

rastapar JRC/raw/GHS_BUILT_S_E2018_GLOBE_R2023A_54009_10_V1_0/GHS_BUILT_S_E2018_GLOBE_R2023A_54009_10_V1_0.tif -o_c=EPSG:4326 -o_p=JRC/processed/S_10 -w=$MAX_WORKERS

rastapar JRC/raw/GHS_BUILT_H_AGBH_E2018_GLOBE_R2023A_54009_100_V1_0/GHS_BUILT_H_AGBH_E2018_GLOBE_R2023A_54009_100_V1_0.tif -o_c=EPSG:4326 -o_p=JRC/processed/H_AGBH_100 -w=$MAX_WORKERS

rastapar JRC/raw/GHS_BUILT_C_MSZ_E2018_GLOBE_R2023A_54009_10_V1_0/GHS_BUILT_C_MSZ_E2018_GLOBE_R2023A_54009_10_V1_0.tif -o_c=EPSG:4326 -o_p=JRC/processed/C_MSZ_10 -w=$MAX_WORKERS

## 
# MAPSPAM

python -m rast_converter mapspam/raw/dataverse_files/Global_Geotiff/spam2020V1r0_global_yield -o_c=EPSG:4326 -o_p=mapspam/processed/spam2020_yield.parquet

## 
# Copernicus
rastapar copernicus/raw/LandCover2018_100m_global_v3/PROBAV_LC100_global_v3.0.1_2018-conso_BuiltUp-CoverFraction-layer_EPSG-4326.tif -o_c=EPSG:4326 -o_p=copernicus/processed/copernicus_builtup_100m_2018

## 
# Kummu
rastapar kummu_etal/GDP_PPP_30arcsec_v3.nc -i_c=epsg:4326 -o_c=EPSG:4326 -o_p=kummu_etal/processed/GDP_PPP_30arcsec -bd=3

###
# GLW

# GLW_FILES=$(find GLW/raw -name "**.tif")

# for file in $GLW_FILES; do
#     rastapar $file -o_c=EPSG:4326 -o_p=GLW/processed
# done

rastapar GLW/raw/horse_2015/5_Ho_2015_Da.tif -o_c=EPSG:4326 -o_p=GLW/processed/horse_2015

rastapar GLW/raw/cattle_2015/5_Ct_2015_Da.tif -o_c=EPSG:4326 -o_p=GLW/processed/cattle_2015

rastapar GLW/raw/sheep_2015/5_Sh_2015_Da.tif -o_c=EPSG:4326 -o_p=GLW/processed/sheep_2015

rastapar GLW/raw/pigs_2015/5_Pg_2015_Da.tif -o_c=EPSG:4326 -o_p=GLW/processed/pigs_2015

rastapar GLW/raw/chicken_2015/5_Ch_2015_Da.tif -o_c=EPSG:4326 -o_p=GLW/processed/chicken_2015

rastapar GLW/raw/duck_2015/5_Dk_2015_Da.tif -o_c=EPSG:4326 -o_p=GLW/processed/duck_2015

rastapar GLW/raw/goat_2015/5_Gt_2015_Da.tif -o_c=EPSG:4326 -o_p=GLW/processed/goat_2015

### AgGDP

# AgGDP_FILES=$(find AgGDP/raw -name "**.tif")

# for file in $AgGDP_FILES
# do
#     rastapar $file -o_c=EPSG:4326 -o_p=AgGDP/processed
# done

rastapar AgGDP/raw/aggdp2010_forest_prior.tif -o_c=EPSG:4326 -o_p=AgGDP/processed/forest
rastapar AgGDP/raw/aggdp2010_crop_prior.tif -o_c=EPSG:4326 -o_p=AgGDP/processed/crop
rastapar AgGDP/raw/aggdp2010_fish_prior.tif -o_c=EPSG:4326 -o_p=AgGDP/processed/fish
rastapar AgGDP/raw/aggdp2010_ls_prior.tif -o_c=EPSG:4326 -o_p=AgGDP/processed/ls_prior
rastapar AgGDP/raw/aggdp2010.tif -o_c=EPSG:4326 -o_p=AgGDP/processed/agggdp



# GVA Thailand

# rastapar local_data/gva/raw/gva_ag_3ss.tif -o_c=EPSG:4326 -o_p=local_data/gva/processed/gva_thai_agri
# rastapar local_data/gva/raw/gva_man_3ss.tif -o_c=EPSG:4326 -o_p=local_data/gva/processed/gva_thai_man
# rastapar local_data/gva/raw/gva_serv_3ss.tif -o_c=EPSG:4326 -o_p=local_data/gva/processed/gva_thai_serv