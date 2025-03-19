<!-- https://ischlo.github.io/global-econ-data/notebooks/ -->

# Global economic activity and related data sets

This repository holds a series of notebooks that contain processing and visualidation of global economic activity data. 
Each link leads to the notebook for the corresponding data set / industry. Data set related links and author information is available in the notebook.

## Data sources 
And sector coverage

* [Global Econ Layers sources](https://ischlo.github.io/global-econ-data/notebooks/table/global-spatial-economics-datasets-public(datasets).csv)

## Covered topics
The covered topics and corresponding data sets are :

### Boundaries

Reference global boundary data sets that are widely used as the standard country shapes.
* [Boundaries](https://github.com/ischlo/global-econ-data/blob/main/notebooks/boundaries.ipynb)

### Agriculture

* [MAPSPAM](https://ischlo.github.io/global-econ-data/notebooks/mapspam.ipynb)
* [AgGDP](https://ischlo.github.io/global-econ-data/notebooks/agriculture_gdp.ipynb)
* [GLW](https://ischlo.github.io/global-econ-data/notebooks/cattle.ipynb)
  
### Industry

* [Mining](https://github.com/ischlo/global-econ-data/blob/main/notebooks/mining_vis.ipynb)
* [Cement](https://github.com/ischlo/global-econ-data/blob/main/notebooks/cement_vis.ipynb)

### Energy 

* [Power generation](https://github.com/ischlo/global-econ-data/blob/main/notebooks/power_vis.ipynb)
* [Power networks](https://github.com/ischlo/global-econ-data/blob/main/notebooks/grid_elec_vis.ipynb)

### Economy

* [World development indicator (WDI)](https://github.com/ischlo/global-econ-data/blob/main/notebooks/wdi_vis.ipynb)
* [DOSE](https://github.com/ischlo/global-econ-data/blob/main/notebooks/dose_vis.ipynb)
* [Global 3 sectors](https://ischlo.github.io/global-econ-data/notebooks/GDP_3_sectors_global.ipynb)

### Infrastructure

#### Global Human Settlement Layer

* [MSZ](https://ischlo.github.io/global-econ-data/notebooks/ghsl_msz_vis.ipynb)

## Filling economic output gaps

A combination of data sets has been done in order to get a global economic activity data set expressed as GDP/GRP produced at the finest spatial resolution available. The DOSE data set at ADMIN_1 resolution has been combined with WDI at ADMIN_0 resolution for 3 sectors: *manufacturing*, *services*, *agriculture*. The methodology is in the notebook: 

* [Combined DOSE-WDI methodology](https://github.com/ischlo/global-econ-data/blob/main/notebooks/missing_countries.ipynb)
* [Combined data adding geometries](https://github.com/ischlo/global-econ-data/blob/main/notebooks/missing_countries_geo.ipynb)

## Points of interest (POIs)
Data from Overture/OpenStreetMap and Foursqaure

* [Overture POIs preprocessing](https://ischlo.github.io/global-econ-data/notebooks/overture_data.ipynb)
* [Overture POIs](https://ischlo.github.io/global-econ-data/notebooks/overture_data_vis.ipynb)
* [Foursquare](https://ischlo.github.io/global-econ-data/notebooks/foursquare.ipynb)
* [Comparison of foursqure and OSM](https://ischlo.github.io/global-econ-data/notebooks/overture_vs_foursquare.ipynb)
* [Comparison of OSM and GHSL](https://ischlo.github.io/global-econ-data/notebooks/ghsl_vs_pois.ipynb)

## Categorisation of Points of Interest (POIs) 

Standardising categories into ISIC codes.

* [ISIC classification](https://github.com/ischlo/global-econ-data/blob/main/notebooks/isic.ipynb)

## Running locally

If you want to run the notebooks locally, clone this repository, then create the python virtual environment with [micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html) by running `micromamba env create -f env.yaml` in the terminal from the main directory. After that activate the environment by running `micromamba activate global-data`. Finally, you will need to link the notebooks with the virtual environment. This step will depend on the setup you use, but for VScode, open the command palette with *COMMAND-SHIFT-P*, then *Python : Select Interpreter*, then *Enter interpreter path* and copy in the path the output of the command `which python` from the terminal. It should be something like `~/user/home/micromamba/envs/global-data/bin/python`. 