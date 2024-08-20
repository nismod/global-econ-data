# Global economic activity data sets

This repository holds a series of notebooks that contain processing and visualidation of global economic activity data. 
Each link leads to the notebook for the corresponding data set / industry. Data set related links and author information is availale in the notebook.

## Data sets covered
The covered data sets are : 
### Boundaries
Reference global boundary data sets that are widely used as the standard country shapes.

* [Boundaries](https://github.com/ischlo/global-econ-data/blob/main/notebooks/boundaries.ipynb)

### Industry

* [Mining](https://github.com/ischlo/global-econ-data/blob/main/notebooks/mining_vis.ipynb)
* [Cement](https://github.com/ischlo/global-econ-data/blob/main/notebooks/cement_vis.ipynb)

### Energy 

* [Power generation](https://github.com/ischlo/global-econ-data/blob/main/notebooks/power_vis.ipynb)
* [Power networks](https://github.com/ischlo/global-econ-data/blob/main/notebooks/grid_elec_vis.ipynb)

### Economy


* [World development indicator (WDI)](https://github.com/ischlo/global-econ-data/blob/main/notebooks/wdi_vis.ipynb)
* [DOSE](https://github.com/ischlo/global-econ-data/blob/main/notebooks/dose_vis.ipynb)

## Filling gaps

A combination of data sets has been done in order to get a global economic activity data set expressed as GDP/GRP produced at the finest spatial resolution available. The DOSE data set at ADMIN1 resolution has been combined with WDI at ADMIN0 resolution for 3 sectors: *manufacturing*, *services*, *agriculture*. The methodology is in the notebook: 

* [Combined DOSE-WDI methodology](https://github.com/ischlo/global-econ-data/blob/main/notebooks/missing_countries.ipynb)
