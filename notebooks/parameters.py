# paramers.py
import pypalettes as pypal
from pathlib import Path
import os
# DOSE WDI PROCESSING
# this is the year for which to generate the dose-wdi data set, easier to set from here than looking for it across the notebook.
year = 2015

# print(Path.cwd())
# https://www.btelligent.com/en/blog/best-practice-working-with-paths-in-python-part-1-2
# https://www.pythoncheatsheet.org/cheatsheet/file-directory-path
# https://medium.com/@ageitgey/python-3-quick-tip-the-easy-way-to-deal-with-file-paths-on-windows-mac-and-linux-11a072b58d5f
# https://realpython.com/python-pathlib/
print(Path(os.getcwd()).resolve())


# 
missing_frac = 0.5
plot_limit = 10_000

# continuous palette
count_palette_name = "OrYel"
count_pal = pypal.load_cmap(count_palette_name)

# categorical palette
cat_pal_name = "Bold"
cat_pal = pypal.load_cmap(name=cat_pal_name)

# gdp pal
gdp_pal_name="Blues"
gdp_pal = pypal.load_cmap(gdp_pal_name)
gdp_pal

# map limits to avoid weird plots
map_limits = [-168.8,-56.9,189.8,77.7]

####
agg_res = 5

##
# LLM for classification
# Load the Sentence-BERT model
light_lm = "all-MiniLM-L6-v2"
full_lm = "all-mpnet-base-v2"

#####
overture_places_landuses_filename = "datasets/overture/processed/places_landuses.parquet"


## paths

# project_repo = lib