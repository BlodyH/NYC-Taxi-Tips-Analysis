# MAST30034 Project 1 README.md
- Name: None
- Student ID: None

**Research Goal:** This project mainly focuses on the analysis towards people's taxi Tip behaviour in NYC, with the help of a Random Forest Regressor and a Logistic Regression Model

**Timeline:** The timeline of this project is 2019 and 2021

## Setup

Python 3 dependencies:

* PySpark
* Pandas,Numpy
* Seaborn,matplotlib.pyplot
* Scikit Learning

We provide a requirement.txt including all of the libraries used. Create the conda environment `nerf` by running:
```
pip install -r requirements.txt
```

## Pipeline
To successfully run the pipeline, first visit `scripts` directory and download the data:
0. `data_download.py`: initialise all the workspaces and download the raw data needed.
Then visit `notebooks` directory and run all files in order:
1. `First Clean.ipynb`: This filters the invalid data included in the raw dataset and stores all the valid data into the directory `data/curated/tlc_data/year/firstclean/`.
2. `plot 2019.ipynb`: This plots the data after first clean for outlier analysis and preliminary analysis. Creates plots for feature analysis.
3. `plot 2021.ipynb`: This plots the data after first clean for outlier analysis and preliminary analysis. Creates plots for feature analysis.
4. `Second Clean.ipynb`: This further cleans the outliers and store all cleaned data in the directory `data/curated/tlc_data/year/finalclean/`.
5. `external_data.ipynb`: This preprocesses the external dataset and stores in the directory `data/curated/`.
6. `merge data.ipynb`: This merges the external data with the taxi data and  stores in the directory `data/curated/tlc_data/year/final_data/`.
7. `heat map.ipynb`: This produces the heat map to analysis the correlation between the features and the final response.
8. `map plot.ipynb`: This produces Geospatial analysis on the response.
9. `plot merge.ipynb`: This produces plots for analysis based on the merged dataset.
10. `aggregate.ipynb`: This produces a aggregated result to help further analysis.
11. `Model Data.ipynb`: This produces a final process on the data to prepare for putting into the model.
12. `RFR_full.ipynb`: This builds the Random Forest Regressor for the final prediction and includes error analysis.
13. `Classification.ipynb`: This builds the Logistic Regression Model for the final classification and includes error analysis.

For the visualisations, all plots are stored in the directory `plots`.



