from pyspark.sql import SparkSession
from urllib.request import urlretrieve
import os
# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("ADS project 1")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .getOrCreate()
)


# This part of code is for initialising the data folder for the working space
# Cited from ADS Tutorial 1
def create_raw_curated():   
    for type in ['raw', 'curated']:
        output_relative_dir = f'../data/{type}/'

        if not os.path.exists(output_relative_dir):
            os.makedirs(output_relative_dir)
        
        # create 'tlc_data' folder
        target_dir = 'tlc_data'
        if not os.path.exists(output_relative_dir + target_dir):
            os.makedirs(output_relative_dir + target_dir)



# This part of code is for initialising the data folder for the working space
# Cited from ADS Tutorial 1
def create_plot():
    output_relative_dir = '../plots/'

    # create 'plots' folder
    if not os.path.exists(output_relative_dir):
        os.makedirs(output_relative_dir)



# This part of code is for initialising the data folder for the working space
# Cited from ADS Tutorial 1

def create_2019_2021(): 
    for type in ['raw', 'curated']:
        output_relative_dir = f'../data/{type}/tlc_data/'

        if not os.path.exists(output_relative_dir):
            os.makedirs(output_relative_dir)
        
    # create folders for data from each year
        for target_dir in ('2019', '2021'):
            if not os.path.exists(output_relative_dir + target_dir):
                os.makedirs(output_relative_dir + target_dir)


# This part of code is for initialising the data folder for the working space
# Cited from ADS Tutorial 1
def create_more():
    for year in ['2019', '2021']:
        output_relative_dir = f'../data/curated/tlc_data/{year}/'

        if not os.path.exists(output_relative_dir):
            os.makedirs(output_relative_dir)
        
        # create three folders each to store data after certain stages
        for target_dir in ('firstclean', 'finalclean', 'final_data'):
            if not os.path.exists(output_relative_dir + target_dir):
                os.makedirs(output_relative_dir + target_dir)


def download(): 
    # The timeline of the year and month of data
    YEAR = ['2019', '2021']
    MONTHS = range(1, 13)


    URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"#year-month.parquet
    output_relative_dir = '../data/raw/'

    # Cited from ADS tutorial 1
    tlc_output_dir = output_relative_dir + 'tlc_data'
    for year in YEAR:
        for month in MONTHS:
            # 0-fill i.e 1 -> 01, 2 -> 02, etc
            month = str(month).zfill(2) 
            print(f"Begin year {year} month {month}")
            
            # generate url
            url = f'{URL_TEMPLATE}{year}-{month}.parquet'
            # generate output location and filename
            output_dir = f"{tlc_output_dir}/{year}/{year}-{month}.parquet"
            # download
            urlretrieve(url, output_dir) 
            
            print(f"Completed year {year} month {month}")
        
        
        
create_raw_curated()
create_plot()
create_2019_2021()
create_more()
download()