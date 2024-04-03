# Imorting libraries for 1st pipeline
import os
import random
from airflow import DAG
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import zipfile

# Importing libraries necessary for 2nd pipeline
import apache_beam as beam
import geopandas as gpd
from geodatasets import get_path
from airflow.sensors.filesystem import FileSensor
import numpy as np
import pandas as pd
import logging
import matplotlib.pyplot as plt
from ast import literal_eval as make_tuple
import shutil


main_dir = '/opt/airflow'
download_dir = '/opt/airflow/downloads'
year = 2022 # Can set it to any year
num_files = 10
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 2),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag1 = DAG(
    'dag1_fetch_files',
    default_args=default_args,
    description='A DAG to fetch webpages, extract file links, download files, zip them and store them in a folder',
    schedule_interval=None,  # Set to None for manual triggering
)

# This function will help fetch the webpage
def fetch_webpage(**kwargs):
    # Define the url
    url = f'https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/'
    response = requests.get(url) # Access the conetens of the url
    # Store the year and the text of the webpage and allow transfer of the data to the next task
    kwargs['ti'].xcom_push(key=f'html_content_{year}', value=response.text) 

# Given the names of the selected files, this function will extract the complete file links and send it to the next task
def extract_file_links(**kwargs):
    # Pull the html content from the fetch_webpage task
    html_content = kwargs['ti'].xcom_pull(task_ids='fetch_webpage', key=f'html_content_{year}')
    soup = BeautifulSoup(html_content, 'html.parser') # Send it through BeautifulSoup
    # Inside the html, make a list of names of all the csv files contined in the webpage
    links = [link['href'] for link in soup.find_all('a') if link['href'].endswith('.csv')]
    # Select the required number of links at random
    selected_links = random.sample(links, min(num_files, len(links)))
    kwargs['ti'].xcom_push(key=f'selected_links_{year}', value=selected_links) # Pass on the links to the next task

# This function will download the files and zip them into the required location
def download_and_zip_files(**kwargs):
    # Get links from the upstream task
    selected_links = kwargs['ti'].xcom_pull(task_ids='extract_file_links', key=f'selected_links_{year}')
    zip_file_path = os.path.join(download_dir, f"{year}_data.zip") # Make path where zip file will be stored
    with zipfile.ZipFile(zip_file_path, 'w') as zipf:
        for link in selected_links: # Iterate over all the selected links
            file_url = f"https://www.ncei.noaa.gov/data/local-climatological-data/access/{year}/" + link
            file_name = link.split('/')[-1]
            file_response = requests.get(file_url, timeout=30) # Download the file
            if file_response.status_code == 200:
                zipf.writestr(file_name, file_response.content) # Write the file into a zip directory

# Executes the fetch_webpage function
fetch_webpage_task = PythonOperator(
    task_id='fetch_webpage',
    python_callable=fetch_webpage,
    dag=dag1,
)

# Executes the extract_file_links function
extract_file_links_task = PythonOperator(
    task_id='extract_file_links',
    python_callable=extract_file_links,
    dag=dag1,
)

# Executes the download_and_zip_files function
download_and_zip_files_task = PythonOperator(
    task_id='download_and_zip_files',
    python_callable=download_and_zip_files,
    dag=dag1,
)
# Dependencies for pipeline 1
fetch_webpage_task >> extract_file_links_task >> download_and_zip_files_task
#############################################################################################
#############################################################################################

## TASK 2 / PIPELINE 2
dag2 = DAG(
    'dag2_analytics',
    default_args=default_args,
    description='DAG for analytics ',
    schedule_interval=timedelta(minutes=1),  # AUtomatically trigger every minute
)

# Function to unzip files
def unzip_file(*context):
    with zipfile.ZipFile(os.path.join(download_dir, f"{year}_data.zip"), 'r') as zip_ref:
        os.makedirs('/opt/airflow/files', exist_ok=True)
        zip_ref.extractall('/opt/airflow/files')

## Function to parse csv
def parseCSV(data):
    df = data.split('","')
    df[0] = df[0].strip('"')
    df[-1] = df[-1].strip('"')
    return list(df)

class ExtractAndFilterFields(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)
        self.headers = {i:ind for ind,i in enumerate(headers)} 

## Function to run beam for getting fields from csv after processing it
def process_csv(**kwargs):
    required_fields = ["LATITUDE","LONGITUDE","HourlyDryBulbTemperature"]
    os.makedirs('/opt/airflow/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/opt/airflow/files/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'FilterAndCreateTuple' >> beam.ParDo(ExtractAndFilterFields(required_fields=required_fields))
            | 'CombineTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )

        result | 'WriteToText' >> beam.io.WriteToText('/opt/airflow/results/result.txt')

# Setup another PythonOperator over Apache Beam to compute monthly averages
class ExtractFieldsWithMonth(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)

        self.headers = {i:ind for ind,i in enumerate(headers)}

    ## Function to get month, latitude, longitude and winddata
    def process(self, element):
        headers = self.headers 
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.required_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            Measuretime = datetime.strptime(element[headers['DATE']],'%Y-%m-%dT%H:%M:%S')
            Month_format = "%Y-%m"
            Month = Measuretime.strftime(Month_format)
            yield ((Month, lat, lon), data)
            
## Function to calculate averages
def compute_avg(data):
    val_data = np.array(data[1])
    val_data_shape = val_data.shape
    val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') 
    val_data = np.reshape(val_data,val_data_shape)
    masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
    res = np.ma.average(masked_data, axis=0)
    res = list(res.filled(np.nan))
    logger = logging.getLogger(__name__)
    logger.info(res)
    return ((data[0][1],data[0][2]),res)

## Function to use beam to compute the averages
def compute_monthly_avg( **kwargs):
    required_fields = ["LATITUDE","LONGITUDE","HourlyDryBulbTemperature"]
    os.makedirs('/opt/airflow/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/opt/airflow/files/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'CreateTupleWithMonthInKey' >> beam.ParDo(ExtractFieldsWithMonth(required_fields=required_fields))
            | 'CombineTupleMonthly' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda data: compute_avg(data))
            | 'CombineTuplewithAverages' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )
        result | 'WriteAveragesToText' >> beam.io.WriteToText('/opt/airflow/results/averages.txt')

class Aggregated(beam.CombineFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i in headers_csv:
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(i.replace('Hourly',''))

    def create_accumulator(self):
        return []
    
    def add_input(self, accumulator, element):
        accumulator2 = {key:value for key,value in accumulator}
        data = element[2]
        val_data = np.array(data)
        val_data_shape = val_data.shape
        val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float')
        val_data = np.reshape(val_data,val_data_shape)
        masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
        res = np.ma.average(masked_data, axis=0)
        res = list(res.filled(np.nan))
        for ind,i in enumerate(self.required_fields):
            accumulator2[i] = accumulator2.get(i,[]) + [(element[0],element[1],res[ind])]

        return list(accumulator2.items())
    
    def merge_accumulators(self, accumulators):
        merged = {}
        for a in accumulators:
                a2 = {key:value for key,value in a}
                for i in self.required_fields:
                    merged[i] = merged.get(i,[]) + a2.get(i,[])

        return list(merged.items())
    
    def extract_output(self, accumulator):
        return accumulator
    
# This function will plot the geomaps
def plot_geomaps(values):
    logger = logging.getLogger(__name__)
    logger.info(values)
    data = np.array(values[1],dtype='float')
    d1 = np.array(data,dtype='float')
    
    world = gpd.read_file(get_path('naturalearth.land'))

    data = gpd.GeoDataFrame({
        values[0]:d1[:,2]
    }, geometry=gpd.points_from_xy(*d1[:,(1,0)].T))
    
    fig, ax = plt.subplots(1, 1, figsize=(10, 5))
    world.plot(ax=ax, color='white', edgecolor='black')
    
    # Plot the heatmap
    data.plot(column=values[0], cmap='viridis', marker='o', markersize=150, ax=ax, legend=True)
    ax.set_title(f'{values[0]} Heatmap')
    os.makedirs('/opt/airflow/results/plots', exist_ok=True)
    
    plt.savefig(f'/opt/airflow/results/plots{values[0]}_heatmap_plot.png')

# Use geopandas to create heatmaps
def create_heatmap_visualization(**kwargs):
    
    required_fields = ["LATITUDE","LONGITUDE","HourlyDryBulbTemperature"]
    with beam.Pipeline(runner='DirectRunner') as p:
        
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/opt/airflow/results/averages.txt*')
            | 'preprocessParse' >>  beam.Map(lambda a:make_tuple(a.replace('nan', 'None')))
            | 'Global aggregation' >> beam.CombineGlobally(Aggregated(required_fields = required_fields))
            | 'Flat Map' >> beam.FlatMap(lambda a:a) 
            | 'Plot Geomaps' >> beam.Map(plot_geomaps)            
        )

## Function to delete csv from the folder
def delete_csv(**kwargs):
    shutil.rmtree('/opt/airflow/files')

# Sensor for file, sensor task
# Poke every 5 secs to see if the archive is available
wait_for_zip = FileSensor(
    task_id='wait_for_zip',
    fs_conn_id='fs_default',
    filepath=os.path.join(download_dir, f"{year}_data.zip"),
    timeout=5,
    poke_interval=5,
    mode='poke',
    dag=dag2,
)


# Unzip the archive and store it at a given place
unzip_file_task = PythonOperator(
    task_id='unzip_file_task',
    python_callable=unzip_file,
    dag=dag2,
)

# Extract contents of CSV files and process them
process_csv_files_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv,
    dag=dag2,
)

# Compute monthly averages using Apache Beam
compute_monthly_avg_task = PythonOperator(
    task_id='compute_monthly_averages',
    python_callable=compute_monthly_avg,
    dag=dag2,
)

# Use 'geopandas' and 'geodatasets' create a visualisation
create_heatmap_task = PythonOperator(
    task_id='create_heatmap_visualization',
    python_callable=create_heatmap_visualization,
    dag=dag2,
)

#Upon completion, delete the CSV Files from the destination
delete_csv_task = PythonOperator(
    task_id='delete_csv_file',
    python_callable=delete_csv,
    dag=dag2,
)

wait_for_zip >> unzip_file_task >> process_csv_files_task >> compute_monthly_avg_task >> create_heatmap_task >> delete_csv_task
