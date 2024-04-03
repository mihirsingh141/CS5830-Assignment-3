from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor


import os
from zipfile import ZipFile


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import apache_beam as beam
import logging
import shutil

import geopandas as gpd
from geodatasets import get_path

from ast import literal_eval as make_tuple




## Task 2

# Function to unzip the files
def unzip(**context):
    with ZipFile("/root/airflow/DAGS/data.zip", 'r') as zObject: 
      zObject.extractall(path="/root/airflow/DAGS/files") 


# Function to parse a csv file
def read_csv(data):
    df = data.split(',')
    df[0] = df[0].strip('"')
    df[-1] = df[-1].strip('"')

    return list(df)

class ExtractandFilter(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []

        # List of all possible headers in the dataset
        headers = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]

        for i,header in enumerate(headers):
            if 'hourly' in header.lower():
                for field in required_fields:
                    if field.lower() in header.lower():
                        self.required_fields.append(i)
        
        self.headers = {header:idx for idx,header in enumerate(headers)}

        # Function to get the required labels from the csv file
        def process(self,element):
            latitude = element[self.headers['LATITUDE']]
            longitude = element[self.headers['LONGITUDE']]

            data = []
            for idx in self.required_fields:
                data.append(element[idx])

            # Ensuring that the row being processed is not the header row
            if latitude != 'LATITUDE':
                yield ((latitude,longitude),data)


# Function to run the defined beam
def process_csv(**kwargs):
    # Exploring based on WindSpeed
    required_fields = ['LATITUDE','LONGITUDE','HourlyWindGustSpeed']
    os.makedirs('/root/airflow/DAGS/files/results1', exist_ok=True)

    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/root/airflow/DAGS/files/*.csv')
            | 'ParseData' >> beam.Map(read_csv)
            | 'FilterAndCreateTuple' >> beam.ParDo(ExtractandFilter(required_fields=required_fields))
            | 'CombineTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )

        result | 'WriteToText' >> beam.io.WriteToText('/root/airflow/DAGS/files/results1/result.txt')



class ExtractWithMonth(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        self.required_fields = []

        # List of all possible headers in the dataset
        headers = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]

        for i,header in enumerate(headers):
            if 'hourly' in header.lower():
                for field in required_fields:
                    if field.lower() in header.lower():
                        self.required_fields.append(i)
        
        self.headers = {header:idx for idx,header in enumerate(headers)}


    def process(self,element):
        latitude = element[self.headers['LATITUDE']]
        longitude = element[self.headers['LONGITUDE']]

        data = []
        for idx in self.required_fields:
            data.append(element[idx])

        # Ensuring it is not a header row
        if latitude != 'LATITUDE':
            meas_time = datetime.strptime(element[self.headers['DATE']],'%Y-%m-%dT%H:%M:%S')
            month_format = "%Y-%m"
            month = meas_time.strftime(month_format)
            yield ((month, latitude, longitude), data)

    
# Function to compute the mean
def compute_mean(data):
    values = np.array(data[1])
    values_shape = values.shape

    values = pd.to_numeric(values.flatten(),errors='coerce',downcast='float')
    values = np.reshape(values,values_shape)

    # Removing the null values from the calculations
    masked_values = np.ma.masked_array(values,np.isnan(values))

    result = np.ma.average(masked_values,axis=0)
    result = list(result.filled(np.nan)) 

    logger = logging.getLogger(__name__)
    logger.info(result)
    return ((data[0][1],data[0][2]),result)
        
            
def compute_month_avg(**kwargs):
    required_fields = ['LATITUDE','LONGITUDE','HourlyWindGustSpeed']
    os.makedirs('/root/airflow/DAGS/files/results2', exist_ok=True)

    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/root/airflow/DAGS/files/*.csv')
            | 'ParseData' >> beam.Map(read_csv)
            | 'CreateTupleWithMonthInKey' >> beam.ParDo(ExtractWithMonth(required_fields=required_fields))
            | 'CombineTupleMonthly' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda data: compute_mean(data))
            | 'CombineTuplewithAverages' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )
        result | 'WriteAveragesToText' >> beam.io.WriteToText('/root/airflow/DAGS/files/results2/averages.txt')


class aggregated(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]

        for header in headers_csv:
            if 'hourly' in header.lower():
                for field in required_fields:
                    if field.lower() in header.lower():
                        self.required_fields.append(header.replace('Hourly',''))

    
    def create(self):
        return []
    
    def add(self, accumulator, element):
        acc = {key:val for key,val in accumulator}

        data = element[2]
        val_data = np.array(data)
        val_data_shape = val_data.shape
        val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float')
        val_data = np.reshape(val_data,val_data_shape)

        masked_data = np.ma.masked_array(val_data, np.isnan(val_data))

        result = np.ma.average(masked_data, axis=0)
        result = list(result.filled(np.nan))

        for idx,i in enumerate(self.required_fields):
            acc[i] = acc.get(i,[]) + [(element[0],element[1],result[idx])]

        return list(acc.items())
    
    def merge(self,accumulators):
        merged = {}

        for acc in accumulators:
            a = {key:val for key,val in acc}

            for field in self.required_fields:
                merged[field] = merged.get(field,[]) + a.get(field,[])

        
        return list(merged.items)
    

    def extract(self,accumulator):
        return accumulator
    


# Function to plot geomaps
def geomaps(data):
    logger = logging.getLogger(__name__)
    logger.info(data)
    values = np.array(data[1],dtype='float')
    

    df = gpd.GeoDataFrame({
        data[0]:values[:,2]
    }, geometry=gpd.points_from_xy(*values[:,(1,0)].T))

    _,ax = plt.subplots(1,1,figsize=(10,6))

    values.plot(column=data[0], marker='o', ax=ax, legend=True)
    plt.title(f'{data[0]} Heatmap')
    os.makedirs('/root/airflow/DAGS/results3', exist_ok=True)

    plt.savefig(f'/root/airflow/DAGS/results3/plot{data[0]}_heatmap_plot.png')

# Function to create plots with beam
def heatmap_visual(**kwargs):
    required_fields = ['LATITUDE','LONGITUDE','HourlyWindGustSpeed']
    with beam.Pipeline(runner='DirectRunner') as p:
        
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/root/airflow/DAGS/files/results2/averages.txt*')
            | 'preprocessParse' >>  beam.Map(lambda a:make_tuple(a.replace('nan', 'None')))
            | 'Global aggregation' >> beam.CombineGlobally(aggregated(required_fields = required_fields))
            | 'Flat Map' >> beam.FlatMap(lambda a:a) 
            | 'Plot Geomaps' >> beam.Map(geomaps)            
        )

# Function to delete a csv file
def delete_csv(**kwargs):
    shutil.rmtree('/root/airflow/DAGS/files')


dag2 = DAG(
    dag_id= 'analyzing_data',
    schedule_interval='@daily',
    default_args={
            'owner': 'first_task',
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            'start_date': datetime(2024, 1, 1),
        },
    catchup=False
)

wait = FileSensor(
    task_id = 'wait',
    mode="poke",
    poke_interval = 5,  # Check every 5 seconds
    timeout = 5,  # Timeout after 5 seconds
    filepath = "/root/airflow/DAGS/data.zip",
    dag=dag2,
    fs_conn_id = "fs_default", # File path system must be defined
)


unzip_task = PythonOperator(
        task_id="unzip",
        python_callable=unzip,
        provide_context=True,
        dag=dag2
    )

process_csv_files_task = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv,
    dag=dag2,
)

compute_monthly_avg_task = PythonOperator(
    task_id='compute_monthly_means',
    python_callable=compute_month_avg,
    dag=dag2,
)

create_heatmap_task = PythonOperator(
    task_id='heatmap_visualization',
    python_callable=heatmap_visual,
    dag=dag2,
)

delete_csv_task = PythonOperator(
    task_id='delete_csv_file',
    python_callable=delete_csv,
    dag=dag2,
)


# Defining the structure of the DAG
wait >> unzip_task >> process_csv_files_task >> compute_monthly_avg_task >> create_heatmap_task >> delete_csv_task