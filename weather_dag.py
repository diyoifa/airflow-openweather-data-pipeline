from airflow import DAG
from datetime import datetime, timezone, timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json
import pandas as pd


# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 15),  # Missing comma fixed
    'email': ['test1002542235@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

# Convert kelvin to fahrenheit
def kelvin_to_fahrenheit(kelvin):
    return (kelvin - 273.15) * 9/5 + 32

def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids='extract_weather_data')
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]

    # Get time zone difference
    timezone_offset = timedelta(seconds=data["timezone"])

    # Convert timestamps to datetime objects with the corresponding time zone
    time_of_record = datetime.fromtimestamp(data["dt"], tz=timezone.utc) + timezone_offset
    sunrise_time = datetime.fromtimestamp(data["sys"]["sunrise"], tz=timezone.utc) + timezone_offset
    sunset_time = datetime.fromtimestamp(data["sys"]["sunset"], tz=timezone.utc) + timezone_offset

    # Transformed data
    transformed_data = {
        "City": city,
        "Description": weather_description,
        "Temperature (F)": temp_fahrenheit,
        "Feels like (F)": feels_like_fahrenheit,
        "Minimun Temp (F)": min_temp_fahrenheit,
        "Max Temp (F)": max_temp_fahrenheit,
        "Pressure": pressure,
        "Humidity": humidity,
        "Wind speed": wind_speed,
        "Time Of Record": time_of_record,
        "Sunrise (Local Time)": sunrise_time,
        "Sunset (Local Time)": sunset_time
    }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    now = datetime.now(timezone.utc)  # Ensure consistent timezone handling
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    df_data.to_csv(f"s3://airflow-openweather/{dt_string}.csv", index=False)

# Define the DAG
with DAG(
    'weather_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    # Define the task
    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_id',
        endpoint='/data/2.5/weather?q=Portland&APPID={your_api_key_here}',
        method='GET',
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20,
    )

    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_id',
        endpoint='/data/2.5/weather?q=Portland&APPID={your_api_key_here}',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
