
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import timedelta, datetime

import pandas as pd
import io
import pyarrow as pa
import pyarrow.parquet as pq
from azure.storage.blob import BlobServiceClient

default_args = {
    'owner':'Naveen',
    'retries': 3,
    'retry_delay': timedelta(seconds = 5)
}

def run_bronze_to_silver():
    connstr = BaseHook.get_connection("marketingdatalake6_connstr")
    dl_connstr = connstr.password
    bronze_container = Variable.get("br_cont")
    silver_container = Variable.get("silverCont")
    blob_service_client = BlobServiceClient.from_connection_string(dl_connstr)
    container_client = blob_service_client.get_container_client(bronze_container)


    # list all blobs in the container
    blobs = container_client.list_blobs()
    files_to_process = [blob.name for blob in blobs]    

    if not files_to_process:
        print("No new files in the bronze container")
        return

    load_date = datetime.now().strftime("%Y-%m-%d")
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for blob_name in files_to_process:
        blob_client = container_client.get_blob_client(blob_name)
        blob_data = blob_client.download_blob().readall()

        df = pd.read_csv(io.BytesIO(blob_data))

        # Type conversions    
        df["Date"] = pd.to_datetime(df["Date"])

        # Duration: "30 days" → 30
        df["Duration"] = (
            df["Duration"]
            .str.split(" ")
            .str[0]
            .astype(int)
        )

        # Target audience split
        df["Gender"] = df["Target_Audience"].str.split(" ").str[0]
        df["Gender"] = df["Gender"].replace("All", "Both")

        df["AgeRange"] = df["Target_Audience"].str.split(" ").str[1]
        df["AgeRange"] = df["AgeRange"].replace("Ages", "All ages")


        # Drop redundant column
        df_silver = df.drop(columns=["Target_Audience"]).copy()

        df_silver["Acquisition_Cost"] = (
            df_silver["Acquisition_Cost"]
            .str.replace("$", "", regex=False)
            .astype(float)
        )

        # Write Silver data
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df_silver)  
        pq.write_table(table, buffer)
        buffer.seek(0)

        output_container_client = blob_service_client.get_container_client(silver_container)
        output_name = f"load_date={load_date}/{run_ts}_{blob_name.split("/")[-1].replace(".csv",".parquet")}"
        output_container_client.upload_blob(
            name = output_name,
            data = buffer,            
            overwrite = True
        )
        
        # moving processed raw files to bronze_processed
        processed_blobname = f"load_date={load_date}/{blob_name}"
        bronze_processed_container_client = blob_service_client.get_container_client("bronze-processed")
        dest_blob_client = bronze_processed_container_client.get_blob_client(processed_blobname)

        dest_blob_client.start_copy_from_url(blob_client.url)
        blob_client.delete_blob()

    print("Bronze to silver processing completed")

    
    

def run_gold():
    connstr = BaseHook.get_connection("marketingdatalake6_connstr")
    dl_connstr = connstr.password
    silver_container = Variable.get("silverCont")

    blob_service_client = BlobServiceClient.from_connection_string(dl_connstr)
    container_client = blob_service_client.get_container_client(silver_container)


    blobs = container_client.list_blobs()

    dfs = []

    # list all blobs in silver container & combining all silver parquet files into single dataframe
    blobs = container_client.list_blobs()  # here even subfolders will also be considered as blobs
    dfs = []
    for blob in blobs:
        if not blob.name.endswith(".parquet"):  # only parquet files
            continue
        elif blob.size == 0:   # skip empty parquet files
            continue
        blob_client = container_client.get_blob_client(blob.name)
        blob_data = blob_client.download_blob().readall()
        df = pd.read_parquet(io.BytesIO(blob_data))
        dfs.append(df)
    
    df_silver = pd.concat(dfs, ignore_index=True)

    # Gold transformation starts here

    gold_fact = df_silver.groupby(["Campaign_ID", "Channel_Used", "Date", "Location"], as_index = False).agg(
    total_clicks = ("Clicks", "sum"),
    total_impressions = ("Impressions", "sum"),
    total_acquisition_cost = ("Acquisition_Cost", "sum"),
    avg_eng_score = ("Engagement_Score", 'mean')
    )

    dim_campaign = (
    df_silver[['Campaign_ID', 'Campaign_Goal', 'Duration', 'Customer_Segment', 'Company']]
    .drop_duplicates()
    .reset_index(drop = True)
    )
    dim_campaign['Campaign_key'] = dim_campaign.index + 1

    dim_channel = df_silver[['Channel_Used']].drop_duplicates().reset_index(drop = True)
    dim_channel['Channel_key'] = dim_channel.index + 1 

    dim_date = df_silver[['Date']].drop_duplicates().reset_index(drop = True)
    dim_date['Year'] = dim_date['Date'].dt.year
    dim_date['Month'] = dim_date['Date'].dt.month
    dim_date['Quarter'] = dim_date['Date'].dt.quarter
    dim_date['Day_of_week'] = dim_date['Date'].dt.day_name()    
    dim_date['date_key'] = dim_date.index+1

    dim_audience = (df_silver[['Gender', 'AgeRange', 'Language', 'Customer_Segment']]
                .drop_duplicates()
                .reset_index(drop = True)
               )                
    dim_audience['audience_key'] = dim_audience.index + 1

    dim_location = df_silver[['Location']].drop_duplicates().reset_index(drop = True)
    dim_location['location_key'] = dim_location.index + 1

    dim_company = df_silver[['Company']].drop_duplicates().reset_index(drop = True)
    dim_company['Company_key'] = dim_company.index + 1

    df_silver = df_silver.drop(columns = ['ROI','Conversion_Rate'])

    gold_fact = gold_fact.merge(
    dim_campaign[['Campaign_key', 'Campaign_ID']],
    on = 'Campaign_ID',
    how = 'left'
    )
    gold_fact =  gold_fact.drop(columns = ['Campaign_ID'])


    gold_fact = gold_fact.merge(
    dim_channel,
    on = 'Channel_Used',
    how = 'left'
    )
    gold_fact = gold_fact.drop(columns = ['Channel_Used'])


    dim_campaign = dim_campaign.merge(
    dim_company,
    on = 'Company',
    how = 'left'
    )
    dim_campaign = dim_campaign.drop(columns = ['Company'])


    gold_fact = gold_fact.merge(
    dim_date[['Date', 'date_key']],
    on = 'Date',
    how = 'left'
    )
    gold_fact = gold_fact.drop(columns = ['Date'])


    gold_fact = gold_fact.merge(
    dim_location,
    on = 'Location',
    how = 'left'
    )
    gold_fact = gold_fact.drop(columns = ['Location'])




    group_cols = [
    "Campaign_ID",
    "Channel_Used",
    "Date",
    "Location",
    "Gender",
    "AgeRange",
    "Language",
    "Customer_Segment"
    ]

    audience_fact = (
        df_silver
        .groupby(group_cols, as_index=False)
        .agg(
            total_clicks=("Clicks", "sum"),
            total_impressions=("Impressions", "sum"),
            total_acquisition_cost=("Acquisition_Cost", "sum"),
            avg_eng_score=("Engagement_Score", "mean")
        )
    )

    audience_fact = audience_fact.merge(
    dim_campaign[["Campaign_key", "Campaign_ID"]],
    on="Campaign_ID",
    how="left"
    )

    audience_fact = audience_fact.merge(
    dim_channel[["Channel_key", "Channel_Used"]],
    on = 'Channel_Used',
    how="left"
    )

    audience_fact = audience_fact.merge(
    dim_date[["date_key", "Date"]],
    on = 'Date',
    how="left"
    )

    audience_fact = audience_fact.merge(
    dim_location,
    on="Location",
    how="left"
    )

    audience_fact = audience_fact.merge(
    dim_audience,
    on=["Gender", "AgeRange", "Language", "Customer_Segment"],
    how="left"
    )

    audience_fact = audience_fact.drop(
    columns=[
        "Campaign_ID",
        "Channel_Used",
        "Date",
        "Location",
        "Gender",
        "AgeRange",
        "Language",
        "Customer_Segment",
        "channel_name",
        "date"
    ],
    errors="ignore"
    )

    dbtables = [dim_audience, audience_fact, dim_company, dim_location, dim_date, dim_channel, dim_campaign, gold_fact]
    dim_audience.name = "dim_audience"
    dim_company.name = "dim_company"
    dim_location.name = "dim_location"
    dim_date.name = "dim_date"
    dim_channel.name = "dim_channel"
    dim_campaign.name = "dim_campaign"
    gold_fact.name = "gold_fact"
    audience_fact.name = "audience_fact"

    for dbtable  in dbtables:
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(dbtable)  
        pq.write_table(table, buffer)
        buffer.seek(0)
    
        output_container_client = blob_service_client.get_container_client("gold")
        # load_date = datetime.now().strftime("%Y-%m-%d")
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # output_name = f"{dbtable.name}/load_date={load_date}/{timestamp}.parquet"
        output_name = f"{dbtable.name}/part-000.parquet"
        output_container_client.upload_blob(
            name = output_name,
            data = buffer,
            overwrite = True
        )


with DAG(
    dag_id = 'project_etl_dag',
    description = 'dag to orchestrate the etl pipeline for the project',
    default_args = default_args,
    start_date = datetime(2025, 12, 25),
    schedule_interval = '@daily'
) as dag:
    
    start = EmptyOperator(task_id = "start")
    
    data_cleaning = PythonOperator(
        task_id = "bronze_to_silver_task",
        python_callable = run_bronze_to_silver #,
    )

    business_tables = PythonOperator(
        task_id = "silver_to_gold_task",
        python_callable = run_gold
    )
    
    end = EmptyOperator(task_id = "end")

    start >> data_cleaning >> business_tables >> end


