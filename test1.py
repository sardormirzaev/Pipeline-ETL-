import pandas as pd
import numpy as np
from io import StringIO
import zipfile,urllib
    
from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 12, 5),
    'retries': 1
}


def storeData(**context):
    df= context['task_instance'].xcom_pull(task_ids='load_Data')
    import pandas as pd
    from google.cloud import bigquery
    from io import StringIO
    import zipfile,urllib
    client = bigquery.Client(project ='ninth-psyche-326613')
    job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(name="TID",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="CID",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="id",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="title",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="length",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="year",field_type="STRING", mode='REPEATED'),
            ],
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(
        df, 'challange.sapmle',defaut_project='ninth-psyche-326613',job_config=job_config
    )  
    print('Successfull')
    return job.result()

def storeData2(**context):
    df= context['task_instance'].xcom_pull(task_ids='load_Data2')
    import pandas as pd
    from google.cloud import bigquery
    from io import StringIO
    import zipfile,urllib
    client = bigquery.Client(project ='ninth-psyche-326613')
    job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(name="TID",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="CID",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="id",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="title",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="length",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="year",field_type="STRING", mode='REPEATED'),
            ],
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, 'challange.sapmle',defaut_project='ninth-psyche-326613',job_config=job_config
    )  
    print('Successfull')
    return job.result()


def loadData(**context):
    from pandas.io import gbq
    import pandas as pd
    from io import StringIO
    import zipfile,urllib
    from google.cloud import bigquery

    url="https://vsis-www.informatik.uni-hamburg.de/download/MusicBrainz/A01/musicbrainz-20000-A01.csv.zip"
    filehandle, _ = urllib.request.urlretrieve(url)
    zip_file_object = zipfile.ZipFile(filehandle, 'r')
    first_file = zip_file_object.namelist()[0]
    file = zip_file_object.open(first_file)
    df=pd.read_csv(StringIO(str(file.read(),'utf-8')))
    
    # Validation
    if df.empty:
        print("No Data downloaded. Finishing execution")
    else:
        pass
        # Check for nulls
    df=df.replace(" ",'')
    df.dropna(inplace=True)
    # Primary Key Check
    df.drop_duplicates(subset='CID',keep="first",inplace=True)
    df['year2']=df['year'].apply(lambda x: x.replace(" ", ""))
    df['C'] = pd.to_numeric(df['year2'].str.extract('(\d+)', expand=False))
    df['year']=df['C'].apply(lambda x: x+2000 if x<=10 else(1900+x if 15<x<100 else x))

    df['leng']=df['length'].apply(lambda x: x.replace(" ", ""))
    df['leng']=df['leng'].apply(lambda x: x.replace(".", ""))
    #df['leng3'] =df['leng2'].str.extract('(\d+)', expand=True)
    df['length']=df['leng'].apply(lambda x: '0'+x[:1]+':'+x[1:3]+':'+x[3:] if x[0]!='0' else x[:2]+':'+x[2:4]+':'+x[4:])
    df=df[["TID", 'CID',"CTID",'SourceID','id','number','title','length','artist','album','year','language']]
    return df.to_json()

def loadData2(**context):
    from pandas.io import gbq
    import pandas as pd
    from io import StringIO
    import zipfile,urllib
    from google.cloud import bigquery

    url="https://vsis-www.informatik.uni-hamburg.de/download/MusicBrainz/A01/musicbrainz-2000000-A01.csv.zip"
    filehandle, _ = urllib.request.urlretrieve(url)
    zip_file_object = zipfile.ZipFile(filehandle, 'r')
    first_file = zip_file_object.namelist()[0]
    file = zip_file_object.open(first_file)
    df=pd.read_csv(StringIO(str(file.read(),'utf-8')))
    
    # Validation
    if df.empty:
        print("No Data downloaded. Finishing execution")
    else:
        pass
        # Check for nulls
    df=df.replace(" ",'')
    df.dropna(inplace=True)
    # Primary Key Check
    df.drop_duplicates(subset='CID',keep="first",inplace=True)
    df['year2']=df['year'].apply(lambda x: x.replace(" ", ""))
    df['C'] = pd.to_numeric(df['year2'].str.extract('(\d+)', expand=False))
    df['year']=df['C'].apply(lambda x: x+2000 if x<=15 else(1900+x if 15<x<100 else x))

    df['leng']=df['length'].apply(lambda x: x.replace(" ", ""))
    df['leng']=df['leng'].apply(lambda x: x.replace(".", ""))
    #df['leng3'] =df['leng2'].str.extract('(\d+)', expand=True)
    df['length']=df['leng'].apply(lambda x: '0'+x[:1]+':'+x[1:3]+':'+x[3:] if x[0]!='0' else x[:2]+':'+x[2:4]+':'+x[4:])
    df=df[["TID", 'CID',"CTID",'SourceID','id','number','title','length','artist','album','year','language']]
    return df.to_json() 

def tryData():

    from pandas.io import gbq
    import pandas as pd
    from io import StringIO
    import zipfile,urllib
    from google.cloud import bigquery
    url="https://vsis-www.informatik.uni-hamburg.de/download/MusicBrainz/A01/musicbrainz-20000-A01.csv.zip"
    filehandle, _ = urllib.request.urlretrieve(url)
    zip_file_object = zipfile.ZipFile(filehandle, 'r')
    first_file = zip_file_object.namelist()[0]
    file = zip_file_object.open(first_file)
    df=pd.read_csv(StringIO(str(file.read(),'utf-8')))
    
    # Validation
    if df.empty:
        print("No Data downloaded. Finishing execution")
    else:
        pass
        # Check for nulls
    df=df.replace(" ",'')
    df.dropna(inplace=True)
    # Primary Key Check
    df.drop_duplicates(subset='CID',keep="first",inplace=True)
    df['year2']=df['year'].apply(lambda x: x.replace(" ", ""))
    df['C'] = pd.to_numeric(df['year2'].str.extract('(\d+)', expand=False))
    df['year']=df['C'].apply(lambda x: x+2000 if x<=15 else(1900+x if 15<x<100 else x))

    df['leng']=df['length'].apply(lambda x: x.replace(" ", ""))
    df['leng']=df['leng'].apply(lambda x: x.replace(".", ""))
    #df['leng3'] =df['leng2'].str.extract('(\d+)', expand=True)
    df['length']=df['leng'].apply(lambda x: '0'+x[:1]+':'+x[1:3]+':'+x[3:] if x[0]!='0' else x[:2]+':'+x[2:4]+':'+x[4:])
    df=df[["TID", 'CID',"CTID",'id','title','length''year']]    
    client = bigquery.Client(project ='ninth-psyche-326613')
    url="https://vsis-www.informatik.uni-hamburg.de/download/MusicBrainz/A01/musicbrainz-20000-A01.csv.zip"
    filehandle, _ = urllib.request.urlretrieve(url)
    zip_file_object = zipfile.ZipFile(filehandle, 'r')
    first_file = zip_file_object.namelist()[0]
    file = zip_file_object.open(first_file)
    df=pd.read_csv(StringIO(str(file.read(),'utf-8')))

    job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(name="TID",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="CID",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="id",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="title",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="length",field_type="STRING", mode='REPEATED'),
                bigquery.SchemaField(name="year",field_type="STRING", mode='REPEATED'),
            ],
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, 'challange.sapmle',defaut_project='ninth-psyche-326613',job_config=job_config
    )  
    return job.result()
dag = DAG(
    'DAG1',
    default_args=default_args,
    description=' Test Data',
    schedule_interval='@once',
)
start = DummyOperator(
    task_id="start",
    dag=dag
)
t1 = PythonOperator(
    task_id='load_Data',
    provide_context=True,
    python_callable=loadData,
    dag=dag,
)
t11 = PythonOperator(
    task_id='load_Data2',
    provide_context=True,
    python_callable=loadData2,
    dag=dag,
)

t2 = PythonOperator(
    task_id='store_data',
    provide_context=True,
    python_callable=storeData,
    dag=dag,
)
t22 = PythonOperator(
    task_id='store_data2',
    provide_context=True,
    python_callable=storeData2,
    dag=dag,
)
t3 = PythonOperator(
    task_id='try_data',
    python_callable=tryData,
    dag=dag,
)
end = DummyOperator(
    task_id="end",
    dag=dag
)
start>>t1>>t2>>t3>>end 

start>>t11>>t22>>t3>>end 