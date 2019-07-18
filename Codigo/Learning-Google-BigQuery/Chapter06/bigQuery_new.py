import os
import uuid
from google.cloud import bigquery
import datetime,time
from pprint import pprint
from bigquery.client import JOB_WRITE_TRUNCATE,JOB_CREATE_IF_NEEDED,JOB_WRITE_APPEND
from google.cloud.exceptions import NotFound

#This is the schema to for the demo table to be created.
#GCPProjectID = "my-first-project-170319"
TableSchemaDefinition = ['EmployeeID:INTEGER','FirstName:STRING','LastName:STRING','JoiningDate:DATE','Employeelocation:STRING'];
LoadFilePath = "gs://myfirstprojectbucket201706/employeedetails.csv"

def main():
    bqclient = bigquery.Client.from_service_account_json(os.getcwd() + '/ProdProject.json')
    # ListAllDatasetsAndTableInProject(bqclient)


    newDatasetName = datetime.datetime.today().strftime("SampleDataset_%Y%m")
    sampleDataset = CreateNewDataset(bqclient, newDatasetName)
    newTableName = "EmployeeDetails_Partition3"
    employeeTable = CreateNewTable(bqclient, newDatasetName, newTableName, "Employees list")
    LoadDataFromFileToTable(bqclient,newDatasetName, newTableName)
    ExecuteQueryAndDisplayResults(bqclient, sampleDataset, employeeTable)


    ExecuteQueryAndCopyRows(bqclient, sampleDataset, employeeTable, "EmployeeDetails_Copy")
    InsertRowsViaAPI(bqclient, sampleDataset, employeeTable)
    print(" ")

def ListAllDatasetsAndTableInProject(bqclient):
    print("BIGQUERY DATASETS:")
    for dataset in bqclient.list_datasets(include_all=True):
        print("|")
        print("`-- Dataset Name: {}".format(dataset.dataset_id))
        print('\n|  +--Table name:'),
        print('\n|  +--Table name: '.join([str(table.table_id) for table in bqclient.list_dataset_tables(dataset)]))
    print("--------------------------------------------------------")


def dataset_exists(dataset, client):
    try:
        client.get_dataset(dataset)
        return True
    except NotFound:
        return False

def table_exists(table, client):
    try:
        client.get_table(table)
        return True
    except NotFound:
        return False

def CreateNewDataset(bqClient,newDatasetName):

    dataset_ref = bqClient.dataset(newDatasetName)
    if not dataset_exists(dataset_ref,bqClient):

        dataset = bqClient.create_dataset(bigquery.Dataset(dataset_ref))

        print('Created dataset {}.'.format(dataset.dataset_id))
        return dataset
    else:
        print("Dataset already exists")
        return dataset_ref

def CreateNewTable(bqClient,newDatasetName,newTableName,tableDescription):
    dataset_ref = bqClient.dataset(newDatasetName)

    table_ref = dataset_ref.table(newTableName)
    table = bigquery.Table(table_ref)

    if not table_exists(table, bqClient):#table.exists():

        table_ref = dataset_ref.table(newTableName)
        table = bigquery.Table(table_ref)

        for fieldDetails in TableSchemaDefinition:
            fieldDetail = fieldDetails.split(':')
            table.schema += (bigquery.SchemaField(fieldDetail[0], fieldDetail[1]),)

        table = bqClient.create_table(table)#,description=tableDescription)

        print('Created table {} in dataset {}.'.format(newTableName, newDatasetName))
        print("New table created successfully")
        return table
    else:
        print("Table already exists")
        return table


def LoadDataFromFileToTable(bqClient,newDatasetName,newTableName):

    dataset_ref = bqClient.dataset(newDatasetName)
    table_ref = dataset_ref.table(newTableName)

    # job_config = bigquery.CSVOptions()
    job_config=bigquery.LoadJobConfig()
    job_config.source_format = "CSV"
    job_config.skip_leading_rows = 1
    job_config.write_disposition = JOB_WRITE_TRUNCATE

    job = bqClient.load_table_from_uri(LoadFilePath, table_ref,job_config=job_config)
    result=job.result()  # Wait for job to complete

    print(" job-type = {}".format(job.job_type))
    print("Load job status")
    print(result.state)
    print("Load job statistics")
    #print(result.statistics)

def ExecuteQueryAndDisplayResults(bqClient, sampleDataset, sampleTable):
    query = """SELECT * FROM `{}.{}` WHERE EmployeeID BETWEEN @MinEmployeeID and @MaxEmployeeID""".format(sampleDataset.dataset_id,sampleTable.table_id)

    query_parameters = [
        bigquery.ScalarQueryParameter('MinEmployeeID', 'INT64', 1),
        bigquery.ScalarQueryParameter('MaxEmployeeID', 'INT64', 5)
    ]

    job_config = bigquery.QueryJobConfig()
    job_config.query_parameters = query_parameters
    job_config.use_legacy_sql = False
    query_job = bqClient.query(query, job_config=job_config)

    # bqQueryJob.begin()

    bqJobResult=query_job.result()
    destination_table_ref = query_job.destination
    table = bqClient.get_table(destination_table_ref)
    for row in bqClient.list_rows(table,start_index=1,max_results=10):#,timeout_ms=int(time.time()+10)):
        print("------------------------------------------------------------------------")
        print(' | '.join([str(result) for result in row]))
        print("------------------------------------------------------------------------")

def ExecuteQueryAndCopyRows(bqClient,sampleDataset,sampleTable,destinationTableName):
    query = """SELECT * FROM `{}.{}` WHERE EmployeeID BETWEEN @MinEmployeeID and @MaxEmployeeID""".format(
        sampleDataset.dataset_id, sampleTable.table_id)

    query_parameters = [
        bigquery.ScalarQueryParameter('MinEmployeeID', 'INT64', 1),
        bigquery.ScalarQueryParameter('MaxEmployeeID', 'INT64', 5)
    ]


    newDatasetName = datetime.datetime.today().strftime("SampleDataset_%Y%m")

    dataset = bqClient.dataset(newDatasetName)
    table_ref = dataset.table(destinationTableName)
    # table = bigquery.Table(table_ref)


    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.query_parameters = query_parameters
    job_config.allow_large_results = True
    job_config.create_disposition=JOB_CREATE_IF_NEEDED
    job_config.default_dataset = sampleDataset
    job_config.write_disposition = JOB_WRITE_APPEND
    job_config.use_legacy_sql = False
    job_config.use_query_cache = True
    query_job = bqClient.query(query, job_config=job_config)
    query_job.result()

def InsertRowsViaAPI(bqClient, sampleDataset, employeeTable):

    table = bqClient.get_table(employeeTable)

    data=[(21,"Jeffery","Sng","2017-08-01","Singapore"),(21,"Jeffery","Sng","2017-08-01","Singapore")]
    errors = bqClient.create_rows(table, data)

    if not errors:
        print('Loaded  rows into {} <-> {}'.format(sampleDataset.dataset_id, employeeTable.table_id))
    else:
        print('Errors:')
        pprint(errors)

if __name__=="__main__":
    main()

