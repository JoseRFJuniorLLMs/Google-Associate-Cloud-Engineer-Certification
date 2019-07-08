# from google.client import bigquery
import os
import pprint
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json

TableSchemaDefinition = ['EmployeeID:INTEGER','FirstName:STRING','LastName:STRING','JoiningDate:DATE','Employeelocation:STRING'];

def CreateNewDataset(bqClient,newDatasetName):
    """Craetes a dataset in a given project.
    If no project is specified, then the currently active project is used.
    """
    dataset_ref = bqClient.dataset(newDatasetName)

    dataset = bqClient.create_dataset(bigquery.Dataset(dataset_ref))

    print('Created dataset {}.'.format(dataset.dataset_id))



def CreateNewTable(bigquery_client,newDatasetName,newTableName):
    """Creates a simple table in the given dataset.
    If no project is specified, then the currently active project is used.
    """
    dataset_ref = bigquery_client.dataset(newDatasetName)

    table_ref = dataset_ref.table(newTableName)
    table = bigquery.Table(table_ref)

    # Set the table schema
    # table.schema = (
    #     bigquery.SchemaField('Name', 'STRING'),
    #     bigquery.SchemaField('Age', 'INTEGER'),
    #     bigquery.SchemaField('Weight', 'FLOAT'),
    # )
    for fieldDetails in TableSchemaDefinition:
        fieldDetail = fieldDetails.split(':')
        table.schema += (bigquery.SchemaField(fieldDetail[0], fieldDetail[1]),)

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(newTableName, newDatasetName))

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

def insert_pubsub_messages(bqclient,message,**kwargs):

    database_name,table_name='pub_sub_demo_1','pull_messages'

    database = bqclient.dataset(database_name)

    # Get the table from the API so that the schema is available.
    dataset_ref = bqclient.dataset(database_name)
    table_ref = dataset_ref.table(table_name)
    table = bqclient.get_table(table_ref)

    if dataset_exists(database, bqclient):
        if table_exists(table, bqclient):
            data = [(kwargs.get('employee_id'), kwargs.get('first_name'), kwargs.get('last_name'), kwargs.get('date_of_joining'), kwargs.get('country'))]
            errors = bqclient.create_rows(table, data)
            if not errors:
                print('Loaded 1 row into {} <-> {}'.format(database_name, table_name))
            else:
                print('Errors:')
                pprint(errors)
        else:
            CreateNewTable(bqclient,database_name,table_name)
            insert_pubsub_messages(bqclient,message)
    else:
        CreateNewDataset(bqclient,database_name)
        insert_pubsub_messages(bqclient,message)

def ListRows(bqclient):
    database_name, table_name = 'pub_sub_demo_1', 'pull_messages'

    sampleDataset = bqclient.dataset(database_name)
    sampleTable = sampleDataset.table(table_name)

    query = """SELECT * FROM `{}.{}` """.format(
        sampleDataset.dataset_id, sampleTable.table_id)

    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = False
    query_job = bqclient.query(query, job_config=job_config)

    bqJobResult = query_job.result()
    destination_table_ref = query_job.destination
    table = bqclient.get_table(destination_table_ref)

    for row in bqclient.list_rows(table):
        print(' | '.join([str(result) for result in row]))


if __name__ == '__main__':
    bqclient = bigquery.Client.from_service_account_json(os.getcwd() + '/ProdProject.json')
    ListRows(bqclient)

