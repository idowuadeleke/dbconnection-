import adal
import requests
import json
from constants import AUTHENTICATION_ENDPOINT,RESOURCE


def create_database(db_details):
    parameters={}

    #convert input details format to dictionary
    for data in db_details:
        db_data = {key: value for (key, value) in data.items()}
        parameters.update(db_data)



    context = adal.AuthenticationContext(AUTHENTICATION_ENDPOINT + parameters.get('tenant_id'))

    token_response = context.acquire_token_with_client_credentials(
     RESOURCE, parameters.get('application_id'), parameters.get('client_secret'))

    #get access token
    access_token = token_response.get('accessToken')

    #add authorization token to request header
    headers = {"Authorization": 'Bearer ' + access_token,'Content-Type': 'application/json', 'Accept': 'application/json'}

    data={"location":parameters.get('location')}

    config={"subscriptionId":parameters.get('subscriptionId'),"resourceGroupName":parameters.get('resourceGroupName'),"serverName":parameters.get('serverName'), "databaseName":parameters.get('databaseName')}

    #endpoint to call to create database
    endpoint='https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/Microsoft.Sql/servers/{serverName}/databases/{databaseName}?api-version=2017-10-01-preview'.format(**config)



    output=requests.put(endpoint,headers=headers,data=json.dumps(data))
        
    if output.status_code == 202:
        status= output.json().get('operation')
    else:
        try:
            status= output.json().get('error', '').get('message')
        except:
            status='An error occured, status code is {}'.format(output.status_code)

    return status     
  


####test create database
# sample_detail=[{'tenant_id':'b224a507-3691-4035-a6bc-342513ebefc6'},{'application_id':'4fe38871-cf27-414c-b6ae-c6069d4347bc'},
#                 {'client_secret':'8HKyPtBP4S8cJha+lx6duSNjd/SVKo+SxeDzRQar9xg= '},{'location':'Australia East'},
#                 {'subscriptionId':'9d6c265a-c13a-4f60-a557-dc689eaa1471'},{'resourceGroupName':'newresource'},
#                 {'serverName':'idowu'},{'databaseName':'testdb'}]

# response=create_database(sample_detail)
# print(response)