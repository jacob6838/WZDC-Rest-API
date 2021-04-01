# WZDC Tool Rest API

This APi is intended to make data collected by the [Work Zone Data Collection Tool](https://github.com/TonyEnglish/Work_Zone_Data_Collection_Toolset) more easily accessible. This APi supports listing/querying and downloading of individual files for RSM {xml}, RSM {upper}, and WZDx {geojson} messages. To acquire an API key, contact [tony@neaeraconsulting.com](mailto://tony@neaeraconsulting.com). The data accessed by this API is also vailable for download from the WZDC tool website, [https://neaeraconsulting.com/V2X_Published](https://neaeraconsulting.com/V2X_Published). 

## API Endpoint and Documentation
[https://wzdc-rest-api.azurewebsites.net](https://wzdc-rest-api.azurewebsites.net)

## Example usage (powershell):
`$response = Invoke-WebRequest 'https://wzdc-rest-api.azurewebsites.net/wzdx/' -Headers @{"auth_key"="*api_key*"}; $response.Content`

### List files and Query
#### Query RSM {xml} files by center/radius
`https://wzdc-rest-api.azurewebsites.net/rsm/uper/?center=40.061336,-105.212715&distance=10y`

#### Query RSM {uper} files by county
`https://wzdc-rest-api.azurewebsites.net/rsm/uper/?county=Larimer County`

#### Query RSM {uper} files by state
`https://wzdc-rest-api.azurewebsites.net/rsm/uper/?state=CO`
`https://wzdc-rest-api.azurewebsites.net/rsm/uper/?state=Colorado`

#### Query WZDx files by zipcode
`https://wzdc-rest-api.azurewebsites.net/rsm/uper/?zipcode=80528`

### Download individual filed
#### Download individual WZDx file by name
`https://wzdc-rest-api.azurewebsites.net/rsm/uper/main-demo--i-25`


## Running locally: 

### Install requirements
`pipn install -r requirements.txt`

### Configure environment variables
`$env:auth_contact_email = "*contact_email*";
$env:storage_connection_string = "*azure-storage-connnection-string*";
$env:sql_connection_string = "*sql-server-connnection-string*";
$env:stored_procedure_find_key = "exec find_key @key = '{0}'";
$env:source_container_name = "*storage-container-name*;"`

### Start local server
`python -m uvicorn main:app --reload`

### Example usage (powershell):
`$response = Invoke-WebRequest 'http://127.0.0.1:8000/rsm/xml/?center=40.061336,-105.212715&distance=10' -Headers @{"auth_key"="*api_key*"}; $response.Content`

