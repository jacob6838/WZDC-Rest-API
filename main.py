import os
import pyodbc
import hashlib
import uuid
import math
import re

from fastapi import FastAPI, Header, HTTPException, status, Request, Query
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError

auth_email = os.environ['auth_contact_email']

app = FastAPI(
    title="Work Zone Data Collection Tool Rest API",
    description='This API hosts work zone data collected by the WZDC ' +
    '(work zone data collection) tool. This data includes RSM messages, both in xml and uper (binary) formats. This API ' +
    f'requires an APi key in the header. Contact <a href="mailto: {auth_email}">{auth_email}</a> for more information on how to acquire and use an API key.',
    docs_url="/",
)

storage_conn_str = os.environ['storage_connection_string']
sql_conn_str = os.environ['sql_connection_string']
blob_service_client = BlobServiceClient.from_connection_string(
    storage_conn_str)

cnxn = pyodbc.connect(sql_conn_str)
cursor = cnxn.cursor()

storedProcFind = os.environ['stored_procedure_find_key']

authorization_key_header = 'auth_key'

container_name = os.environ['source_container_name']

file_types_dict = {
    'rsm-xml': {
        'subdir': 'rsm-xml',
        'list_endpoint': 'rsm-xml',
        'name_prefix': 'rsm-xml',
        'file_type': 'xml'
    },
    'rsm-uper': {
        'subdir': 'rsm-uper',
        'list_endpoint': 'rsm-uper',
        'name_prefix': 'rsm-uper',
        'file_type': 'uper'
    },
    'wzdx': {
        'subdir': 'wzdx',
        'list_endpoint': 'wzdx',
        'name_prefix': 'wzdx',
        'file_type': 'geojson'
    }
}


@app.get("/wzdx", tags=["wzdx-list"])
def get_wzdx_files_list(request: Request,
                        center: str = Query('', title='Center', description='Center of query location, in the format "lat,long"',
                                            regex='^(-?\\d+(\\.\\d+)?),\\s*(-?\\d+(\\.\\d+)?)$'),
                        distance: float = Query(
                            0, title='Distance', description='Maximum distance (in km) from center location'),
                        county: str = Query(
                            None, title='County', description='County'),
                        state: str = Query(
                            None, title='State', description='State'),
                        zip_code: str = Query(
                            None, title='Zip Code', description='Zip code'),
                        ):
    file_type = 'wzdx'

    auth_key = request.headers.get(authorization_key_header)
    valid = authenticate_key(auth_key)
    if not valid:
        get_correct_response(auth_key)

    check_dist = False
    ref_loc = parseCoordinates(center)
    if not distance == 0 and ref_loc:
        ref_dist = distance
        check_dist = True

    location_params = []

    for val in [{'name': 'county_names', 'value': county},
                {'name': 'state_names', 'value': state},
                {'name': 'zip_code', 'value': zip_code}]:
        if val['value']:
            location_params.append(val)

    if check_dist:
        return getFilesByDistance(file_type, container_name, ref_loc, ref_dist)
    elif county or state or zip_code:
        return getFilesByMetadata(file_type, container_name, location_params)
    else:
        return getFilesByType(file_type, container_name)


@app.get("/wzdx/{file_name}", tags=["wzdx-file"])
def get_wzdx_file(request: Request, file_name: str):
    file_type = 'wzdx'

    auth_key = request.headers.get(authorization_key_header)
    valid = authenticate_key(auth_key)
    if not valid:
        get_correct_response(auth_key)

    return getFilesListByName(file_type, file_name, container_name)


@app.get("/rsm-xml", tags=["xml-list"])
def get_rsm_files_list_location_filter(request: Request,
                                       center: str = Query('', title='Center', description='Center of query location, in the format: lat,long',
                                                           regex='^(-?\\d+(\\.\\d+)?),\\s*(-?\\d+(\\.\\d+)?)$'),
                                       distance: float = Query(
                                           0, title='Distance', description='Maximum distance (in km) from center location'),
                                       county: str = Query(
                                           None, title='County', description='County'),
                                       state: str = Query(
                                           None, title='State', description='State'),
                                       zip_code: str = Query(
                                           None, title='Zip Code', description='Zip code'),
                                       ):
    file_type = 'rsm-xml'

    auth_key = request.headers.get(authorization_key_header)
    valid = authenticate_key(auth_key)
    if not valid:
        get_correct_response(auth_key)

    check_dist = False
    ref_loc = parseCoordinates(center)
    if not distance == 0 and ref_loc:
        ref_dist = distance
        check_dist = True

    location_params = []

    for val in [{'name': 'county_names', 'value': county},
                {'name': 'state_names', 'value': state},
                {'name': 'zip_code', 'value': zip_code}]:
        if val['value']:
            location_params.append(val)

    if check_dist:
        return getFilesByDistance(file_type, container_name, ref_loc, ref_dist)
    elif county or state or zip_code:
        return getFilesByMetadata(file_type, container_name, location_params)
    else:
        return getFilesByType(file_type, container_name)


@app.get("/rsm-xml/{file_name}", tags=["xml-file"])
def get_rsm_file(request: Request, file_name: str):
    file_type = 'rsm-xml'

    auth_key = request.headers.get(authorization_key_header)
    valid = authenticate_key(auth_key)
    if not valid:
        get_correct_response(auth_key)

    return getFilesListByName(file_type, file_name, container_name)


@app.get("/rsm-uper", tags=["uper-list"])
def get_rsm_uper_files_list(request: Request,
                            center: str = Query('', title='Center', description='Center of query location, in the format: lat,long',
                                                regex='^(-?\\d+(\\.\\d+)?),\\s*(-?\\d+(\\.\\d+)?)$'),
                            distance: float = Query(
                                0, title='Distance', description='Maximum distance (in km) from center location'),
                            county: str = Query(
                                None, title='County', description='County'),
                            state: str = Query(
                                None, title='State', description='State'),
                            zip_code: str = Query(
                                None, title='Zip Code', description='Zip code'),
                            ):
    file_type = 'rsm-uper'

    auth_key = request.headers.get(authorization_key_header)
    valid = authenticate_key(auth_key)
    if not valid:
        get_correct_response(auth_key)

    check_dist = False
    ref_loc = parseCoordinates(center)
    if not distance == 0 and ref_loc:
        ref_dist = distance
        check_dist = True

    location_params = []

    for val in [{'name': 'county_names', 'value': county},
                {'name': 'state_names', 'value': state},
                {'name': 'zip_code', 'value': zip_code}]:
        if val['value']:
            location_params.append(val)

    if check_dist:
        return getFilesByDistance(file_type, container_name, ref_loc, ref_dist)
    elif county or state or zip_code:
        return getFilesByMetadata(file_type, container_name, location_params)
    else:
        return getFilesByType(file_type, container_name)


@app.get("/rsm-uper/{rsm_name}", tags=["uper-file"])
def get_rsm_uper_file(request: Request, rsm_name: str):
    file_type = 'rsm-uper'

    auth_key = request.headers.get(authorization_key_header)
    valid = authenticate_key(auth_key)
    if not valid:
        get_correct_response(auth_key)

    return getFilesListByName(file_type, rsm_name, container_name)


def authenticate_key(key):
    try:
        key_hash = str(hashlib.sha256(key.encode()).hexdigest())
        print(key_hash)
        return find_key(key_hash)
    except:
        return False


def get_correct_response(auth_key):
    if not auth_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No authentication key was specified. If you have a key, please add auth_key: **authentication_key** to your " +
            f"request header. If you do not have a key, email {auth_email} to get a key.",
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication key",
        )


def find_key(key_hash):
    cursor.execute(storedProcFind.format(key_hash))

    row = cursor.fetchone()

    if row:
        return True
    else:
        return False


def validNumOrNone(values):
    value1, value2 = values
    if re.match('^-?[0-9]e\\+[0-9]{2}$', str(value1)) or re.match('^-?([0-9]*[.])?[0-9]+$', str(value1)):
        value1 = float(value1)
    else:
        return None

    if re.match('^-?[0-9]e\\+[0-9]{2}$', str(value2)) or re.match('^-?([0-9]*[.])?[0-9]+$', str(value2)):
        value2 = float(value2)
    else:
        return None

    return value1, value2


def getDist(origin, destination):
    origin = validNumOrNone(origin)
    destination = validNumOrNone(destination)

    if not origin or not destination:
        return None

    lat1, lon1 = origin  # lat/lon of origin
    lat2, lon2 = destination  # lat/lon of dest

    radius = 6371.0*1000  # meters

    dlat = math.radians(lat2-lat1)  # in radians
    dlon = math.radians(lon2-lon1)

    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c

    return d


def getWZId(file_type, name):
    type_values = file_types_dict[file_type]

    begin_str = '{:s}--'.format(type_values['name_prefix'])
    end_str = '--1-of-1.{:s}'.format(type_values['file_type'])
    alt_end_str = '.{:s}'.format(type_values['file_type'])

    name = name.split('/')[-1]
    if name.startswith(begin_str):
        name = name[len(begin_str):]
    if name.endswith(end_str):
        name = name[:-len(end_str)]
    elif name.endswith(alt_end_str):
        name = name[:-len(alt_end_str)]
    return name


def parseCoordinates(center_str):
    if type(center_str) != str:
        return None

    ref_loc = None
    center_split = center_str.split(',')
    if len(center_split) == 2:
        ref_loc = validNumOrNone(
            (center_split[0].strip(), center_split[1].strip()))
    return ref_loc


def getBlobOrNoneByDistance(file_type, blob, ref_loc, ref_dist):
    begin_loc = (blob.metadata.get('beginning_lat'),
                 blob.metadata.get('beginning_lon'))
    end_loc = (blob.metadata.get('ending_lat'),
               blob.metadata.get('ending_lon'))

    if begin_loc[0] and begin_loc[1] and end_loc[0] and end_loc[1]:
        center_loc = ((float(begin_loc[0])+float(end_loc[0]))/2,
                      (float(begin_loc[1])+float(end_loc[1]))/2)
        blob_dist = getDist(ref_loc, center_loc) / 1000  # Convert meters to km
        if blob_dist and blob_dist <= ref_dist:
            return {'name': getWZId(file_type,
                                    blob.name), 'id': blob.metadata.get('group_id', 'unknown')}
        else:
            pass
    return None


def getFilesListByName(file_type, rsm_name, container_name):
    type_values = file_types_dict[file_type]
    name_beginning = '{0}/{1}--{2}'.format(
        type_values['subdir'], type_values['name_prefix'], rsm_name)

    # For RSM files, multiple files can exist for a single work zone. Thus, these files have --i-of-N at the end of the name
    if file_type == 'rsm-xml' or file_type == 'rsm-uper':
        initial_blob_name = '{0}--1-of-1.{1}'.format(
            name_beginning, type_values['file_type'])
    else:
        initial_blob_name = '{0}.{1}'.format(
            name_beginning, type_values['file_type'])

    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=initial_blob_name)
    files = []

    try:
        group_id = blob_client.get_blob_properties().metadata.get('group_id', 'unknown')
    except:
        raise HTTPException(
            status_code=404,
            detail=f"Specified {file_type} file not found. Try using the {type_values['list_endpoint']} endpoint to return a list of current files",
        )

    if group_id != 'unknown':
        container_client = blob_service_client.get_container_client(
            container_name)
        blob_list = container_client.list_blobs(
            name_starts_with=name_beginning, include='metadata')
        for blob in blob_list:
            if blob.metadata.get('group_id') == group_id:
                if file_type == 'rsm-uper':
                    files.append({'source_name': blob.name, 'size': blob.size, 'data': str(blob_service_client.get_blob_client(
                        container=container_name, blob=blob.name).download_blob().readall())})
                else:
                    files.append({'source_name': blob.name, 'size': blob.size, 'data': blob_service_client.get_blob_client(
                        container=container_name, blob=blob.name).download_blob().readall().decode('utf-8')})

    return {'num_files': len(files), 'id': group_id, 'files': files}


def getFilesByDistance(file_type, container_name, ref_loc, ref_dist):
    type_values = file_types_dict[file_type]

    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(
        name_starts_with=type_values['subdir'] + '/', include='metadata')

    blob_names = []
    for blob in blob_list:
        if blob.metadata:
            entry = getBlobOrNoneByDistance(
                file_type, blob, ref_loc, ref_dist)
            if entry and entry not in blob_names:
                blob_names.append(entry)
        else:
            pass
    return {'query_parameters': {'distance': f'{ref_dist:.0f} km', 'center': [ref_loc[0], ref_loc[1]]}, 'data': blob_names}


def getFilesByType(file_type, container_name):
    type_values = file_types_dict[file_type]

    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(
        name_starts_with=type_values['subdir'] + '/', include='metadata')

    blob_names = []
    for blob in blob_list:
        if blob.metadata:
            blob_names.append({'name': getWZId(file_type, blob.name),
                               'id': blob.metadata.get('group_id', 'unknown')})

    return {'query_parameters': None, 'data': blob_names}


def getFilesByMetadata(file_type, container_name, query_params):
    print(query_params)
    type_values = file_types_dict[file_type]

    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs(
        name_starts_with=type_values['subdir'] + '/', include='metadata')

    blob_names = []
    for blob in blob_list:
        if blob.metadata:
            valid = True

            for param in query_params:
                values = [x.lower()
                          for x in blob.metadata.get(param['name'], '').split(',')]
                if param['value'] and param['value'].lower() not in values:
                    valid = False

            if valid:
                blob_names.append({'name': getWZId(file_type, blob.name),
                                   'id': blob.metadata.get('group_id', 'unknown')})

    formatted_query_params = []
    for param in query_params:
        formatted_query_params.append({param["name"]: param["value"]})

    return {'query_parameters': formatted_query_params, 'data': blob_names}
