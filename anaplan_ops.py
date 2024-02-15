# ===============================================================================
# Created:        2 Feb 2023
# Updated:        30 Jan 2024
# @author:        Quinlan Eddy
# Description:    A test module to get Workspaces using a loop to test multithreading
# ===============================================================================


import logging
import requests
from concurrent.futures import ThreadPoolExecutor
import sys
import os
import json
import globals


# Enable logger
logger = logging.getLogger(__name__)


# === Interface with Anaplan REST API   ===
def anaplan_api(uri, verb, data=None, body={}, token_type="Bearer ", compress_upload_chunks=True):
    """
    Sends a request to the Anaplan API using the specified URI, HTTP verb, and request data.

    Args:
        uri (str): The URI of the API endpoint.
        verb (str): The HTTP verb to use for the request (e.g., 'GET', 'POST', 'PUT', 'DELETE', 'PATCH').
        data (bytes, optional): The data to send in the request body for 'PUT' requests. Defaults to None.
        body (dict, optional): The JSON data to send in the request body for 'POST' requests. Defaults to {}.
        token_type (str, optional): The type of authentication token to include in the request header. Defaults to "Bearer ".

    Returns:
        requests.Response: The response object returned by the API.

    Raises:
        requests.exceptions.HTTPError: If the API request returns an HTTP error status code.
        requests.exceptions.RequestException: If there is an error sending the API request.
        Exception: If an unexpected error occurs.

    """
    # Set the header based upon the REST API verb 
    # Use 'application/x-gzip' for PUT requests to upload a compressed file or 'application/octet-stream' for an uncompressed file
    if verb == 'PUT':    
        get_headers = {
            'Content-Type': 'application/x-gzip' if compress_upload_chunks else 'application/octet-stream',
            'Accept': '*/*',
            'Authorization': token_type + globals.Auth.access_token
        }
    else: 
        get_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': token_type + globals.Auth.access_token
        }

    # Select operation based upon the the verb
    try:
        match verb:
            case 'GET':
                res = requests.get(uri, headers=get_headers)
            case 'POST':
                res = requests.post(uri, headers=get_headers, json=body)
            case 'PUT':
                res = requests.put(uri, headers=get_headers, data=data)
            case 'DELETE':
                res = requests.delete(uri, headers=get_headers)
            case 'PATCH':
                res = requests.patch(uri, headers=get_headers)
        
        res.raise_for_status()

        return res

    except requests.exceptions.HTTPError as err:
        print(
            f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text}')
        logger.error(
            f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text}')
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logger.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


# === Fetch File ID ===
def fetch_file_id(**kwargs):
    """
    Fetches the ID of a file in Anaplan based on the provided parameters.

    Args:
        **kwargs: Keyword arguments containing the following parameters:
            - file_to_upload (str): The path of the file to upload.
            - base_uri (str): The base URI of the Anaplan API.
            - workspace_id (str): The ID of the Anaplan workspace.
            - model_id (str): The ID of the Anaplan model.

    Returns:
        str: The ID of the file in Anaplan.

    Raises:
        Exception: If the file is not found and cannot be created.

    """
    try:
        # If import_data_source is provided then set as file name
        if kwargs.get("import_data_source"):
            file_name = kwargs["import_data_source"]
        else:
            # Isolate file name
            file_name = os.path.basename(kwargs["file_to_upload"])
            
        logger.info(f"File name to search for: {file_name}")
        print(f"File name to search for: {file_name}")

        # Get file ID from existing file in the Anaplan model
        file_id = get_file_id(file_name, **kwargs)
        if file_id:
            logger.info(f"File ID found: {file_id}")
            print(f"File ID found: {file_id}")
            # If a match is found, return the ID
            return file_id
        else:
             # If no match is found, create a new file (import data source) and return the ID
            file_id = create_import_data_source(file_name, **kwargs)
            logger.info(f"File ID created: {file_id}")
            print(f"File ID created: {file_id}")
            return file_id

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")
        raise


# === Get File ID ===
def get_file_id(file_name, **kwargs):
    """
    Get the ID of a file in Anaplan based on its name.

    Args:
        file_name (str): The name of the file.
        **kwargs: Additional keyword arguments containing the base URI, workspace ID, and model ID.

    Returns:
        str or None: The ID of the file if found, None otherwise.
    """
    # Get a list of files
    uri = f'{kwargs["base_uri"]}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files'
    res = anaplan_api(uri=uri, verb="GET")

    # Isolate the nested_results
    files = json.loads(res.text)['files']

    # See if filename matches and ID and return the ID
    # Iterate through each file in the "files" list of the JSON data
    for file in files:
        if file['name'] == file_name:
            return file['id']
    return None


# === Create Import Data Source in Anaplan ===
def create_import_data_source(file_name, **kwargs):
    """
    Creates an import data source in Anaplan.

    Args:
        file_name (str): The name of the file to be created.
        **kwargs: Additional keyword arguments containing the base URI, workspace ID, and model ID.

    Returns:
        str: The ID of the created file.
    """
    uri = f'{kwargs["base_uri"]}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{file_name}'
    res = anaplan_api(uri, verb="POST", body={"chunkCount": 0})
    return json.loads(res.text)['file']['id']


# === Set Chunk Count ===
def set_chunk_count(chunk_count, file_id, **kwargs):
    """
    Set the chunk count for a file in Anaplan.

    Parameters:
    - chunk_count (int): The number of chunks to divide the file into.
    - file_id (str): The ID of the file in Anaplan.
    - **kwargs: Additional keyword arguments containing the base URI, workspace ID, and model ID.

    Returns:
    None
    """
    # Set count
    uri = f'{kwargs["base_uri"]}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{file_id}'
    anaplan_api(uri=uri, verb="POST", body={'chunkCount': chunk_count})
    logger.info(f'Chunk count set to {chunk_count} for file ID {file_id}.')
    print(f'Chunk count set to {chunk_count} for file ID {file_id}.')


# === Upload Chunk === 
def upload_chunk(file_path, file_id, chunk_num, **kwargs):
    """
    Uploads a single chunk to an API.

    Parameters:
    file_path (str): The path of the file to be uploaded.
    file_id (str): The ID of the file.
    chunk_num (int): The number of the chunk being uploaded.
    **kwargs: Additional keyword arguments containing the base URI, workspace ID, and model ID.

    Returns:
    None
    """

    # Read in file and PUT to endpoint 
    with open(file_path, 'rb') as file:

        # Read in file content
        file_content = file.read()

        logger.info(f'Uploading chunk {chunk_num} of file ID {file_id}.')
        print(f'Uploading chunk {chunk_num} of file ID {file_id}.')

        # Set URI
        uri = f'{kwargs["base_uri"]}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{file_id}/chunks/{chunk_num}'
        
        # PUT to endpoint
        anaplan_api(uri=uri, verb="PUT", data=file_content, compress_upload_chunks=kwargs["compress_upload_chunks"])


#def upload_all_chunks(directory_path, max_workers=5, **kwargs):
def upload_all_chunks(**kwargs):
    """
    Uploads all chunks in the specified directory to an API using multiple threads.

    Parameters:
    - kwargs (dict): Keyword arguments containing the necessary information for uploading chunks.
        - chunk_files (list): List of file paths for each chunk.
        - max_workers (int): Maximum number of worker threads to use.
        - Other optional arguments specific to the upload process.

    Returns:
    - None

    Raises:
    - Any exceptions that occur during the upload process.

    """
    # Get File ID
    file_id = fetch_file_id(**kwargs)

    # Set Chunk Count
    chunk_count = len(kwargs["chunk_files"])
    set_chunk_count(chunk_count, file_id, **kwargs)

    with ThreadPoolExecutor(max_workers=kwargs["max_workers"]) as executor:
     
        # Use enumerate to get the index (chunk_id) and file_path for each file
        futures = [executor.submit(upload_chunk, file_path, file_id, chunk_id, **kwargs) 
                for chunk_id, file_path in enumerate(kwargs["chunk_files"])]
        
        # Wait for all futures to complete and potentially collect results
        for future in futures:
            result = future.result()  # This blocks until the future is completed
        

