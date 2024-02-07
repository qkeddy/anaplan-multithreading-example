# ===============================================================================
# Created:        2 Feb 2023
# Updated:        30 Jan 2024
# @author:        Quinlan Eddy
# Description:    A test module to get Workspaces using a loop to test multithreading
# ===============================================================================


import logging
import requests
from concurrent.futures import ThreadPoolExecutor
import globals
import sys
import os
import json


# === Interface with Anaplan REST API   ===
def anaplan_api(uri, verb, data=None, body={}, token_type="Bearer "):

    # Set the header based upon the REST API verb    
    if verb == 'PUT':
        get_headers = {
            'Content-Type': 'application/octet-stream',
            'Accept': 'application/json',
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
        logging.error(
            f'{err} in function "{sys._getframe().f_code.co_name}" with the following details: {err.response.text}')
        sys.exit(1)
    except requests.exceptions.RequestException as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)
    except Exception as err:
        print(f'{err} in function "{sys._getframe().f_code.co_name}"')
        logging.error(f'{err} in function "{sys._getframe().f_code.co_name}"')
        sys.exit(1)


def fetch_file_id(**kwargs):

    # Isolate file name
    file_name = os.path.basename(kwargs["file_to_upload"])

    # Get a list of files
    uri = f'{kwargs["base_uri"]}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files'
    res = anaplan_api(uri=uri, verb="GET") 

    # Isolate the nested_results
    nested_results = json.loads(res.text)['files']


    # See if filename matches and ID and return the ID
    # Iterate through each file in the "files" list of the JSON data
    for file in nested_results:
        # Check if the current file's name matches the target file name
        if file['name'] == file_name:
            # If a match is found, return the file's ID
            return file.get['id']
    
    # If no match is found, create a new file and return the ID
    uri = f'{kwargs["base_uri"]}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{file_name}'
    res = anaplan_api(uri, verb="POST", body={"chunkCount": 0}) 

    # Return the ID of the new file created
    return json.loads(res.text)['file']['id']



def set_chunk_count(chunk_count, file_id, **kwargs):
    # Set count
    uri = f'{kwargs["base_uri"]}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{file_id}'
    print(uri)
    anaplan_api(uri=uri, verb="POST", body={'chunkCount': chunk_count})



def upload_chunk(file_path, **kwargs):
    """
    Uploads a single chunk to an API.
    """
    # Implement the upload logic here
    print(f'Uploading {file_path}...')  

    # Simulate upload delay
    uri = f'{kwargs["base_uri"]}/workspaces/{kwargs["workspace_id"]}/models/{kwargs["model_id"]}/files/{kwargs["file_id"]}'
    
    anaplan_api(uri=uri, verb="POST", body={'chunkCount': kwargs["chunk_count"]})
    print(f'Finished uploading {file_path}.')



#def upload_all_chunks(directory_path, max_workers=5, **kwargs):
def upload_all_chunks(**kwargs):
    """
    Uploads all chunks in the specified directory to an API using multiple threads.
    """
    # chunk_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]

    # chunk_files = []  # Initialize an empty list to store the file paths
    # # Iterate over each item in the directory
    # for f in os.listdir(directory_path):
    #     full_path = os.path.join(directory_path, f)  # Construct the full path
    #     # Check if the path is a file, not a directory
    #     if os.path.isfile(full_path):
    #         chunk_files.append(full_path)  # Add the path to the list
    
    # Get File ID

    # Set Chunk Count
    chunk_count = len(kwargs["chunk_files"])

    # Set File ID
    file_id = fetch_file_id(**kwargs)
    set_chunk_count(chunk_count, file_id, **kwargs)

    # with ThreadPoolExecutor(max_workers=kwargs["max_workers"]) as executor:
    #     futures = [executor.submit(upload_chunk, file_path, **kwargs) for file_path in kwargs["chunk_files"]]
    #     for future in futures:
    #         future.result()  # Wait for all uploads to complete

