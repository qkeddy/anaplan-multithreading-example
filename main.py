# ===============================================================================
# Created:        2 Feb 2023
# Updated:        30 Jan 2024
# @author:        Quinlan Eddy
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import sys
import logging
import utils

import anaplan_oauth
import globals
import anaplan_ops
import file_ops

def main():
	
	# Clear the console
	utils.clear_console()

	# Enable logging
	logger = logging.getLogger(__name__)

	# Get configurations & set variables
	settings = utils.read_configuration_settings()
	verbose_endpoint_logging = settings["verboseEndpointLogging"]
	oauth_service_uri = settings["uris"]["oauthService"]
	integration_api_uri = settings["uris"]["integrationApi"]
	thread_count = settings["threadCount"]
	compress_upload_chunks = settings["compressUploadChunks"]
	upload_chunk_size_mb = settings["uploadChunkSizeMb"]
	delete_upload_chunks = settings["deleteUploadChunks"]
	database = settings["database"]
	rotatable_token = settings["rotatableToken"]
	workspace_id = settings["workspaceId"]
	model_id = settings["modelId"]

	# Get configurations from the CLI
	args = utils.read_cli_arguments()
	register = args.register

	# Set the client_id and token_ttl from the CLI arguments
	globals.Auth.client_id = args.client_id
	if args.token_ttl == "":
		globals.Auth.token_ttl = int(args.token_ttl)

	# If register flag is set, then request the user to authenticate with Anaplan to create device code
	if register:
		logger.info(f'Registering the device with Client ID: {globals.Auth.client_id}')
		anaplan_oauth.get_device_id(uri=f'{oauth_service_uri}/device/code')
		anaplan_oauth.get_tokens(uri=f'{oauth_service_uri}/token', database=database)
		
	else:
		print('Skipping device registration and refreshing the access_token')
		logger.info('Skipping device registration and refreshing the access_token')
		anaplan_oauth.refresh_tokens(uri=f'{oauth_service_uri}/token', database=database, delay=0, rotatable_token=rotatable_token)

	# Start a tread to refresh the token at intervals specified by the `delay` parameter
	refresh_token = anaplan_oauth.refresh_token_thread(1, name="Refresh Token", delay=2000, uri=f'{oauth_service_uri}/token', database=database, rotatable_token=settings["rotatableToken"])
	refresh_token.start()

	# Set File to upload and import data source
	file_to_upload = args.file_to_upload
	import_data_source = args.import_data_source
	
	# Chunk files
	chunk_files = file_ops.write_chunked_files(file=file_to_upload, chunk_size_mb=upload_chunk_size_mb, compress_upload_chunks=compress_upload_chunks)

	# Upload files to Anaplan
	anaplan_ops.upload_all_chunks(file_to_upload=file_to_upload, import_data_source=import_data_source, chunk_files=chunk_files, compress_upload_chunks=compress_upload_chunks, max_workers=thread_count, verbose_endpoint_logging = verbose_endpoint_logging, base_uri=integration_api_uri, workspace_id=workspace_id, model_id=model_id)  

	# Delete temporary files
	if delete_upload_chunks:
		file_ops.delete_files(chunk_files)

	print('Process complete. Exiting...')
	logger.info('Process complete. Exiting...')

	# Exit with return code 0
	sys.exit(0)


if __name__ == '__main__':
    main()
