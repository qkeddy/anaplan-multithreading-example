# ===============================================================================
# Created:        2 Feb 2023
# Updated:        30 Jan 2024
# @author:        Quinlan Eddy
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import sys
import logging
import utils
import anaplan_auth_api
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
	authentication_uri = settings["uris"]["authenticationApi"]
	oauth_service_uri = settings["uris"]["oauthService"]
	integration_api_uri = settings["uris"]["integrationApi"]
	thread_count = settings["threadCount"]
	compress_upload_chunks = settings["compressUploadChunks"]
	upload_chunk_size_mb = settings["uploadChunkSizeMb"]
	delete_upload_chunks = settings["deleteUploadChunks"]
	database = settings["database"]
	rotatable_token = settings["rotatableToken"]
	access_token_ttl = settings["accessTokenTtl"]
	workspace_id = settings["workspaceId"]
	model_id = settings["modelId"]

	# Get configurations from the CLI
	args = utils.read_cli_arguments()
	register = args.register

	# Based on authentication mode access Anaplan via the authentication API or OAuth API
	if settings["authenticationMode"] == "OAuth":  # Use OAuth
		print("Authorization via OAuth API")

		# Set the client_id from the CLI arguments
		globals.Auth.client_id = args.client_id
		
		# If register flag is set, then request the user to authenticate with Anaplan to create device code
		if register:
			logger.info(f'Registering the device with Client ID: {globals.Auth.client_id}')
			anaplan_oauth.get_device_id(uri=f'{oauth_service_uri}/device/code')
			anaplan_oauth.get_tokens(uri=f'{oauth_service_uri}/token', database=database)
			
		else:
			print('Skipping device registration and refreshing the access_token')
			logger.info('Skipping device registration and refreshing the access_token')
			anaplan_oauth.refresh_tokens(uri=f'{oauth_service_uri}/token', database=database, delay=0, rotatable_token=rotatable_token)

		# Start a thread to refresh the token at intervals specified by the `delay` parameter
		refresh_token = anaplan_oauth.refresh_token_thread(1, name="Refresh Token", delay=access_token_ttl, uri=f'{oauth_service_uri}/token', database=database, rotatable_token=settings["rotatableToken"])
		refresh_token.start()
	else:
		if settings["authenticationMode"] == "basic":
			print("Using Basic Authentication")
			# Set variables
			anaplan_auth_api.basic_authentication(
				uri=f'{authentication_uri}/authenticate', username=args.user, password=args.password)
		elif settings["authenticationMode"] == "cert_auth":
			print("Using Certificate Authentication")
			anaplan_auth_api.cert_authentication(
				uri=f'{authentication_uri}/authenticate', public_cert_path=settings["publicCertPath"], private_key_path=settings["privateKeyPath"], private_key_passphrase=args.private_key_passphrase)
		else:
			print("Please update the `settings.json` file with an authentication mode of `basic`, `cert_auth`, or `OAuth`")
			logging.error(
				"Please update the `settings.json` file with an authentication mode of `basic`, `cert_auth`, or `OAuth`")
			sys.exit(1)

		# Start background thread to refresh the `access_token`
		refresh_token = anaplan_auth_api.refresh_token_thread(
			thread_id=1,
			name="Refresh Token",
			delay=access_token_ttl,
			uri=f'{authentication_uri}/refresh'
		)
		refresh_token.start()		

	# Set File to upload and import data source
	file_to_upload = args.file_to_upload
	import_data_source = args.import_data_source
	
	# Chunk files
	chunk_files = file_ops.write_chunked_files(file=file_to_upload, chunk_size_mb=upload_chunk_size_mb, compress_upload_chunks=compress_upload_chunks)

	# Upload files to Anaplan
	anaplan_ops.upload_all_chunks(file_to_upload=file_to_upload, import_data_source=import_data_source, chunk_files=chunk_files, compress_upload_chunks=compress_upload_chunks, max_workers=thread_count, verbose_endpoint_logging=verbose_endpoint_logging, base_uri=integration_api_uri, workspace_id=workspace_id, model_id=model_id)  

	# Delete temporary files
	if delete_upload_chunks:
		file_ops.delete_files(chunk_files)

	print('Process complete. Exiting...')
	logger.info('Process complete. Exiting...')

	# Exit with return code 0
	sys.exit(0)


if __name__ == '__main__':
	main()
