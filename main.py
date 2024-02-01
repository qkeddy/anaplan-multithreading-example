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
import threading


def main():
	
	# Clear the console
	utils.clear_console()

	# Enable logging
	logger = logging.getLogger(__name__)

	# Get configurations & set variables
	settings = utils.read_configuration_settings()
	oauth_service_uri = settings["uris"]["oauthService"]
	database = settings["database"]
	rotatable_token = settings["rotatableToken"]

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
	refresh_token = anaplan_oauth.refresh_token_thread(1, name="Refresh Token", delay=5, uri=f'{oauth_service_uri}/token', database=database, rotatable_token=settings["rotatableToken"])
	refresh_token.start()

	# Start multiple threads to get workspaces in parallel
	# Start new Threads
	for i in range(5):
		get_workspaces = anaplan_ops.get_workspaces_thread(i, name="Get Workspaces", counter=3, delay=10)
		get_workspaces.start()

	# Exit with return code 0
	sys.exit(0)


if __name__ == '__main__':
    main()
