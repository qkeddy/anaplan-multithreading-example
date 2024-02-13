# ===============================================================================
# Created:        2 Feb 2023
# Updated:        30 Jan 2024
# @author:        Quinlan Eddy
# Description:    Main module for invocation of Anaplan operations
# ===============================================================================

import shutil
import os
import gzip
import logging


# Enable logger
logger = logging.getLogger(__name__)

def copy_file_multiple_times(file, count):
    """
    Copy a file multiple times.

    Args:
        file (str): The path of the file to be copied.
        count (int): The number of times to copy the file.

    Returns:
        list: A list of paths of the created files.
    """
    created_files = []

    # Split the file path into directory, file name, and extension
    directory, file_name = os.path.split(file)
    file_base_name, file_extension = os.path.splitext(file_name)

    # Copy the file multiple times
    for i in range(1, count + 1):
        # Generate new file name with suffix
        new_file_name = f"{file_base_name}_{i:03d}{file_extension}"
        new_file_path = os.path.join(directory, new_file_name)

        # Copy the file
        shutil.copyfile(file, new_file_path)
        logger.info(f"Copied to: {new_file_path}")
        print(f"Copied to: {new_file_path}")

        # Add the new file path to the list
        created_files.append(new_file_path)

    return created_files



def delete_files(file_paths):
    """
    Deletes the files specified by the given file paths.

    Args:
        file_paths (list): A list of file paths to be deleted.

    Returns:
        None
    """
    for file in file_paths:
        try:
            os.remove(file)
            logger.info(f"Deleted: {file}")
            print(f"Deleted: {file}")
        except OSError as e:
            logger.error(f"Error: {e.strerror}, while deleting file {file}")
            print(f"Error: {e.strerror}, while deleting file {file}")



def write_chunked_files(file, chunk_size_mb=1, compression=True):
    """
    Write a large file in chunks.

    Args:
        file (str): The path of the file to be written in chunks.
        chunk_size_mb (int): The size of each chunk in megabytes.
        compression (bool): Flag to toggle GZip compression on or off.

    Returns:
        list: A list of paths of the created chunk files.
    """
    # Approximate number of characters per MB (assuming 1 char = 1 byte)
    chars_per_mb = 1024 * 1024

    # Split the file path into directory, file name, and extension
    directory, file_name = os.path.split(file)
    file_base_name, file_extension = os.path.splitext(file_name)

    # Initialize counters
    current_size = 0
    max_size = chunk_size_mb * chars_per_mb
    chunk_number = 1
    chunk_files = []  # Initialize an empty list to store the file paths

    # Open the input file
    with open(file, 'r', encoding='utf-8') as file:
        while True:
            # Create a new file for each chunk
            if compression:
                chunk_file_name = f"{file_base_name}_chunk_{chunk_number:03d}{file_extension}.gz"
            else:
                chunk_file_name = f"{file_base_name}_chunk_{chunk_number:03d}{file_extension}"
            chunk_file_path = os.path.join(directory, chunk_file_name)

            # Add fully qualified file to chunk_files array
            chunk_files.append(chunk_file_path)

            # Open the chunk file in gzip format
            if compression:
                open_func = gzip.open
            else:
                open_func = open

            # Open the chunk file in gzip format
            with open_func(chunk_file_path, 'wt', encoding='utf-8') as chunk_file:
                # Read through the file line by line and write to the chunk file
                for line in file:
                    line_size = len(line.encode('utf-8'))
                    
                    # Check if adding this line would exceed the size limit
                    if current_size + line_size > max_size:
                        chunk_number += 1
                        break

                    # Write the line to the chunk file
                    chunk_file.write(line)
                    current_size += line_size
                else:
                    # End of file reached
                    break

                # Reset the current size for the next chunk
                current_size = 0

            # Write message
            logger.info(f"Chunk written to {chunk_file_path}")
            print(f"Chunk written to {chunk_file_path}")

    # Write final message
    logger.info(f"Chunking complete. Total chunks: {chunk_number}")
    print(f"Chunk written to {chunk_file_path}")
   
    # Return the last chunk number as the number of chunks
    return chunk_files