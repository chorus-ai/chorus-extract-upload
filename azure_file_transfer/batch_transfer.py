import subprocess
import os
import sys
import shutil
import platform
from datetime import datetime, timedelta

def check_az_command():
    # Check if the 'az' command exists in the system's PATH
    if shutil.which('az') is None:
        print("Error: The 'az' command is not found. Please install Azure CLI before running this script.")
        sys.exit(1)

def set_environment_variable():
    # Set AZURE_CLI_DISABLE_CONNECTION_VERIFICATION to 1
    os.environ['AZURE_CLI_DISABLE_CONNECTION_VERIFICATION'] = '1'
    print("AZURE_CLI_DISABLE_CONNECTION_VERIFICATION set to 1.")

def run_az_login():
    try:
        # Run the 'az login' command using subprocess
        subprocess.run(['az', 'login'], check=True)
        print("Azure login successful.")
    except subprocess.CalledProcessError as e:
        # Handle errors from the 'az login' command
        print(f"Azure login failed. Error: {e}")
        sys.exit(1)

def check_source_dir_structure(source_dir):
    # List subdirectories under source_dir
    subdirs = [x[0] for x in os.walk(source_dir)]

    # Check if there is a directory with numbers and if it contains the required subdirectories
    for subdir in subdirs:
        if os.path.basename(subdir).isdigit():
            sub_subdirs = [d for d in os.listdir(subdir) if os.path.isdir(os.path.join(subdir, d))]
            if set(['Waveforms', 'OMOP', 'Images']).issubset(sub_subdirs):
                return True

    # Print the actual directory structure and exit with an error message
    print(f"Error: The source directory structure is not as expected. It should contain a directory with numbers in the name and subdirectories 'Waveforms', 'OMOP', 'Images'.")
    print(f"Actual directory structure:")
    for subdir in subdirs:
        print(f"- {os.path.basename(subdir)}")
    sys.exit(1)


def upload_blob_batch(storage_account_name, sas_token, container_name, source_dir, if_unmodified_since=None):
    try:
        # Build the 'az storage blob upload-batch' command
        command = [
            'az', 'storage', 'blob', 'upload-batch',
            '--destination', container_name,
            '--account-name', storage_account_name,
            '--source', source_dir,
            '--sas-token', sas_token,
            '--output', 'json'
        ]

        # Add --if-unmodified-since if provided
        if if_unmodified_since:
            command.extend(['--if-unmodified-since', if_unmodified_since])

        # Run the Azure CLI command
        subprocess.run(command, check=True)

    except subprocess.CalledProcessError as e:
        # Handle any exceptions and print the error message
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Get user input for storage_account_name, container_name, source_dir, and sas_token
    storage_account_name = input("Enter the Azure Account name (Azure blob storage bucket): ")
    container_name = input("Enter the container name: ")

    # Check if the 'az' command exists
    check_az_command()

    # Set the environment variable AZURE_CLI_DISABLE_CONNECTION_VERIFICATION to 1
    set_environment_variable()

    # Run 'az login' and check for success
    run_az_login()

    # Check if the line is present in the hosts file
    check_hosts_file()

    # Determine the appropriate path separator based on the operating system
    if sys.platform.startswith('win'):
        source_dir = input("Enter the source directory (Windows format, e.g., C:\\path\\to\\source): ")
    else:
        source_dir = input("Enter the source directory (Unix format, e.g., /path/to/source): ")

    sas_token = input("Enter the SAS token: ")

    # Prompt user for if_unmodified_since
    if_unmodified_since_input = input("Do you want to set 'if_unmodified_since'? (Y/N) az-cli ignores this flag as of Jan 2024: ")
    if_unmodified_since = None

    if if_unmodified_since_input.upper() == 'Y':
        # Get today's date and subtract one day
        yesterday = datetime.now() - timedelta(days=1)

        # Format the date as a string in the required format
        if_unmodified_since = yesterday.strftime('%Y-%m-%dT%H:%M:%SZ')

    # Check if the source directory structure is as expected
    check_source_dir_structure(source_dir)

    # Set output to json 
    output ="json"

    # Call the upload_blob_batch function with user-provided and default parameters
    upload_blob_batch(storage_account_name, sas_token, container_name, source_dir, if_unmodified_since)

