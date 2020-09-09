import subprocess
from subprocess import CalledProcessError
import json
import pandas as pd
from dateutil import parser
import csv
import glob
import logging


def find_latest_directory(directories):
    """
    Return the latest dates-version given a list of directories
    :param directories: A list of directories
    :return: Latest dates-version
    """
    dates_versions = []
    for directory in directories:
        if not directory:
            continue

        version_index = directory.find("-v")
        if version_index == -1:
            logging.error(f'Cannot find version in {directory}')
            continue
        version = int(directory[version_index+2:-1])

        try:
            date = parser.parse(directory[:version_index], fuzzy=True)
        except ValueError:
            logging.error(f'Cannot parse date from directory: {directory}')

        dates_versions.append((date, version))

    return max(dates_versions)


def extract(check_latest=True):
    """
    Extract all the grouped states endpoint
    :return: Else return a list containing all the grouped states csv endpoint
             Return None if an error occurs with retrieving data from endpoint
    """
    with open("../config.json") as config:
        config_dict = json.load(config)
        directory = config_dict["visit_data_endpoint"]

    # Alert the user if the endpoint cannot be found
    try:
        # Obtain all files within the directory endpoint
        directory_ls = subprocess.check_output(["gsutil", "ls", directory])

        # Find latest date-version, terminate if the latest one is already fetched
        directories = directory_ls.decode("utf-8").split("\n")
        print("Finding latest date-version...")
        latest_date, latest_version = find_latest_directory(directories)
        print(f'Latest date-version: {latest_date.strftime("%Y%m%d")}-{latest_version}')

        if check_latest and latest_date < parser.parse(config_dict["latest_date"]) and \
                latest_version <= config_dict["latest_version"]:
            logging.error("The latest date-version already exists. Terminating.")
            return

        latest_directory = f'{directory}{latest_date.strftime("%Y%m%d")}-v{latest_version}/'
        latest_directory_ls = subprocess.check_output(["gsutil", "ls", latest_directory])
        latest_files_list = latest_directory_ls.decode("utf-8").split("\n")

        grouped_states = []
        for file in latest_files_list:
            if "grouped" in file:
                grouped_states.append(file)

        # Update config
        config_dict["latest_date"] = latest_date.strftime("%Y%m%d")
        config_dict["latest_version"] = latest_version
        with open("../config.json", "w") as config:
            json.dump(config_dict, config, indent=2)

        print("Extraction success")
        return grouped_states

    except CalledProcessError:
        logging.error(f"Cannot find endpoint {directory}")
        return


def download(grouped_states):
    """
    Download all the csv into local directory
    :param grouped_states: List of states endpoints
    :return: None

    Pros: Don't have to keep everything in memory before processing
    Cons: Takes a while to download everything
    """
    with open("../config.json") as config:
        config_dict = json.load(config)

    # Save all the csv into the directory specified in config
    for state in grouped_states:
        subprocess.check_output(["gsutil", "cp", state, config_dict["grouped_states_path"]])


def in_memory(grouped_states):
    """
    Load all the grouped states data in memory
    :param grouped_states: List containing endpoints to grouped states
    :return: A list containing each csv in a list format

    Pros: Much faster to process
    Cons: Might explode if memory gets too large
    """
    states_data = []
    for state in grouped_states:
        byte_string = subprocess.check_output(["gsutil", "cat", state])
        string_list = byte_string.decode("utf-8").split("\n")

        state_data = []
        for string in string_list:
            # Ignore list containing empty string, which happens in the end
            if len(string) == 0:
                continue

            # Delimit on commas but ignore commas within quotes
            row = ['{}'.format(x) for x in list(csv.reader([string], delimiter=',', quotechar='"'))[0]]
            state_data.append(row)

        states_data.append(state_data)

    return states_data


def transform(states_data=None):
    """
    Combine all group states data into one dataframe and save it to disk as csv
    :param states_data: List of states data in memory, None if using downloaded csv
    :return: Concatenated dataframe of all grouped states
    """
    with open("../config.json") as config:
        config_dict = json.load(config)

    file_name = f'visit_data_{config_dict["latest_date"]}-{config_dict["latest_version"]}'

    # In-memory
    if states_data:
        dfs = []
        for state in states_data:
            state_df = pd.DataFrame(state[1:], columns=state[0])
            dfs.append(state_df)

        transformed_df = pd.concat(dfs, ignore_index=True)
        transformed_df.to_csv(file_name + "_in_memory")

        return transformed_df
    # From download
    else:
        csv_files = sorted(glob.glob(f'{config_dict["grouped_states_path"]}*'))

        dfs = []
        for state in csv_files:
            dfs.append(pd.read_csv(state))

        transformed_df = pd.concat(dfs, ignore_index=True)
        transformed_df.to_csv(file_name + "_downloaded")

        return transformed_df

