from dateutil.parser import parse
import pandas as pd
import json
from urllib.error import HTTPError
import datetime
from dateutil import parser

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    try:
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False


def extract_source(date):
    """
    Return source csv using given date
    If csv cannot be found by using given date, then use selenium for retrieval
    :param date: Date to extract from source
    :return: Raw pandas DataFrame of source csv and the source csv link used
    """
    with open("config.json") as config:
        config_dict = json.load(config)

    csv_source_link = config_dict["csv_source_link"] + f"{date}.csv"
    source_link = config_dict["source_link"]
    csv_xpath = config_dict["csv_xpath"]

    try:
        source_csv = pd.read_csv(csv_source_link, low_memory=False)
        print("Found source csv using csv source link in config...")
        return source_csv, csv_source_link
    except HTTPError:
        print(f"Cannot find csv with specified date: {date}")

    driver = webdriver.Safari()
    driver.get(source_link)
    try:
        csv_element = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, csv_xpath))
        )
        csv_url = csv_element.get_attribute("href")
        print(f'Source csv found using xpath: {csv_url}')
    finally:
        driver.quit()

    if csv_url is not None:
        print(f"Found source csv using xpath...")
        return pd.read_csv(csv_url, low_memory=False), csv_url

    print("Cannot find csv from source using xpath")
    return None


def transform(source_csv):
    """
    Transforms source csv by converting date columns into individual rows
    :return: Transformed pandas DataFrame
    """
    date_columns = [is_date(column) for column in source_csv.columns]
    non_date_columns = []
    new_columns = ["date", "value"]

    # Extract non_date columns from source csv
    for column, date in zip(source_csv.columns, date_columns):
        if not date:
            non_date_columns.append(column)

    # Terminates execution if there are differences in non date columns
    if not check_column_names(non_date_columns):
        return

    transformed_list = []

    print("Concatenating all csv into one...")
    # Iterate the DataFrame and add new rows per date column
    for series in source_csv.iterrows():
        cur_series = series[1]
        non_date_list = cur_series[non_date_columns].to_list()

        for date, value in cur_series[date_columns].items():
            new_row = non_date_list + [date, value]

            transformed_list.append(new_row)

    transformed_df = pd.DataFrame(transformed_list, columns=non_date_columns+new_columns)

    return transformed_df


def check_column_names(non_date_columns):
    """
    Check if all the non-date columns matches the current list of non-date columns
    If there's a difference, then alert the user
    :param non_date_columns: List of non date column names in the source csv
    :return: True if there are no errors, false otherwise with printouts on the error
    """
    with open("config.json") as config:
        static_non_date_columns = json.load(config)["static_non_date_columns"]

    set_static = set(static_non_date_columns)
    set_source = set(non_date_columns)

    if set_static == set_source:
        print("Config column names matches source column names...")
        return True

    missing_columns = set_static.difference(set_source)
    extra_columns = set_source.difference(set_static)

    if len(missing_columns) > 0:
        for col in missing_columns:
            print(f"\"{col}\" is missing from source csv.")
    if len(extra_columns) > 0:
        for col in extra_columns:
            print(f"\"{col}\" is extra in source csv.")

    return False


def extract_transform_save(date=None):
    """
    Extract csv from source link using current date
    If version does not match or if current date does not have updated csv,
    Use selenium to load web page and retrieve current csv link
    Save new csv if exists and valid
    :return: None
    """
    # Etl date specified, else use today's date
    if date is None:
        date = str(datetime.date.today())

    source = extract_source(date)

    if source is not None:
        source_csv, source_csv_link = source[0], source[1]

        # Check if extracted source link is updated
        # If it is the same, then skip processing
        with open("config.json", "r") as config:
            config_dict = json.load(config)
            latest_source_link = config_dict["latest_source_link"]

        if latest_source_link != source_csv_link:
            transformed_csv = transform(source_csv)

            if transformed_csv is not None:
                file_name = config_dict["save_path"]

                latest_date = parser.parse(source_csv_link.split("/")[-1], fuzzy=True)
                transformed_csv.to_csv(f"{file_name}/"
                                       f"apple_mobility_{latest_date.strftime('%Y-%m-%d')}.csv")

        # Update latest source link and csv source link
        with open("config.json", "w") as config:
            config_dict["latest_source_link"] = source_csv_link
            config_dict["csv_source_link"] = source_csv_link[:-14]  # Everything before the date section
            json.dump(config_dict, config, indent=2)
