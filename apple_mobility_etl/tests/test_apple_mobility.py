import apple_mobility
import json
import copy
import pandas as pd
import os.path


class TestClass:
    def test_valid_extract_september_2(self):
        with open("config.json") as config:
            config_dict = json.load(config)

        temp_config = copy.deepcopy(config_dict)
        temp_config["csv_source_link"] = "https://covid19-static.cdn-apple.com/covid19-mobility-data/" \
                                         "2016HotfixDev5/v3/en-us/applemobilitytrends-"

        with open("config.json", "w") as config:
            json.dump(temp_config, config, indent=2)

        source_csv, latest_source_link = apple_mobility.extract_source("2020-09-02")

        assert (source_csv is not None)
        assert (latest_source_link is not None)
        pd.testing.assert_frame_equal(source_csv, pd.read_csv(config_dict["test_extracted_csv"],
                                                              low_memory=False,
                                                              index_col=0))

        with open("config.json", "w") as config:
            json.dump(config_dict, config, indent=2)

    def test_extract_using_xpath(self):
        source_csv, latest_source_link = apple_mobility.extract_source("2020-08-32")
        assert(source_csv is not None)
        assert(latest_source_link is not None)

    def test_transform_extracted(self):
        with open("config.json") as config:
            config_dict = json.load(config)

        extracted_csv = pd.read_csv(config_dict["test_extracted_csv"],
                                    low_memory=False,
                                    index_col=0)

        transformed_csv = pd.read_csv(config_dict["test_transformed_csv"],
                                      low_memory=False,
                                      index_col=0)

        pd.testing.assert_frame_equal(apple_mobility.transform(extracted_csv),
                                      transformed_csv)

    def test_extract_transform_save(self):
        with open("config.json") as config:
            config_dict = json.load(config)

        test_date = "2020-09-02"
        temp_dict = copy.deepcopy(config_dict)
        temp_dict["save_path"] = "tests/"
        temp_dict["latest_source_link"] = ""

        with open("config.json", "w") as config:
            json.dump(temp_dict, config, indent=2)

        apple_mobility.extract_transform_save(test_date)

        with open("config.json", "w") as config:
            json.dump(config_dict, config, indent=2)

        print(os.getcwd())
        assert(os.path.exists("tests/apple_mobility_2020-09-02.csv"))

        transformed_csv = pd.read_csv(config_dict["test_transformed_csv"],
                                      low_memory=False,
                                      index_col=0)
        new_csv = pd.read_csv("tests/apple_mobility_2020-09-02.csv",
                              low_memory=False,
                              index_col=0)

        pd.testing.assert_frame_equal(transformed_csv, new_csv)

    def test_extra_columns_in_csv(self):
        with open("config.json") as config:
            config_dict = json.load(config)

        extra_columns = ["extra"]
        static_non_date_columns = config_dict["static_non_date_columns"]

        assert(apple_mobility.check_column_names(static_non_date_columns + extra_columns) is False)

    def test_missing_columns_in_csv(self):
        with open("config.json") as config:
            config_dict = json.load(config)

        static_non_date_columns = config_dict["static_non_date_columns"]

        assert(apple_mobility.check_column_names(static_non_date_columns[:-1]) is False)
