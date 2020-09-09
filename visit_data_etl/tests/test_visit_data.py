import json
from etl import visit_data
from dateutil import parser
import pandas as pd
import copy


class TestClass:
    def test_extract_correct_grouped_states(self):
        with open("config.json") as config:
            config_dict = json.load(config)

        group_states_list = set(config_dict["group_states_list"])

        extracted_states = visit_data.extract(check_latest=False)

        for state in extracted_states:
            assert(state[state.index("grouped"):-4] in group_states_list)

    def test_download_and_in_memory_consistency(self):
        extracted_states = visit_data.extract(check_latest=False)

        visit_data.download(extracted_states)
        states_data = visit_data.in_memory(extracted_states)

        in_memory_df = visit_data.transform(states_data)
        download_df = visit_data.transform()

        pd.testing.assert_frame_equal(in_memory_df, download_df)

    def test_terminate_if_latest_exists(self):
        assert(visit_data.extract() is None)

    def test_invalid_endpoint(self):
        with open("config.json") as config:
            config_dict = json.load(config)

        temp_dict = copy.deepcopy(config_dict)
        temp_dict["visit_data_endpoint"] = "gs://data.visitdata.org/processed/vendor/threesquare/asof/"

        with open("config.json", "w") as config:
            json.dump(temp_dict, config, indent=2)

        assert(visit_data.extract() is None)

        with open("config.json", "w") as config:
            json.dump(config_dict, config, indent=2)

    def test_obtain_latest_date_version(self):
        directories = [
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200902-v0/",
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200902-v1/",
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200901-v0/",
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200830-v0/",
        ]

        latest_date, latest_version = visit_data.find_latest_directory(directories)

        assert(latest_date == parser.parse("20200902"))
        assert(latest_version == 1)

    def test_obtain_latest_date_version_with_one_invalid_date(self):
        directories = [
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200902-v0/",
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200902-v1/",
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/-v0/",
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200830-v0/",
        ]

        latest_date, latest_version = visit_data.find_latest_directory(directories)

        assert(latest_date == parser.parse("20200902"))
        assert(latest_version == 1)

    def test_obtain_latest_date_version_with_missing_version(self):
        directories = [
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200902-v0/",
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200902-v1/",
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200901/",
            "gs://data.visitdata.org/processed/vendor/foursquare/asof/20200830/",
        ]

        latest_date, latest_version = visit_data.find_latest_directory(directories)

        assert(latest_date == parser.parse("20200902"))
        assert(latest_version == 1)