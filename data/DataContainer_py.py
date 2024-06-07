#  Copyright (c) 2024. Alexander Schmid
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation, either version 3 of the License, or
#      (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#
#      You should have received a copy of the GNU General Public License
#      along with this program.  If not, see <http://www.gnu.org/licenses/>.

import logging
import os
from datetime import date, datetime

import numpy as np
import pandas as pd

from data.BVVScalper_py import BVVScalper
from data.Config import Config


class DataContainer:

    @classmethod
    def get_data_csv_name(cls):
        raise NotImplementedError("data_csv_name must be specified.")

    @classmethod
    def get_data_types(cls):
        raise NotImplementedError("data_types must be specified.")

    @classmethod
    def get_subfolder_name(cls):
        return ""

    _config_main_folder_path = ["general", "main_folder_path"]

    def __init__(self, config: Config, scalper: BVVScalper):
        self.scalper = scalper
        self.config = config
        self.data = None
        self.__dir_path = None
        return

    def get_dir(self):
        if self.__dir_path is None:
            self.__dir_path = self.config.get(self._config_main_folder_path)
            return self.get_dir()
        if self.__dir_path == "":
            logging.warning(f"{self._config_main_folder_path} not specified in config. Using current working directory.")
            return os.getcwd()
        else:
            directory = self.__dir_path
            if not os.path.exists(directory):
                os.makedirs(directory)
                logging.info(f"created main directory {directory}")

            subfolder_name = self.get_subfolder_name()
            if subfolder_name != "":
                directory = os.path.join(directory, subfolder_name)
                if not os.path.exists(directory):
                    os.makedirs(directory)
            return directory

    def load(self, keep_n=2):
        """
        Loads the latest csv file into "self.data" with dtypes self.__DATA_TYPES and deletes the oldest files.
        :param keep_n: the number of latest files to keep
        :return: True if data was loaded from file, False otherwise (data is empty)
        """
        data_csv_name = self.get_data_csv_name()
        data_types = self.get_data_types()

        directory = self.get_dir()
        pattern = data_csv_name + "_"
        logging.info(f"starting to load {data_csv_name}...")

        # List all files in the directory that match the naming pattern
        files = [f for f in os.listdir(directory) if f.startswith(pattern) and f.endswith(".csv")]

        if len(files) == 0:
            logging.warning(f"could not find any files in {directory} matching pattern {pattern}.")
            self.data = pd.DataFrame(columns=data_types.keys())
            return

        # Sort the files by modification time, from oldest to newest
        files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)))

        # Identify the file(s) to keep
        files_to_keep = files[-keep_n:]

        # Delete all files except the n-th latest
        for file in files:
            if file not in files_to_keep:
                os.remove(os.path.join(directory, file))
                logging.info(f"deleted file f:{file}")

        # Load the latest CSV file
        latest_file_path = os.path.join(directory, files_to_keep[-1])  # The latest file

        date_columns = [column for column, dtype in data_types.items() if dtype == datetime]

        df = pd.read_csv(latest_file_path)  # may be modified by user!
        logging.info(f"loaded data from file {files_to_keep[-1]}")

        # parse dates manually because of date type
        for date_column in date_columns:
            df[date_column] = pd.to_datetime(df[date_column])

        # add all columns that are non-existent as nan columns
        df = df.reindex(columns=self.get_data_types().keys())
        # fill in default values
        df = df.fillna(self._get_data_defaults())

        self.data = self.validate_data(df)
        return True

    def save(self):
        """
        Saves data to csv_file. Only columns specified via self.get_data_types will be included.
        :return: Nothing.
        """

        directory = self.get_dir()
        timestamp = date.today()

        data_to_save = self.data.copy()

        expected_data_types = self.get_data_types()

        # only sort present columns, delete columns we do not want to save
        filtered_dict = {key: value for key, value in expected_data_types.items() if key in data_to_save.columns}
        data_to_save = data_to_save[list(filtered_dict.keys())]

        # Do desired/necessary conversions
        for column in data_to_save.columns:
            # Convert datetime columns to date

            if expected_data_types[column] is datetime:
                data_to_save[column] = pd.to_datetime(data_to_save[column]).dt.date

            # change possible linebreaks \n to \\n to preserve them!
            if filtered_dict[column] == str:
                data_to_save.loc[:, column] = data_to_save[column].apply(lambda x: x.replace('\n', '\\n') if isinstance(x, str) else x)

        # save data with timestamp
        save_name = self.get_data_csv_name() + f"_{timestamp}.csv"
        if len(data_to_save) > 0:
            data_to_save.to_csv(os.path.join(directory, save_name), index=False, encoding="utf_8_sig")
            logging.info(f"saved file {save_name}")
        else:
            logging.debug(f"skipped saving file {save_name} because data was empty.")
        return

    def _get_data_defaults(self):
        """
        Get default values for columns.
        :return: dict of default values, column = key. Not every column of get_data_types must be included.
        """
        return {}

    def validate_data(self, df=None):
        """
        Validates data and inserts default values if columns are created.
        :param df: df to be validated. Default: self.data.
        :return: validated df
        """
        own_data = False
        if df is None:
            own_data = True
            df = self.data

        logging.debug(f"starting to validate data of {self.__class__} (self_data = {own_data})...")

        data_types = self.get_data_types()

        # only convert present columns
        filtered_dict = {key: value for key, value in data_types.items() if key in df.columns}

        # Separate handling for specific dtypes
        for column, dtype in filtered_dict.items():
            if dtype == datetime:
                df[column] = pd.to_datetime(df[column], format="ISO8601")
            elif dtype == int or dtype == float:
                df[column] = pd.to_numeric(df[column], errors="coerce")
            elif dtype == bool:
                df[column] = df[column].apply(lambda x: True if x in ["True", "true"] else False).astype(bool)
            else:
                df[column] = df[column].astype(dtype)

        # prevent strings "nan"
        df = df.replace("nan", np.nan)
        df = df.replace("", np.nan)
        return df
