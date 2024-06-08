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
from typing import Union

import pandas as pd

from data.BVVScalper_py import BVVScalper
from data.Config import Config
from data.DataContainer_py import DataContainer
from datetime import datetime


class CourseContainer(DataContainer):

    @classmethod
    def get_subfolder_name(cls):
        return "courses"

    @classmethod
    def get_data_csv_name(cls):
        return "courses_data"

    @classmethod
    def get_data_types(cls):
        # deep data must be counted below!
        return {
            "id": str,
            "district": str,
            "label": str,
            "type": str,  # training, refresher
            "date_start": datetime,
            "date_end": datetime,
            "license_category": str,  # Halle, Beach
            "license_type": str,  # A, AK, B, BK, C, "CP", "CT", D
            "registration_start": datetime,
            "registration_end": datetime,
            "reregistration_end": datetime,  # deep
            "deregistration_end": datetime,  # deep
            "free_space": int,
            "granted_space": int,
            "waiting_count": int,
            "city": str,
            "address": str,  # deep
            "remark": str,  # deep
            "contact_name": str,  # deep
            "contact_mail": str,  # deep
            "contact_phone": str,  # deep
            "contact_mobile": str,  # deep
            "management_reminder_count": int,  # number of times we have sent reminder mails to management for this course
            "player_reminder_count": int,  # number of times we have sent reminder mails to each to this course registered player
        }

    def _get_data_defaults(self):
        return {
            "management_reminder_count": 0,
            "player_reminder_count": 0
        }

    _DEEP_DATA_COUNT = 8
    _SUBFOLDER_BACKUP_NAME = "backup"

    def get_backup_dir(self):
        directory = os.path.join(self.get_dir(), CourseContainer._SUBFOLDER_BACKUP_NAME)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def __init__(self, config: Config, scalper: BVVScalper):
        super().__init__(config, scalper)
        return

    def save(self):
        self.data = self.data.sort_values(by=["district", "type", "license_category", "license_type", "registration_end"], ascending=[True, True, True, True, True])
        return super().save()

    def assert_deep_data(self, lids: Union[list[str], str]):
        if isinstance(lids, str):
            lids = [lids]

        lids = set(lids)
        logging.info(f"asserting deep data for course_ids {lids}...")

        # select the rows of interest
        selected_courses = self.data[self.data["id"].isin(lids)]

        # select any row with no deep data at all (to prevent constant updating when not every deep data is filled by BVV)
        selected_courses_na = selected_courses[selected_courses.isna().sum(axis=1) >= self._DEEP_DATA_COUNT]

        if len(selected_courses_na) > 0:
            lids = selected_courses_na["id"].tolist()
            res = self.update(self.scalper.get_deep_course_info(lids))
            logging.info(f"updated deep data for course_ids {lids}")
            return res

        logging.info(f"deep data was already available for {lids}")
        return None

    def update(self, df=None, columns=None):
        """
        Updates the currently loaded data_df with the provided df. This may also add courses/rows but no columns.
        :param df: df with column id as well as all data columns that should be overwritten. Default: scalping data
        :param columns: the data columns on which data gets changed. Default: "df.columns" (without id)
        :return: dataframe (copy) of newly added courses sorted by registration_start
        """
        keys = ["id"]

        if df is None or df.empty:
            df = self.scalper.get_courses()

        if "id" not in df.columns:
            raise ValueError("Data provided to update course_df had no id column.")

        df = self.validate_data(df)

        # If no columns specified, update using all columns from df except keys
        if columns is None:
            columns = [col for col in df.columns if col not in keys]

        # Ensure only columns present in data_df are updated
        columns_to_update = [col for col in columns if col in self.data.columns]

        # Merge with indicator to see which rows are present in both DataFrames (_merge column with left_only, right_only, both)
        merged_df = pd.merge(self.data, df[keys + columns_to_update],
                             on=keys, how='outer', suffixes=('', '_update'), indicator=True)

        # Update the specified columns if the value in df is not null, for existing columns only
        for column in columns_to_update:
            condition = ~merged_df[column + '_update'].isna()
            merged_df.loc[condition, column] = merged_df.loc[condition, column + '_update']

        # Drop the temporary update columns and the _merge indicator, sort the table for readability
        merged_df.drop(columns=[col + '_update' for col in columns], inplace=True)

        # Insert default values for newly added rows, provided data will not get overwritten
        mask = merged_df['_merge'] == 'right_only'
        subset = merged_df[mask]
        data_defaults = self._get_data_defaults()
        filled_subset = subset.fillna(data_defaults)
        merged_df.loc[mask] = filled_subset

        result = merged_df[(merged_df['_merge'] == 'right_only')].copy()

        merged_df.drop(columns=["_merge"], inplace=True)
        merged_df.sort_values(by=["district", "type", "license_category", "license_type", "registration_end"],
                              ascending=[True, True, True, True, False], inplace=True)

        self.data = merged_df

        # Prepare result
        result.drop(columns=["_merge"], inplace=True)
        result.sort_values(by=["registration_start"], ascending=False, inplace=True)
        new_ids = [i for i in result["id"]].sort()
        logging.info(f"updating courses_df resulted in new courses. Added ids: {new_ids}")
        return result
