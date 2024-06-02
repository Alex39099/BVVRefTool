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
from datetime import datetime

import numpy as np
import pandas as pd

from data.BVVScalper_py import BVVScalper
from data.Config import Config
from data.DataContainer_py import DataContainer
from data.helpfunctions import replace_umlaute


class RegistrationContainer(DataContainer):

    @classmethod
    def get_subfolder_name(cls):
        return "courses"

    @classmethod
    def get_data_csv_name(cls):
        return "registrations_data"

    @classmethod
    def get_data_types(cls):
        return {
            "course_id": str,  # must be provided by courseContainer
            "course_label": str,
            "last_name": str,  # e notation
            "first_name": str,  # e notation
            "birthday": datetime,
            "registration_status": str,  # approved, cancelled, waiting
            "participation_status": str,  # passed, failed, missed, pending
            "waiting_position": int,
            "confirmation_status": str,  # confirmed, denied, pending
            "status": str,  # added, changed, unchanged, removed (must be set to changed by user in order to detect confirmation_status changes)
        }

    def _get_data_defaults(self):
        return {
            "confirmation_status": "pending",
            "status": "unchanged"
        }

    def validate_data(self, df=None):
        df = super().validate_data(df)
        # df = replace_umlaute(df, columns=["last_name", "first_name"])
        if "confirmation_status" in df.columns:
            mask = ~df["confirmation_status"].isin(["confirmed", "denied", "pending"])
            df.loc[mask, "confirmation_status"] = "pending"

        logging.debug("finished validating of data.")
        return df

    def __init__(self, config: Config, scalper: BVVScalper):
        super().__init__(config, scalper)
        self.last_update_return = None
        return

    def save(self):
        self.data = self.data[self.data["status"] != "removed"]
        self.data.loc[:, "status"] = self._get_data_defaults()["status"]
        self.data.loc[~self.data["confirmation_status"].isin(["confirmed", "denied", "pending"]), "confirmation_status"] = "pending"

        # sort data
        self.data = self.data.sort_values(by=["course_id", "course_label", "registration_status", "waiting_position", "last_name", "first_name"])
        return super().save()

    def insert_course_id(self, courses):
        """
        Inserts the course_ids into the loaded registrations df. Deletes entries where no course is found.
        :param courses: the courses provided by courseContainer (updated!).
        :return: Nothing.
        """
        assert self.data is not None

        size_before = len(self.data)

        # drop every course column except of label and id
        courses = courses[["id", "label"]]
        # rename according to registrations' naming conventions
        courses = courses.rename(columns={"id": "course_id", "label": "course_label"})

        # merge on course_label/label, delete every registration where no course is found
        merged_df = self.data.merge(right=courses, how='inner', on="course_label", suffixes=("", "_right"))

        # replace old course_id with new one
        merged_df = merged_df.drop(columns=["course_id"])
        merged_df = merged_df.rename(columns={"course_id_right": "course_id"})

        if size_before != len(merged_df):
            logging.info("removed registrations when inserting course data.")

        self.data = merged_df

        logging.info("inserted course_id into registrations.")
        return

    def update(self, df=None):
        """
        Updates the currently loaded data_df with the provided df. This may remove rows from data_df but no columns.
        :param df: the df with column label, last_name, first_name and birthday. Default: scalped data
        :return: newly_added, registration_changed, participation_changed
        """
        assert self.data is not None
        keys = ['course_label', 'last_name', 'first_name', 'birthday']

        if df is None or df.empty:
            df = self.scalper.get_registrations()

        df = self.validate_data(df)

        # update using all columns from df except keys
        columns = [col for col in df.columns if col not in keys]

        # Ensure only columns present in data_df are updated
        columns_to_update = [col for col in columns if col in self.data.columns]

        # maybe have more registration for one combination of keys (e.g. cancelled, re-registered). This results in duplicates when merging on those keys.
        # Solution: We need to merge only on not already known cancelled registrations
        old_not_cancelled_registrations = self.data.loc[self.data["registration_status"] != "cancelled"]

        # need to remove as many exact cancelled registrations as in self.data (this removes already known cancelled registrations, but not new ones)
        old_cancelled_registrations = self.data.loc[self.data["registration_status"] == "cancelled"].copy()
        old_counts = old_cancelled_registrations[df.columns.tolist()].value_counts().to_dict()
        for row, count in old_counts.items():
            for _ in range(count):
                # Find the index to be removed
                idx = df[(df == row).all(axis=1)].index
                if idx.empty:
                    break
                df = df.drop(idx[0])

        # Merge with indicator
        merged_df = pd.merge(old_not_cancelled_registrations, df[keys + columns_to_update],
                             on=keys, how='outer', suffixes=('', '_update'), indicator=True)

        # Detect which rows had changes in participation_status/registration_status or got newly added
        changed_condition = (merged_df["_merge"] == "both") & ((merged_df["participation_status"] != merged_df["participation_status_update"])
                                                               | (merged_df["registration_status"] != merged_df["registration_status_update"])
                                                               | (merged_df["status"] == "changed"))

        # Update the specified columns if the value in df is not null, for existing columns only
        for column in columns_to_update:
            condition = ~merged_df[column + '_update'].isna()
            merged_df.loc[condition, column] = merged_df.loc[condition, column + '_update']

        # Drop the temporary update columns, table already sorted by keys
        merged_df.drop(columns=[col + '_update' for col in columns], inplace=True)

        # Flag old_cancelled_registrations as both to determine newly_added_condition correctly
        if len(old_cancelled_registrations) != 0:
            common_rows = merged_df.merge(old_cancelled_registrations, on=old_cancelled_registrations.columns.tolist(), how="inner")
            merged_df['_merge'] = merged_df.apply(lambda row: 'both' if ((row[old_cancelled_registrations.columns.tolist()] == common_rows[old_cancelled_registrations.columns.tolist()]).all(axis=1)).any() else row['_merge'], axis=1)
        newly_added_condition = (merged_df['_merge'] == 'right_only')

        to_be_removed_condition = (merged_df["_merge"] == "left_only")

        # Set confirmation_status for any newly added registrations
        merged_df.loc[newly_added_condition & (merged_df["participation_status"] == "pending"), "confirmation_status"] = "pending"

        merged_df = merged_df.drop(columns=['_merge'])

        merged_df["status"] = "unchanged"
        merged_df.loc[changed_condition, "status"] = "changed"
        merged_df.loc[newly_added_condition, "status"] = "added"
        merged_df.loc[to_be_removed_condition, "status"] = "removed"

        # add old registrations again
        old_cancelled_registrations["status"] = "unchanged"
        merged_df = pd.concat([merged_df, old_cancelled_registrations])

        self.data = merged_df
        return

    def _get_by_status(self, status):
        return self.data[self.data["status"] == status]

    def get_unchanged(self):
        return self._get_by_status("unchanged")

    def get_changed(self, include_added=False):
        if include_added:
            return self.data[self.data["status"].isin(["added", "changed"])]
        return self._get_by_status("changed")

    def get_added(self):
        return self._get_by_status("added")

    def get_removed(self):
        return self._get_by_status("removed")
