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
from datetime import datetime, date
from typing import Union

import pandas as pd

from data.BVVScalper_py import BVVScalper
from data.Config import Config
from data.DataContainer_py import DataContainer
from data.helpfunctions import remove_duplicates


class PersonContainer(DataContainer):

    @classmethod
    def get_data_types(cls):
        return {
            "last_name": str,
            "first_name": str,
            "birthday": datetime,
            "sex": str,  # m, f
            "street": str,
            "postalcode": str,
            "city": str,
            "phone": str,
            "mobile": str,
            "mail": str,
            "license_category": str,
            "license_type": str,  # A, AK, B, BK, C, "CP", "CT", D, "DK"
            "license_id": str,
            "license_bvv_id": str,
            "license_since": datetime,
            "license_expire": datetime,
            "club": str,
            "club_membership_expire": datetime,
            "club_team": str,  # this should boost priority if team is in need of refs (H1-H4, D1-D4)
            "wants_higher_license": bool,
            "help_count": int,  # this directly boosts priority, set manually
            "failed_higher_license_count": int,  # this decreases priority
            # "receives_mail_notification": bool,  # remove? has no function atm
            # does this person want mail notifications. If not and added to course, info to SR management!
        }

    @classmethod
    def get_subfolder_name(cls):
        return "persons"

    @classmethod
    def get_data_csv_name(cls):
        return "persons_data"

    def validate_data(self, df=None):
        df = super().validate_data(df)
        # df = replace_umlaute(df, columns=["last_name", "first_name", "club"])
        df = remove_duplicates(df, save_path_duplicates=os.path.join(self.get_backup_dir(),
                                                                     "duplicate_persons_loading.csv"))
        logging.debug("finished validating of data.")
        return df

    _SUBFOLDER_BACKUP_NAME = "backup"

    _json_club_name = ["general", "club_name"]
    _json_club_membership_file_name = ["general", "club_membership_file_path"]

    def get_backup_dir(self):
        directory = os.path.join(self.get_dir(), PersonContainer._SUBFOLDER_BACKUP_NAME)
        if not os.path.exists(directory):
            os.makedirs(directory)
        return directory

    def __init__(self, config: Config, scalper: BVVScalper):
        super().__init__(config, scalper)
        self.clubname = config.get(self._json_club_name)
        return

    def _get_data_defaults(self):
        return {
            "license_category": "Halle",
            "license_type": "DK",
            "license_since": datetime.today(),
            "license_expire": datetime(year=datetime.today().year + 1, month=datetime.today().month,
                                       day=datetime.today().day),
            "club": self.clubname,
            "wants_higher_license": False,
            "help_count": 0,
            "failed_higher_license_count": 0,
            "receives_mail_notification": True,
        }

    def update(self, df=None, columns=None, keep_persons=True, save_deleted_entries=True):
        """
        Updates the currently loaded persons_df with the provided df. This may also add persons/row but not columns. Default values will get inserted if not specified within columns of df.
        :param df: df with columns last_name, first_name and birthday as well as all data columns that should overwrite a person's data. Default: scalp data
        :param columns: the data columns on which data gets changed.
        :param keep_persons: if false, every person not included in the provided df will get removed from persons_df.
        :param save_deleted_entries: if true, saves removed persons/entries in a temporary file.
        :return: Nothing.
        """

        keys = ['last_name', 'first_name', 'birthday']
        original_columns = self.data.columns

        if df is None:
            df = self.scalper.get_licenses()

        if df.empty:
            return

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

        if not keep_persons:
            # Keep only those rows that are found in df
            if save_deleted_entries:
                # backup deleted rows
                save_path = os.path.join(self.get_backup_dir(), "update_persons_df_deleted_entries.csv")
                backup = self.data[merged_df['_merge'] == 'left_only'].copy()
                if not backup.empty:
                    backup.sort_values(by=["last_name", "first_name"], ascending=True, inplace=True)
                    backup.to_csv(save_path, index=False, encoding="utf_8_sig")
                    logging.info(f"saved deleted persons ({len(backup)}) in file {save_path}")

            self.data = merged_df[merged_df['_merge'].isin(['both', 'right_only'])]
        else:
            self.data = merged_df

        # Insert default values for newly added rows, provided data will not get overwritten
        mask = merged_df['_merge'] == 'right_only'
        subset = self.data.loc[mask]
        data_defaults = self._get_data_defaults()
        filled_subset = subset.fillna(data_defaults)

        self.data.loc[mask] = filled_subset

        # Drop the temporary update columns and the _merge indicator, sort the table for readability
        self.data = self.data[original_columns].copy()
        self.data.sort_values(by=["last_name", "first_name"], ascending=True, inplace=True)

    def update_membership(self, df=None):
        """
        Updates the attribute CLUB_MEMBERSHIP_EXPIRE within the currently loaded persons_df with the provided df.
        :param df: None or df with columns last_name, first_name, birthday, club_membership_expire (may have null entries)
        :return:
        """
        df = df.copy()
        today = date.today()
        # Determine the current and previous timeframes based on today's date
        if today.month <= 6:
            # First half of the year
            current_timeframe_end = date(today.year, 7, 31)  # one month time to get new member list
            previous_timeframe_end = date(today.year - 1, 12, 31)
        else:
            # Second half of the year
            current_timeframe_end = date(today.year + 1, 1, 31)
            previous_timeframe_end = date(today.year, 6, 30)

        if df is not None:
            # ensure that df columns are datetime objects
            df["birthday"] = pd.to_datetime(df["birthday"])
            df["club_membership_expire"] = pd.to_datetime(df["club_membership_expire"])

        self.data["club_membership_expire"] = self.data.apply(update_club_membership_expire, axis=1, df=df,
                                                              current_timeframe_end=current_timeframe_end,
                                                              previous_timeframe_end=previous_timeframe_end)
        logging.info(
            f"updated club_membership_expire of all loaded persons (current_timeframe_end = {current_timeframe_end}, previous_timeframe_end = {previous_timeframe_end})")

    def get_club_members(self):
        return self.data[
            (self.data["club_membership_expire"] >= datetime.now()) | self.data["club_membership_expire"].isna()]

    def get_persons_by_license(self, license_category: str, license_type: Union[str, list[str]], only_club_members=True, max_expire_offset=None,
                               wants_higher_license: Union[None, True, False] = None, treat_expired_as_type_dk=True):
        """
        Get persons by license.
        :param license_category: license_category (Halle/Beach)
        :param license_type: license_type.
        :param only_club_members: only return club_members? default: true
        :param max_expire_offset: max offset for license_expire, past and future. default: only currently valid licenses
        :param wants_higher_license: only return people who want a higher license? default: false
        :param treat_expired_as_type_dk: Should we treat expired licenses as type DK?
        :return: all persons with the given filters applied.
        """
        if only_club_members:
            df = self.get_club_members()
        else:
            df = self.data

        df = df[df["license_category"] == license_category]

        # include expired licenses as DK if necessary
        if treat_expired_as_type_dk and license_type == "DK":
            expired_licenses = df[df["license_expire"] <= datetime.now()].copy()
            expired_licenses["license_expire"] = pd.NaT
            expired_licenses["license_type"] = "DK"
            expired_licenses["wants_higher_license"] = True
            df.update(expired_licenses)

        if isinstance(license_type, list):
            df = df[df["license_type"].isin(license_type)]
        else:
            df = df[df["license_type"] == license_type]

        time = datetime.now()
        if max_expire_offset is not None:
            df = df[df["license_expire"].isna() | ((df["license_expire"] >= time - max_expire_offset) & (df["license_expire"] <= time + max_expire_offset))]
        else:
            df = df[df["license_expire"].isna() | (df["license_expire"] >= time)]

        if wants_higher_license is not None:
            df = df[df["wants_higher_license"]]
        return df

    def increment_data_value(self, df):
        """
        Increments failed_higher_license_count by 1.
        :param df: df with columns last_name, first_name, birthday
        :return: nothing
        """
        if df is None or df.empty:
            return

        keys = ['last_name', 'first_name', 'birthday']
        df = df.drop_duplicates(subset=keys, ignore_index=True)

        data = self.data.set_index(keys)
        df = df.set_index(keys)

        data.loc[df.index, 'failed_higher_license_count'] += 1
        self.data = data.reset_index()


def update_club_membership_expire(row, df, current_timeframe_end, previous_timeframe_end):
    """
    Update the club_membership_expire date with pandas apply function.
    :param row: current row (needed for apply)
    :param df: None or list of members provided by the club containing attributes/columns LAST_NAME, FIRST_NAME, BIRTHDATE, CLUB_MEMBERSHIP_EXPIRE (may have null entries). Umlaute are expected to be transformed.
    :param current_timeframe_end: end of the current timeframe for membership cancellations
    :param previous_timeframe_end: end of the previous timeframe for membership cancellations
    :return: If person in df, then updated value (value in df or (if not present) current_timeframe_end). If person is not in df, already present value in dataframe or (if not present) previous_timeframe_end.
    """
    # df should be the list of members provided by the club.
    # Check if the person is in df

    name = row["last_name"] + ", " + row["first_name"]
    current_val = row["club_membership_expire"]

    if df is not None and ((df["last_name"] == row["last_name"]) &
                           (df["first_name"] == row["first_name"]) &
                           (df["birthday"] == row["birthday"])).any():

        # Person is in df, check for null club_membership_expire in df
        person_df = df[(df["last_name"] == row["last_name"]) &
                       (df["first_name"] == row["first_name"]) &
                       (df["birthday"] == row["birthday"])]

        if pd.isnull(person_df["club_membership_expire"].iloc[0]):
            # If club_membership_expire is null in df, set to end of current timeframe
            logging.debug(f"updated club_membership_expire of {name} "
                          f"from {current_val} to {current_timeframe_end} because there was no expiring date in club's member list.")
            return current_timeframe_end
        else:
            # If not null, update club_membership_expire
            new_val = person_df["club_membership_expire"].iloc[0]
            logging.debug(f"updated club_membership_expire of {name} "
                          f"from {current_val} to {new_val} (club member list data).")
            return person_df["club_membership_expire"].iloc[0]
    else:
        # Person is not in df, check if club_membership_expire should be updated
        if pd.isnull(row["club_membership_expire"]) or (row["club_membership_expire"] > previous_timeframe_end):
            logging.debug(
                f"updated club_membership_expire of {name} "
                f"from {current_val} to {previous_timeframe_end} because he/she was not included in the club's member list.")
            return previous_timeframe_end
        else:
            # If club_membership_expire is earlier, do nothing
            return row["club_membership_expire"]
