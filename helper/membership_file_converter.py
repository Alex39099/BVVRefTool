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

import pandas as pd

def read_club_membership_file(filepath, date_format, name_converter):
    """
    Reads the club_membership_file at the given path.
    :param filepath: the file path to the club_membership_file (excel)
    :param date_format: the date_format in club_membership_file
    :param name_converter: name converter {"last_name, first_name": "new_last_name, new_first_name"}
    :return: df
    """
    columns_naming = {
        "Nachname": "last_name",
        "Vorname": "first_name",
        "GebDatum": "birthday",
        "Strasse": "street",
        "Plz": "postalcode",
        "Ort": "city",
        "Handy": "mobile",
        "EMail": "mail",
        "Eintritt": "club_membership_enter",
        "Austritt": "club_membership_expire"
    }

    keep_everything_str = {key: str for key in columns_naming.keys()}

    df = pd.read_excel(filepath, sheet_name=0, usecols=columns_naming.keys(), converters=keep_everything_str)
    df = df.rename(columns=columns_naming)

    # parse datetime
    date_columns = ["birthday", "club_membership_enter", "club_membership_expire"]
    for date_column in date_columns:
        df[date_column] = pd.to_datetime(df[date_column], format=date_format)

    # convert names
    for index, row in df.iterrows():
        combined_name = f"{row['last_name']}, {row['first_name']}"
        if combined_name in name_converter:
            new_last_name, new_first_name = name_converter[combined_name].split(',')
            df.at[index, 'last_name'] = new_last_name.strip()
            df.at[index, 'first_name'] = new_first_name.strip()
            logging.debug(f"NAME_CONVERTER: converted {combined_name} to {new_last_name}, {new_first_name} while reading club_membership_file.")

    return df
