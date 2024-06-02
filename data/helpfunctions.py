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


def replace_umlaute(df, columns=None, inplace=False):
    """
    Replaces all German Umlaute using the e notation. Also sharp S is changed to ss.
    :param df: the dataframe on which the operation is performed.
    :param columns: the columns on which the operation should be performed. Default is all columns.
    :param inplace: If True, performs operation inplace and returns None.
    :return: Object after replacement.
    """
    replacements = {
        'ä': 'ae',
        'ö': 'oe',
        'ü': 'ue',
        'Ä': 'Ae',
        'Ö': 'Oe',
        'Ü': 'Ue',
        'ß': 'ss'
    }

    if columns is not None:
        if inplace:
            gf = df
        else:
            gf = df.copy()
        gf[columns] = gf[columns].replace(replacements, regex=True)
        return gf

    return df.replace(replacements, regex=True, inplace=inplace)


def remove_duplicates(df, subset=None, save_path_duplicates=None):
    """
    Removes duplicates from dataframe based on completeness.
    :param df: the dataframe.
    :param subset: set of columns present in df used for identifying duplicates. Default: ["last_name", "first_name", "birthday"]
    :param save_path_duplicates: save path of csv file for all detected duplicates.
    :return: df without duplicates that had the most NA values.
    """
    gf = df.copy()

    if subset is None:
        subset = ["last_name", "first_name", "birthday"]

    ascending = [True] * len(subset) + [False]

    gf["non_null_count"] = gf.apply(lambda row: row.count(), axis=1)  # count non-null elements in temporary column
    gf_sorted = gf.sort_values(by=subset + ["non_null_count"], ascending=ascending)  # sort within same name by non-null count

    # save duplicates into file
    if save_path_duplicates is not None:
        gf_duplicates = gf_sorted[gf_sorted.duplicated(subset=subset, keep=False)]
        if not gf_duplicates.empty:
            gf_duplicates.to_csv(save_path_duplicates, index=False)
            logging.info(f"removed duplicate persons but created a backup: {save_path_duplicates}")

    # drop duplicates but keep most complete row for each person, remove tempo column
    gf_unique = gf_sorted.drop_duplicates(subset=subset, keep="first").drop(columns=["non_null_count"])
    return gf_unique
