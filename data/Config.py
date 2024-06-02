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

import json
import logging
from typing import Union


class Config:

    def __init__(self, json_obj: dict):
        self.json = json_obj

    @staticmethod
    def load(file):
        """
        Loads a config from file.
        :param file: the file.
        :return: the config.
        """
        json_obj = json.load(file)
        logging.debug(f"loaded config from file {file}.")
        return Config(json_obj=json_obj)

    def save(self, file, indent=4, ensure_ascii=True):
        """
        Saves the config to file.
        :param file: destination file.
        :param indent: for readability, default: 4.
        :param ensure_ascii: Should we assure ascii encoding? default: true
        :return: Nothing.
        """
        json.dump(self.json, file, indent=indent, ensure_ascii=ensure_ascii)
        logging.debug(f"saved config to file {file}.")

    def __navigate_json(self, key: Union[list[str], str]):
        if isinstance(key, str):
            return self.json
        else:
            json_obj = self.json
            for k in key[:-1]:  # do not navigate last key to make set function possible
                json_obj = json_obj[k]
        return json_obj

    def set(self, key: Union[list[str], str], value):
        json_navigated = self.__navigate_json(key)
        if isinstance(key, str):
            old_value = json_navigated[key]
            json_navigated[key] = value
        else:
            old_value = json_navigated[key[-1]]
            json_navigated[key[-1]] = value
        logging.debug(f"changed config value {key} from {old_value} to {value}")

    def get(self, key: Union[list[str], str]):
        json_navigated = self.__navigate_json(key)
        if isinstance(key, str):
            return json_navigated
        else:
            return json_navigated[key[-1]]


def dump(config: Config, file, indent=4):
    """
    Dumps the config in the given file.
    :param config: the config.
    :param file: the file.
    :param indent: for readability, default: 4.
    :return: Nothing.
    """
    config.save(file, indent=indent)
