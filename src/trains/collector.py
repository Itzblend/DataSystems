import requests
from typing import List
import json
import os
from datetime import datetime


def _get_trains() -> dict:
    url = "https://rata.digitraffic.fi/api/v1/trains"
    trains_json = requests.get(url).json()
    for row in trains_json:
        # Add current time to each row
        row["fetchedAt"] = str(datetime.now())
    return trains_json


def write_dict_list_to_file(
    dict_list: List[dict], file_path: str, nd: bool = False
) -> None:

    dir_tree_list = [row["departureDate"].split("-") for row in dict_list]
    dir_tree_list = [
        list(x) for x in set(tuple(x) for x in dir_tree_list)
    ]  # Deduplicate
    for dir_tree in dir_tree_list:
        os.makedirs(os.path.join(file_path, *dir_tree), exist_ok=True)

    if nd:
        for d in dict_list:
            dir_tree = d["departureDate"].split("-")
            with open(
                os.path.join(file_path, *dir_tree, str(d["trainNumber"]) + ".json"), "w"
            ) as f:
                json.dump(d, f)
    else:
        raise NotImplementedError
