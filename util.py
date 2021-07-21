import gzip
import json


def read_dicts_from_zip(file_name):
    """
    Helper function to read from the zipped files.
    """
    with gzip.open(file_name, "r") as f:
        json_bytes = f.read()

    json_str = json_bytes.decode("utf-8")
    return json.loads(json_str)