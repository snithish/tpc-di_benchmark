import json

import xmltodict


def main():
    with open("/home/airflow/gcs/data/CustomerMgmt.xml", "r") as file, open("/home/airflow/gcs/data/CustomerMgmt.json",
                                                                            "w+") as destination:
        xmltodict.parse(file.read(), item_depth=2, item_callback=append_as_json(destination), attr_prefix="attr_")


def append_as_json(file_handle):
    def add_to_file(path, item):
        print(path)
        action_element = path[1]
        action_attributes = action_element[1]
        item["Customer"]["ActionType"] = action_attributes["ActionType"]
        item["Customer"]["ActionTS"] = action_attributes["ActionTS"]
        file_handle.write(json.dumps(item) + "\n")
        return True

    return add_to_file


if __name__ == '__main__':
    main()
