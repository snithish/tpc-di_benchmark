import json

import xmltodict


def handle(path, item):
    handle.counter += 1
    print('json: ', (json.dumps(item)), "\n")
    return handle.counter < 5


handle.counter = 0

if __name__ == '__main__':
    t = open('CustomerMgmt.xml')
    a = xmltodict.parse(t.read(), item_depth=2, item_callback=handle)
    print(a)
