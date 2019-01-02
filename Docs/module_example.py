ccmodule = {
    "description": "Increments a number",
    "depends": [],
    "provides": [],
    "inputs": {
        "number": "The number to be incremented"
    },
    "outputs": {
    },
    "defaults": {
        "priority": 10,  # Bulk
        "runOn": "always"
    },
    "status": {
        "progress": "Progress 0-100%"
    }
}


def process_task(cc, task):

    # parse args
    number = task["args"]["number"]

    # process
    number += 1

    # return result
    result = {'number': number}
    return 100, result