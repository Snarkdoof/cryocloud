
ccmodule = {
    "description": "Log error to local file",
    "depends": [],
    "provides": [],
    "inputs": {
        "src": "Full path to input file that is reported on",
        "product": "Product ID of the product",
        "result": "The end result"
    },
    "outputs": {},
    "defaults": {
        "priority": 0,  # Bulk
        "runOn": "error"
    }
}


def process_task(self, task):
    """
    """
    print("Log error file", task["args"])

    return 100, None
