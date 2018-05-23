

ccmodule = {
    "description": "Report stuff",
    "depends": [],
    "provides": [],
    "inputs": {
        "src": "Full path to input file that is reported on",
        "product": "Product ID of the product",
        "result": "The end result"
    },
    "outputs": {
    },
    "defaults": {
        "priority": 0,  # Bulk
        "runOn": "always"
    }
}


def process_task(self, task):
    """
    """
    print("REPORT", task["args"])

    return 100, None
