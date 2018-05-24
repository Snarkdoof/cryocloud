

ccmodule = {
    "description": "Create Wind <something> based on a product",
    "depends": ["product"],
    "provides": ["SarWind"],
    "inputs": {
        "src": "Full path to input file",
        "configOverride": "Override of configuration parameters (as a map)"
    },
    "outputs": {
        "product": "The productID of the processed product",
        "result": "'ok' or 'error: ...'",
        "dst": "Destination path of the resulting wind output"
    },
    "defaults": {
        "priority": 50,  # Normal
        "runOn": "success"
    }
}


def process_task(self, task, stop_event):
    print("Process wind here...")
    retval = {
        "product": "fakeproduct",
        "result": "ok",
        "dst": "/tempdir/fakeproduct.nc"
    }
    return 100, retval
