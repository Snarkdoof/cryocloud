

## CryoCloud Workflows

CryoCloud provides a graph workflow abstraction where a graph can be defined
in JSON. A workflow describes how processing is performed, and input nodes
generate "pebbles" of information that "trickles" through the graph. As such,
a single job (for example a satellite product) is discovered, the
corresponding pebble is generated and passed through the graph. Multiple
pebbles can be run through a graph at the same time, and these are kept fully
separated.


### Key concepts

* Workflow is a graph describing a workflow. When "executing" a workflow, a "pebbles" is created by input modules (e.g. dirwatchers), and these are then passed through the graph.

* Nodes: Define nodes for the graph. Edges are defined using "downstreamOf"

* Edges: downstreamOf specifies which nodes are parents of a given node. This can be done implicitly by defining nodes as "children" instead of as separate nodes.

* args: Argument mapping to the input arguments of a node. These can be strings, config, output from other nodes in the graph, or options given on the command line. This is where a lot of the funky stuff happens, as one can map for example the output filename of one module to the input of another etc.

* options: A workflow can define options - these will become available as commandline parameters for ccworkflow

* Special argument options: 
  * "default": set default value if the given one can't be resolved
  *  "type": "file" - if the file is remote it will be copied, if it's compressed it will be uncompressed


### Workflow definition file (JSON)

Workflow definitions must be contained within { "workflow": {<put it here>}}

Requires "name" and "description", and must have some modules. The "entry" point is implicitily defined.


```
{
  "workflow": {
    "name": "Simple test",
    "description": "This is a simple test workflow",
    "nodes": [
      {
        "module": "dirwatcher",
        "name": "Input",
        "downstreamOf": ["entry"],
        "args": {
            "src": "/tmp/inputdir"
        }
      },
      {
        "module": "noop",
        "name": "FakePrepare",
        "args": {
          "time": "3"
        },
        "downstreamOf": ["Input"]
      },
      {
        "module": "noop",
        "name": "FakeDoStuff",
        "args": {
          "time": {"stat": "parent.runtime"}
        },
        "downstreamOf": ["FakePrepare"]
      }
    ]
  }
}
```

### Module definition:
  "module": "NAME OF MODULE" - this needs a ccmodule definition in the file (look below on "writing CryoCloud modules")

  "name": "Unique name for this workflow" - Use for readability and to get access to return values for later modules

  "config": "The config root for any configs used in this definition (only needed if 'config' is used in arguments)

  "runOn": "success/error/timeout/always/never" - when resolved, the parent either succeeded, failed or timed out. Limit when this module runs (can be set default by module)

  "resolveOnAny": "Resolve when ALL parents have completed (default) or when ANY parent has completed",
  
  "type": "normal/admin" - type of CryoCloud worker that should take this job

  "ccnode": "If given, run on the given CryoCloud processing node - or use runtime stats (stat) or config, typically {"stat": "parent.node"}

  "downstreamOf": [parent1, parent2] - These are the edges of the graph, this module processes AFTER some other node. Refers to the unique name

  "children" [module definitions] - Inline definition of children - same as giving this module a name and set "downstreamOf" to this modules name

  "args": {argument definition} - How to resolve input arguments for this module (see below)


### Argument definition:

"args": {argumentname: value} where argument is the name of the input argument, value can be one of:

  * a string - is delivered as is, example {"ignoreerrors": "true"}
  * Output: another module's return value: {"output": "modulename.returnvaluename"}, e.g.: {"src": {"output": "MyInput.product"}} to use the "product" return value from the "MyInput" module as "src" input argument
  * Config a config parameter, "blocksize": {"config": "blocksize"}. This requires "config" to be set in the module definition (e.g. "config": "Cryonite.Readers.Sentinel")
  * Stat: Runtime stats, possible stats for now are: "node", "worker", "priority", "runtime", "cpu_time", "max_memory". Difficult to see why you would use this for now.

  * you can also specify that it's a file, allowing the worker to automatically copy/unzip etc. Example:
     "args": {"src": {"output": "parent.dst", "type": "file", "proto": "http", unzip": "true", "copy": "true"}}

     In this the "copy" is default true, proto is default "ssh" and unzip is default false 
     The output "/tempdisk/foo/bar" would in this case be rewritten to "http://[ip of node that executed the parent]/tempdisk/foo/bar"


## Writing a CryoCloud module

When writing a CryoCloud module, a ccmodule definition must be written in your
file. It should provide both documentation of the module and describe the
inputs, outputs, processing defaults and inform about which status parameters
it makes available. This both allows others to use your module more easily and
it allows CryoCloud to check graphs for inconsistencies.

* Depends: [list of strings] - This module must be connected to parents that provide all items in the list
* Provides: [list of strings] - This module provides all these items (convention should be used for which strings)
* inputs: {} - The input arguments of this module in the form "argname": "human readable description"
* outputs: {} - The output arguments of this module - the return value should be a map with these keys and values. Format is "argname":"human readable description"
* defaults: {} - Processing defaults. Can be ["priority", "runOn", "resolveOnAny", "type"],
* status: {} - CryoCloud status parameters provided in the form "name": "human readable description". 

#### Example
```
ccmodule = {
    "description": "Create SAR Wind based on a product from L1 GRD",
    "depends": ["Product"],
    "provides": ["SarWind"],
    "inputs": {
        "src": "Product",
        "configOverride": "Override of configuration parameters (as a map)",
        "program": "IDL Program to run",
        "configFile": "Config file for IDL program"
    },
    "outputs": {
        "product": "The productID of the processed product",
        "datafile": "The datafile used for processing",
        "result": "'ok' or 'error: ...'",
        "dst": "Destination path of the resulting wind output"
    },
    "defaults": {
        "priority": 50,  # Normal
        "runOn": "success"
    },
    "status": {
        "progress": "Progress 0-100%",
        "state": "Current state of processing"
    }
}
```

The module must also contain the method "process_task", where cancel_event is
optional. Cancel_event will be set if the job should be aborted, so if your
module has a loop, use `while cancel_event.isSet():` rather than `while True`.

Following is an example for simple processing wind using IDL

```
def process_task(worker, task, cancel_event=None):
    args = task["args"]
    if "src" not in args:
        raise Exception("Can't create wind without data (src argument missing)")

    if "program" not in args:
        raise Exception("Need a program to run (program argument missing)")

    if "configFile" not in args:
        raise Exception("Need config file")

    retval = {
        "datafile": datafile,
        "product": _get_product(os.path.split(datafile)[1])
    }

    if "configOverride" in args:
        idlcmd = os.path.basename(args["program"][:-4] + ", '%s', '%s', configoverride = '%s'" %
                                  (datafile, args["configFile"], args["configOverride"]))
    else:
        idlcmd = os.path.basename(args["program"])[:-4] + ", '%s', '%s'" % (datafile, args["configFile"])

    cmd = ["-IDL_STARTUP", "' '", "-IDL_PATH", "'<IDL_DEFAULT>'", "-e", idlcmd]

    task["args"]["fullPath"] = datafile
    task["args"]["cmd"] = cmd

    p, ret = idl.process_task(worker, task, cancel_event=cancel_event)
    for r in ret:
        retval[r] = ret[r]

    return p, ret


```
