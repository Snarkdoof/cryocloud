

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

* **Nodes**: Define nodes for the graph. Edges are defined using "downstreamOf"

* **Edges**: downstreamOf specifies which nodes are parents of a given node. This can be done implicitly by defining nodes as "children" instead of as separate nodes.

* **args**: Argument mapping to the input arguments of a node. These can be strings, config, output from other nodes in the graph, or options given on the command line. This is where a lot of the funky stuff happens, as one can map for example the output filename of one module to the input of another etc.

* **options**: A workflow can define options - these will become available as commandline parameters for ccworkflow
    * Options are on the form {"doboogie": {"help": "Should we boogie?", "default": false, "type": "bool"}} where *help* is the help string for -h/--help, *default* is the default value and *type* is optional. If *type* is not "bool", it will not be a flag but require a value on the commandline, e.g. not "--doboogie", but "--someparam somevalue"
    **Types**
        * *bool* - Boolean (true if option is given)
        * *list* - Comma separated list
        * *json* - JSON data

* Special argument options:
    * *default*: set default value if the given one can't be resolved
    * *type*
        * *file* - if the file is remote it will be copied, if it's compressed it will be uncompressed
        * *tempdir* - create a temporary directory, specify *id* to refer to the same temporary directory multiple places (will be unique for each pebble). Automatically removed.


### File preparation
CryoCloud can do various file pre and post processing steps to ensure that modules do not need unneccesary complexity and can run on local data. In particular, Amazon S3 urls are supported as *s3://server/container/key*, as well as *ssh://server/fullpath*, although ssh requires keys to be pre-distributed and is as such not particularly flexible. CryoCloud can also upload returned files using *post* definition. Zipping files is currently not supported but might be added.

Files can also be unzipped automatically, converting a single filename to a list of files. Unzip handles both zip, tar and tar.gz/tgz files.

In short, the modules should normally only work on local files, not download things themselves, unzip or upload files, but this should be left to CryoCloud. Big IO jobs might be useful to use explicit download/unzip using the *unzip* module (possibly also using *type:admin*) to avoid hogging CPU's for IO intensive tasks and allow CryoCloud to manage these.


### Kubernetes and Docker integration
CryoCloud has two ways to use dockers. In both cases logs and status messages are handled as normal, and can be accessed using the normal CryoCore tools (cclog, ccstatus etc).

    1. Run fully contained dockers, typically through Kubernetes on a cluster or cloud solution. This is done by specifying *image* in the workflow and starting ccworkflow with --kubernetes. The image must have everything onboard, and S3 is suggested as transfer of data in and out of the container. Use "s3://server/bucket/key" as paths for input data, *post* definition for return data. The docker must have CryoCloud installed.

    2. Run local code in a docker environment. Typically useful for testing, where a docker is *not* fully contained but can provide the runtime environment for the module. The module as well as any input paths are mapped as volumes automatically. For proper deployments of finalized code, running as self contained dockers is likely better. the docker must have CryoCloud/dockercc installed.

As CryoCloud has a running overview of capabilities of workers, it will create Kubernetes deployments for all missing modules. These will currently be started to process **only** the particular module, even if the image supports multiple modules.

## Workflow definition file (JSON)

Workflow definitions must be contained within { "workflow": {<put it here>}}

Requires "name" and "description", and must have some modules. The "entry" point is implicitily defined.

The following workflow is a simple demonstration of 

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
        "module": "wind",
        "name": "Wind",
        "config": "CryoniteOcean.Wave",
        "args": {
          "src": {"output": "parent.fullpath", "type": "file", "unzip": "true"},
          "configOverride": "",
          "program": "wind.sav",
          "configFile": {"config": "ConfigFile"}
        },
        "downstreamOf": ["Input"]
      },
      {
      "module": "KSAT_report",
      "runOn": "success",
      "downstreamOf": ["Wind"],
      "args": {
        "product": {
          "output": "parent.product"
        }
      }
    }, {
      "module": "remove",
      "runOn": "always",
      "downstreamOf": ["Wind"],
      "args": {
        "src": {"output": "Input.fullpath"}
      }
    }
    ]
  }
}
```

### Module definition:
   **args**: *{argument definition}* - How to resolve input arguments for this module (see below)

  **ccnode**: If given, run on the given CryoCloud processing node - or use runtime stats (stat) or config, typically {"stat": "parent.node"}

  **children**: *[module definitions]* - Inline definition of children - same as giving this module a name and set "downstreamOf" to this modules name

  **config**: The config root for any configs used in this definition (only needed if 'config' is used in arguments)

  **depends**: *["Thing1", "Thing2"]* - Override the ccmodule's definition of dependencies. This is only used for validation, and must be matched with "provides" from upstream in the graph.

  **downstreamOf**: *[parent1, parent2]* - These are the edges of the graph, this module processes AFTER some other node. Refers to the unique name

  **deferred**: *true/false* - A module that is deferred will be queued immediately for processing, but not actually started until a pebble has completed. Particularly useful for cleanup tasks if not using tempdirs.
  
  **docker**: *docker image* - Docker image providing a core runtime environment for this module. Any referred paths will be mapped in as volumes, mirroring the external environment. Very useful to test run code inside a docker environent without deploying them. The docker images must be generated particularly for this, some examples should be in the *environments* repository on gitlab - basically they need the dockercc from CryoCore in the path. If you are looking for Kubernetes or cloud deployments, you likely want to look at *image*, not *docker*.

  **global**: *true/false* - Use in combination with *"runOn":"error"* for a global error handler. Any error will trigger this handler. Cleanup is likely better done using *tempdir* or *deferred* modules.

  **gpu**: true/false* - Does this module require a GPU to run? Default false. If true, tasks will *only* be processed by GPU workers. Try to keep GPU modules as tight as possible to avoid CPU intensive tasks blocking GPUs.

  **image**: *docker image* - Docker image that can be executed to handle jobs for this module. Particularly useful for Kubernetes integration.

  **loop**: *parameter name* - Loop over a single parameter. This is similar to splitOn, but loop doesn't split it into multiple jobs running in parallel, but will make a worker loop over a list of parameters sequentially. It's basically a way to handle a list of items without having a loop internally in the module. Doesn't need merging, end result is merged into a list, order is preserved.

  **max_parallel**: *number* - Limit the amount of jobs that is allowed to run in parallel. Useful for example to limit jobs for external services (e.g. downloads). 

  **merge**: *true* - On completion, this module will merge split jobs. Return values are transformed into lists. The lists are ordered just like the split input parameter. E.g. inputs ["in1", "in2", "in3"] will give return ["ret1", "ret2", "ret3"].

  **module**: *NAME OF MODULE* - this needs a ccmodule definition in the file (look below on "writing CryoCloud modules")

  **name**: *Unique name for this workflow* - Use for readability and to get access to return values for later modules

  **outputs**: *map of outputs* - as defined in the module definition. This overrides the module definition, and is mostly useful for input modules that take in anything. The *netwatcher* is an example here, where the given jsonschema restricts arguments, or a module that loads files and passes on information. Do not use for normal modules if at all possible (they should know what they provide).

  **post**: *[{'output': argumentname, 'target': prefix, 'basename':true/false}]* - Post processing specification. Highly useful for postprocessing returned files in contained modules. For example: *{"output": "dst", "target": "s3://server/container/", "basename": true}* will upload the file returned as 'dst' to the given S3 container. *target* is mapped like *args*, so for example *"target": {"options": "targetserver"}* allows a commandline parameter to speficy the final destionation of the workflow. If the return value is a list, it will be iterated over and all items in the list will be processed.

  **priority**: *number* - Override the default priority of the ccmodule. CryoCloud will choose higher priority jobs over lower. Useful to balance throughput vs latency of a workflow, limit disk usage, spread IO etc.

  **provides**: *["Thing1", "Thing3"]* - Override the ccmodule's definition of provides. Useful for example for generic input modules (such as cmdline)

  **resolveOnAny**: Resolve when ALL parents have completed (default) or when ANY parent has completed",

  **restrictions**: Map of restrictions for running. This can typically be limitations to avoid an unbalanced (or variable) workflow to clog up something. An example can be:
  ---
  "restrictions": {
    "CryoniteTidevann:geocode.pending": "<3",
    "NodeController.seer3:cache.free": ">536870912000"
  }
  ---
  These restrictions tries to keep the number of pending geocode jobs to 3 or less (it will queue another job if it's 2), and also wait to start processing until the free disk on the 'cache' disk on seer3 is at least 500GB. This of course means that a workflow can be blocked due to external factors. Such blocking might be printed by ccworkflow, but a top tip is to check that they restrictions are met if things aren't running.

  **runIf**: *if statement* - Python statement, e.g. *"output:SomeModule.someReturnValue!='SomeString'"*. The module will only run if the statement returns True. Useful to split the flow in one of multiple branches. Notice that if you want to check a boolean option, you must use *=='True'*, not *== True*, as the output will be a string regardless of type.

  **runOn**: *success/error/timeout/always/never* - when resolved, the parent either succeeded, failed or timed out. Limit when this module runs (can be set default by module)

  **runOnHead**: *true/false* - Should this module be executed on the head node. Notice that this is ONLY useful for tiny jobs, as the head is always a single machine, and not likely a processing machine at that. Useful for example for merging, rewriting arguments etc.

  **serviceURL**: *url* - The URL of a running microservice. The module definition is fetched from this URL on startup, so no stubs need to be available otherwise. As the definition is fetched and checked on startup, the microservice needs to be running at the time ccworkflow is started.

  **splitOn**: *argument name* - The named args must be a list. This list will be split into separate entries and executed as parallel jobs. Notice that nested splits are not supported, so the workflow must be merged before a new split. If you need multiple splits, check *microservices*, which is in a separate repository (cryonite/microservices).

  **replicas**: *number* - How many replicas to start if managing containers. Default 1, only used for Kubernetes integration for now.

  **type**: *normal/admin* - type of CryoCloud worker that should take this job. Admin jobs are typically processed in bulk, should be rather quick and possibly IO intensive. Good examples of admin jobs could be moving files or archiving them after processing. Note that each physical CryoCloud node typically only has a single admin worker, but at least one normal worker pr CPU.

  **volumes**: *[[source1, dest1, ro], [source2, dest2, rw]]* - Force mappings of directories as volumeqs. CryoCloud will automatically map directories that are given as paths, so you only need this if your module require external volumes that are not referred to by the workflow. Try without specifying this first.

  **workdir**: *path* - Working directory for this module. Must be an absolute path on the processing nodes (not the one running ccworkflow). Useful for example as *"workdir": {"option": "codedir"}*, allowing command line specification of the code to run.

### Argument definition:

**args**: *{argumentname: value}* where argument is the name of the input argument, value can be one of:

  * a string - is delivered as is, example {"ignoreerrors": "true"}

  * Config a config parameter, "blocksize": {"config": "blocksize"}. This requires "config" to be set in the module definition (e.g. "config": "Cryonite.Readers.Sentinel")
  
  * Output: another module's return value: {"output": "modulename.returnvaluename"}, e.g.: {"src": {"output": "MyInput.product"}} to use the "product" return value from the "MyInput" module as "src" input argument
  
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


## Existing CryoCloud modules
CryoCloud has some modules already built in to provide generic functionality. 

### Input modules
  **cmdline** - Commandline input, mostly useful for testing where you want to start processing from the command line. When the queued pebble has completed, it will exit.

  **dirwatcher** - Watch a directory for changes, trigger each new, stable file as a pebble. Supports recursive monitoring as well. Useful if processing should be done automatically for any newly added file. Does lag a bit to ensure that files are not still being updated.

  **netwatcher** - Listen to a port and handle HTTP requests. Provide a JSON Schema to validate posts automatically. Allows posting of resources to process.


### Administrative modules
  **communicate** - Allow communication between processing lines/pebbles. This is only useful in very complicated graphs, and should likely not be used. Take a look at *runIf* instead and see if that can help. Created to allow processing of one pebble to wait for processing of another pebble if for example two products being processed have dependencies for each other.

  **move** - Move files to or from local disks. Normally handled automatically by CryoCloud, but could be useful to make a backup copy or handle large or slow IO tasks.

  **noop** - Does nothing, but will sleep for a given number of seconds. Only useful for basic testing of graphs.

  **unzip** - Unzip files. Normally handled automatically by CryoCloud but particularly large jobs or more advanced jobs can be created using this module explicitly.

  **remove** - Remove files or folders. Useful if tempdirs are not used and you need to clean up files, for example deleting the input file when completed. This module also can clean up unzipped versions of files (in fact, the orinal file is likely already deleted if unzipped).

  **waitforfile** - Wait for local files to become "stable", could be used to wait for an external process to finish and provide it's results somewhere.


### Integration modules
  **docker** - Run other modules in docker environments. Normally you this module should never be used.

  **idl** - Run processes in IDL, monitor and report. Do not use this directly, but write custom modules that use this module. Look for examples in CryoniteOcean.

### Processing modules
  **ffmpeg** - Transcode files using FFMPEG

  **build_docker** - Build dockers. Don't use if you are not 100% sure
