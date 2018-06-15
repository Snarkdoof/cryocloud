

## Simple overview:

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

  "ccnode": "If given, run on the given CryoCloud processing node - or use runtime stats (stat) or config, typically {"stat": "parent.node"}

  "downstreamOf": [parent1, parent2] - These are the edges of the graph, this module processes AFTER some other node. Refers to the unique name

  "children" [module definitions] - Inline definition of children - same as giving this module a name and set "downstreamOf" to this modules name

  "args": {argument definition} - How to resolve input arguments for this module (see below)
}


### Argument definition:

"args": {argumentname: value} where value can be one of:

  * a string - is delivered as is, example {"ignoreerrors": "true"}
  * Output: another module's return value: {"output": "modulename.returnvaluename"}, e.g.: {"src": {"output": "MyInput.product"}} to use the "product" return value from the "MyInput" module as "src" input argument
  * Config a config parameter, "blocksize": {"config": "blocksize"}. This requires "config" to be set in the module definition (e.g. "config": "Cryonite.Readers.Sentinel")
  * Stat: Runtime stats, possible stats for now are: "node", "worker", "priority", "runtime", "cpu_time", "max_memory". Difficult to see why you would use this for now.

  * you can also specify that it's a file, allowing the worker to automatically copy/unzip etc. Example:
     "args": {"src": {"output": "parent.dst", "type": "file", "proto": "http", unzip": "true", "copy": "true"}}

     In this the "copy" is default true, proto is default "ssh" and unzip is default false 
     The output "/tempdisk/foo/bar" would in this case be rewritten to "http://[ip of node that executed the parent]/tempdisk/foo/bar"


