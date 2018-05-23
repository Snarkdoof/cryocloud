Note:

PYTHONPATH must resolve to the handler and the modules
by default, the processing nodes will add CryoCloud/Modules/ to the path

CryoCore must be configured correctly on all nodes, and on the same DB

Run a testhandler:
PYTHONPATH=.:CryoCloud/Examples/ ./CryoCloud/Tools/head.py  --handler testhandler

Run node (start once on all nodes)
./CryoCloud/Tools/node.py
