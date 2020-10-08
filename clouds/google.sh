#!/bin/bash

echo "Startup script for Google Cloud ccnodes"

cd /home/cryocore/git/cryocloud
git pull

# Now we're current, update all repos
/home/cryocore/git/cryocloud/clouds/update_repos

# Start ccnodes
screen -d ccnode

# Check for GPUs and start GPU nodes too
