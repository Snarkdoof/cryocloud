#!/bin/bash

echo "Startup script for Google Cloud ccnodes"

cd ~/git/cryocloud
git pull

# Now we're current, update all repos
cd
/home/cryocore/git/cryocloud/clouds/update_repos `pwd`

# Start ccnodes
screen -d ccnode

# Check for GPUs and start GPU nodes too
