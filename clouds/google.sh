#!/bin/bash

echo "Startup script for Google Cloud ccnodes"

cd ~/git/cryocloud
git pull

# Now we're current, update all repos
cd ~/git
/home/cryocore/git/cryocloud/clouds/update_repos `pwd`

# Check for any SSD disks, mount on /scratch as raid-0
/home/cryocore/git/cryocloud/clouds/make_ssd_raid.py /dev/md0 /scratch

# Start ccnodes
screen -d -m ccnode

# Check for GPUs and start GPU nodes too
