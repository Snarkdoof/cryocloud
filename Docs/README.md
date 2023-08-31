# Installation

Start by checking out CryoCore and CryoCloud wherever you want them.

```
git clone https://github.com/Snarkdoof/cryocore.git
git clone https://github.com/Snarkdoof/cryocloud.git
```

## Install CryoCore

There is an install script for Ubuntu. If you have a different distro you need to go through it manually.

```
cd cryocore
./CryoCore/Install/install.sh

# Optional but recommended - shared memory makes local host stuff way faster
./build/ccshm.sh
```


### CryoCore daemon

If you want to run the cryocore daemon to gather system information and
start/stop processes, you should update some config parameters too.

```

ccconfig System.SystemControl.default_environment="PYTHONPATH=/home/cryocore/git/"
ccconfig System.SystemControl.default_user="cryocore"

# Automatically start
sudo systemctl enable cryocore.service

```

If you use a different username or path, update it as needed.


## Install CryoCloud

There is no particular installation of CryoCloud, but a few environment
variables are highly useful. Again, ensure that the paths are according to
your installation.

```
# Add these to your .bashrc or equivalent - some are likely there after CryoCore installation

export PYTHONPATH=$PYTHONPATH:/home/cryocore/git/cryocore:.
export PATH=$PATH:/home/cryocore/git/cryocore/bin:/home/cryocore/git/cryocloud/bin
export CC_DIR=/home/cryocore/git/cryocloud
```

