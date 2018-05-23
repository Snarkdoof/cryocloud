Docker - installation, programming and running


__________________
Install
__________________
First of all, install docker-ce (add repository and use apt):
  https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/

Also think about adding your user to the "docker" group (edit /etc/group, find docker and add your username at the end - then re-login). The "groups" command should then list "docker". Otherwise, you will have to run docker with sudo.

Build "cryocloud" docker to see that all works
./build-docker.sh cryocloud

It should build. Try running
docker run cryocloud TailLog.py  - it should list some logs.  Use sudo docker run... if you are not in the docker group

_________________
Development
_________________
You are now ready to PROGRAM using it

1: Create a directory in the docker dir (this one) (mkdir docker/<yourname>)
2: Create a Dockerfile. It should contain something like:

# This is a cryocloud based thing CryoCloud will be added to /cc/ in the docker
FROM cryocloud

# Add your own directories to be copied to the docker here. Doesn't need to be under cc, you decide. Add all your directories one after the other.

ADD ksat /cc/ksat

# If you have a requirementes.txt (just a list of pip requirements, one entry on each line)
ADD docker/<yourname>/requirements.txt /cc

# If you need to install system packages, remember the update first to actually have an index of packages
# RUN apt-get update && apt-get install psutil

# Use pip to install any requirements
RUN pip3 install -r requirements.txt

# Export environment things - extend PYTHONPATH, don't overwrite it if you are not totally certain you want to
ENV PYTHONPATH=$PYTHONPATH:ksat

# If you want to run something on the docker while it's being built - for example alter a file, create a symlink
#RUN ["somecommand", "someargument"]

# The command to run when the docker is started
CMD ["python3", "./CryoCloud/Tools/head.py", "--handler", "idltest"]


_________________
Build it
_________________

Again run ./build-docker.sh <yourname>

This should build the docker and succeed.


_________________
Run it
_________________

docker run <yourname>


__________________________________
Running dockers in CryoCloud
__________________________________

CryoCloud can run dockers very easily...

In your CryoCloud head handler, create jobs like this:

    self.head.add_job(1, self._taskid, {"target": "<yourname>", "dirs": {"/rawdata/somedir": "/input", "/processed/somewriteabledir": "/output"}}, module="docker")


And that's pretty much it.

__________________________________
Running the CryoCloud tools in docker...
__________________________________

Just run the cryocore docker with standard tools - you're already in the cryocore directory

docker run cryocloud TailLog.py -f 

You already have PYTHONPATH, CryoCore/Tools and CryoCloud/Tools in your path

__________________________________
Running the head in a docker...
__________________________________

While CryoCloud processing nodes should not run in dockers for performance reasons, the head can. This means that if you want to run stuff on our development machines, things should be easy for you. Build CryoCloud (./build-docker.sh cryocloud) and run it directly with:
docker run cryocloud head.py --help

-----------

If you want to do something more crazy, there is a DockerProcess wrapper defined in CryoCloud/Common that can help you run docker from Python. You likely won't need to, but DockerProcess will at least make it somewhat more easy.

