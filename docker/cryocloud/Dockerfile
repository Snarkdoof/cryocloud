# We're using Python3 for CryoCloud
FROM ubuntu:xenial
# python:3.5

WORKDIR /cc

# Copy CryoCore & CryoCloud 
ADD . /cc
ADD ksat/.config /root/.cryoconfig
ADD docker/cryocloud/requirements.txt /cc

# GDAL
# ADD docker/cryocloud/gdal-docker-master/ /usr/local/src/gdal-docker
#RUN /usr/local/src/gdal-docker/build.sh


# RUN apt-get update && apt-get install psutil
RUN apt-get update
RUN apt-get -y install software-properties-common python-software-properties apt-utils

# Raster IO++
RUN add-apt-repository ppa:ubuntugis/ppa && \
  apt-get -y update && \
  apt-get -y install python-numpy gdal-bin libgdal-dev python3-pip
  
RUN pip3 install rasterio

RUN pip3 install -r requirements.txt

# Trim away cache files - no need for these
RUN apt-get clean

ENV NAME CryoCloud
ENV PYTHONPATH="${PYTHONPATH}:/cc/:/cclib"
ENV PATH="${PATH}:/cc/bin"

RUN rm /usr/bin/python
RUN ln -s python3.5 /usr/bin/python

RUN mkdir /cclib

# Do we have an application to run?
# We're starting a CryoCloud node for now
# CMD ["python3", "./CryoCloud/Tools/node.py"]

# RUN PYTHONPATH=. python3 ./CryoCore/Tools/TailLog.py


# We test this by printing logs (should list something)
# CMD ["python3", "./CryoCloud/Tools/node.py"]
# CMD ["python3", "./CryoCore/Tools/TailLog.py"]