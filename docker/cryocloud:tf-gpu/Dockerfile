# We're using Python3 for CryoCloud
FROM tensorflow/tensorflow:latest-devel-gpu-py3


# GDAL
# ADD docker/cryocloud/gdal-docker-master/ /usr/local/src/gdal-docker
#RUN /usr/local/src/gdal-docker/build.sh


# RUN apt-get update && apt-get install psutil

# Raster IO++
RUN add-apt-repository ppa:ubuntugis/ppa; apt-get -y update; apt-get -y install python-numpy gdal-bin libgdal-dev; pip3 install rasterio

# Trim away cache files - no need for these
RUN apt-get clean

RUN pip3 install keras scikit-learn pandas matplotlib

ADD . /cc
ADD docker/cryocloud:tf/requirements.txt /cc
WORKDIR /cc
RUN  pip3 install -r requirements.txt

ENV NAME CryoCloud
ENV PYTHONPATH="${PYTHONPATH}:/cc/:/cclib"
ENV PATH="${PATH}:/cc/bin"

RUN rm /usr/bin/python; ln -s python3.5 /usr/bin/python

# Copy CryoCore & CryoCloud 
ADD ksat/.config /root/.cryoconfig
ADD ksat/.config /.cryoconfig
RUN mkdir /cclib

# Do we have an application to run?
# We're starting a CryoCloud node for now
# CMD ["python3", "./CryoCloud/Tools/node.py"]

# RUN PYTHONPATH=. python3 ./CryoCore/Tools/TailLog.py


# We test this by printing logs (should list something)
# CMD ["python3", "./CryoCloud/Tools/node.py"]
# CMD ["python3", "./CryoCore/Tools/TailLog.py"]
