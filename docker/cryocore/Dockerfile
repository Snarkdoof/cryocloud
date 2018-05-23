# We're using Python3 for CryoCloud
FROM python:3.5

WORKDIR /cc

# Copy CryoCore & CryoCloud 
ADD . /cc
ADD ksat/.config /cc
ADD docker/cryocore/requirements.txt /cc

# RUN apt-get update && apt-get install psutil

RUN pip3 install -r requirements.txt

ENV NAME CryoCore
ENV PYTHONPATH="${PYTHONPATH}:/cc/"
ENV PATH="${PATH}:/cc/CryoCore/Tools"

# Do we have an application to run?
# We're starting a CryoCloud node for now
# CMD ["python3", "./CryoCloud/Tools/node.py"]

# RUN PYTHONPATH=. python3 ./CryoCore/Tools/TailLog.py


# We test this by printing logs (should list something)
# CMD ["python3", "./CryoCloud/Tools/node.py"]
CMD ["python3", "./CryoCore/Tools/TailLog.py"]
# CMD ["sleep", "5m"]
