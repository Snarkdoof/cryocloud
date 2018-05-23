FROM cryocloud

ADD ksat /cc/ksat

ENV PYTHONPATH=$PYTHONPATH:ksat

CMD ["python3", "./CryoCloud/Tools/head.py", "--handler", "idltest"]