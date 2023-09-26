FROM python:3.11

RUN apt update && \
 DEBIAN_FRONTEND=noninteractive \
 apt-get install -y --allow-downgrades --allow-remove-essential --allow-change-held-packages \
 python3-pip

ADD . /vox_harbor
WORKDIR /vox_harbor

RUN python3 -m pip install -r requirements.txt
EXPOSE 8002

CMD ["python3", "main.py", "shard"]
