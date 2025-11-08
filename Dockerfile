FROM python:latest

RUN [ "mkdir", "-p", "/workspace" ]

WORKDIR /workspace
COPY river_monitor_api/ /workspace/river_monitor_api
COPY config.json /workspace
COPY requirements.txt /workspace
RUN [ "python3", "-m", "pip", "install", "-r", "/workspace/requirements.txt" ]

CMD [ "python3", "-m", "river_monitor_api" ]
