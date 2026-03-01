FROM python:3.12-slim-bookworm
RUN mkdir /data
COPY controller/ /
COPY requirements.txt message_broker.py config.py logger.py /
# COPY theHarvester/ /theHarvester/
WORKDIR /
RUN pip install -r requirements.txt
EXPOSE 5672

ENTRYPOINT ["python","main.py"]
