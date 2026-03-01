FROM python:3.12-slim-bookworm
COPY password/ /
COPY requirements.txt message_broker.py config.py logger.py /
WORKDIR /
RUN pip install -r requirements.txt

ENTRYPOINT ["python","main.py"]
