import glob
import json
import logging
import os
import pathlib
import re
from os.path import join

import pika  # noqa

import messageBroker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Password module is listening...")

    dir_path = join(pathlib.Path(), "theHarvester")
    data_path = glob.glob(f"{dir_path}/**/*", recursive=True)
    string_to_match = "password"

    files = [i for i in data_path if os.path.isfile(i)]
    final_files = {}

    def get_password():
        for file in files:
            with open(file, "rb") as fp:
                data = fp.read()
                if string_to_match.encode() in data:
                    output = data.decode("ISO-8859-1")
                    pattern_to_find = re.findall(r"password\S ([^\n]*)", output)
                    # Convert list to string and remove quotes
                    match = str(pattern_to_find)[1:-1].strip("'")
                    final_files.update({"password": match, "filename": file})

        return final_files

    password = get_password()
    # Convert into a json string
    password_to_json = json.dumps(password)

    message_broker = messageBroker()
    channel = message_broker.get_channel()
    channel.queue_declare(queue="letterbox")
    # Publish to the queue
    channel.basic_publish(
        exchange="",
        routing_key="letterbox",
        body=password_to_json,
    )

    print("Message was sent!")

    message_broker.close_connection()
