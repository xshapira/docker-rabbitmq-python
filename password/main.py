import asyncio
import json
import pathlib
import re
from typing import Any, Callable

from aio_pika import Message

import logger
from message_broker import RabbitMQ

log = logger.setup_logger(__name__)


def get_files_from_path(directory: str) -> list[str]:
    """
    Return a list of files from the harvester directory.
    The function is called by the main function and used to create a list
    of files that are then passed into the `get_data_from_file`
    and `send_message` functions.

    :return: A list of all the files in the directory
    """
    dir_path = pathlib.Path(directory)
    data_path = list(dir_path.rglob("*"))
    return [str(f) for f in data_path if f.is_file()]


def get_password(string_to_match: str) -> dict[str, dict[str, str]]:
    """
    Search through all the files in a given directory and return
    any file that contains the word `password`. It will then return
    a dictionary with the filename and password as key value pairs.

    :return: A dictionary containing the password and the filename
    """
    final_files = {"passwords": {}}
    files = get_files_from_path("theHarvester")

    for file in files:
        with open(file, "rb") as fp:
            data = fp.read()
            if string_to_match.encode() in data:
                output = data.decode("ISO-8859-1")
                pattern_to_find = re.findall(rf"{string_to_match}. (\S+)", output)
                # Convert list to string and remove quotes
                # And confirm the password we get is not an empty string
                if match := str(pattern_to_find)[1:-1].strip("'"):
                    final_files["passwords"].update(
                        {"password": match, "filename": file}
                    )

    return final_files


def password_to_json() -> str:
    password: dict[str, Any] = get_password("password")
    # Convert into a json string
    return json.dumps(password)


async def publish_message(
    rabbitmq: RabbitMQ, password_to_json: Callable[[], str]
) -> None:
    """
    Publish a message to the `letterbox` exchange. The function takes
    one argument, `rabbitmq`, which is an instance of RabbitMQ.

    :param rabbitmq: RabbitMQ: Access the rabbitmq instance
    """
    body = password_to_json()
    message = Message(body=body.encode())
    await asyncio.sleep(0.5)
    await rabbitmq.publish(message, routing_key="letterbox")


async def main() -> None:
    log.info("Password module is listening...")
    log.info(password_to_json)

    try:
        log.info("Password was sent!")
    except Exception as ex:
        log.info(f"Password was not sent! {ex}")

    rabbitmq = await RabbitMQ()
    await publish_message(rabbitmq, password_to_json)


if __name__ == "__main__":
    asyncio.run(main())
