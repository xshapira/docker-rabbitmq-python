import asyncio
import json
import pathlib
from collections import Counter
from typing import Any

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


def get_files_by_type(files: list[str]) -> dict[str, Any]:
    """
    Take a list of files and returns a dictionary containing the file types
    as keys and the number of files with that type as values.
    Sort the file types by most common to least common.

    :param files: list[str]: Get a list of files
    :return: A dictionary containing the file types and the number
    of files that are of each type
    """
    final_files = {"files_by_type": {}}
    counts = Counter()

    for file in files:
        file_path = pathlib.Path(file)

        # Use the `suffix`` attribute to get the file extension
        # otherwise set the file type to unknown.
        file_type = file_path.suffix or "unknown"
        counts[file_type] += 1

    sorted_file_type = sorted(
        counts.items(),
        key=lambda x: x[1],
        reverse=True,
    )

    for file_type, count in sorted_file_type:
        final_files["files_by_type"].update({file_type: count})
    return final_files


def get_sorted_file_sizes(files: list[str]) -> dict[str, Any]:
    """
    Return a dictionary of the top 10 files by size. The function sorts
    the files by size and then takes the top 10.

    :return: A dictionary with the path of the file as key and size
    in mb as value
    """
    final_files = {"sorted_file_sizes": {}}

    # Sorting the files by size and then taking the top 10 files.
    sorted_files = sorted(
        files,
        key=lambda x: pathlib.Path(x).stat().st_size,
        reverse=True,
    )[:10]

    for file in sorted_files:
        file_path = pathlib.Path(file)
        file_size = file_path.stat().st_size
        # Show the size in megabytes
        file_size_mb = f"{str(round(file_size / (1024 * 1024), 3))} MB"
        # Convert `file_path` (a Path object) to a string for use as
        # dictionary key
        final_files["sorted_file_sizes"].update({str(file_path): file_size_mb})

    return final_files


def get_final_files() -> str:
    """
    Return a JSON object containing the file names and sizes of all files
    in the `theHarvester` directory. The function first gets a list of
    all files in `theHarvester` directory, then it creates two
    dictionaries: one that contains only .py files and another that
    contains only .json files.

    It then sorts each dictionary by size (smallest to largest) and merges
    them into one dictionary which is returned as a JSON object.

    :return: A json string with all the files that are in
    the folder `theharvester` and their sizes
    """

    files = get_files_from_path("theHarvester")
    files_by_type = get_files_by_type(files)
    sorted_file_sizes = get_sorted_file_sizes(files)

    # Merging two dictionaries
    final_files = {**files_by_type, **sorted_file_sizes}
    return json.dumps(final_files)


async def publish_message(rabbitmq: RabbitMQ) -> None:
    """
    Publish a message to the letterbox exchange.
    The function takes one argument, `rabbitmq`, which is an instance
    of RabbitMQ.

    :param rabbitmq: RabbitMQ: Access the rabbitmq instance that
    has been created in the main function
    """
    body = get_final_files()
    message = Message(body=body.encode())
    await asyncio.sleep(0.5)
    await rabbitmq.publish(message, routing_key="letterbox")


async def main() -> None:
    log.info("Analyze module is listening...")
    log.info(get_final_files())

    try:
        log.info("Files were sent!")
    except Exception as ex:
        log.info(f"Files were not sent {ex}")

    rabbitmq = await RabbitMQ()
    await publish_message(rabbitmq)


if __name__ == "__main__":
    asyncio.run(main())
