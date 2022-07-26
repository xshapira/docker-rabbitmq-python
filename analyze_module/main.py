import glob
import json
import logging
import os
import pathlib
from collections import Counter
from os.path import join

import messageBroker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logger.info("Analyze module is listening...")

    dir_path = join(pathlib.Path(), "theHarvester")
    data_path = glob.glob(f"{dir_path}/**/*", recursive=True)
    files = [i for i in data_path if os.path.isfile(i)]

    def get_file_type():
        final_files = {"file_types": {}}
        counts = Counter()
        for file in files:
            file_type = os.path.splitext(file)[1]

            if "." not in file_type:
                # Checking if the file type is unknown. If it is unknown,
                # it will set the file type to unknown.
                file_type = "unknown"
            counts[file_type] += 1

        for file_type, count in counts.items():
            final_files["file_types"].update({file_type: count})
        return final_files

    def get_file_size():
        final_files = {"file_sizes": {}}
        # Sorting the files by size and then taking the top 10 files.
        files_ascending = sorted(files, key=lambda x: os.stat(x).st_size, reverse=True)[
            :10
        ]

        for path_of_file in files_ascending:
            size_of_file = os.stat(path_of_file).st_size
            # Show the size in megabytes
            size_of_file_mb = f"{str(round(size_of_file / (1024 * 1024), 3))} MB"
            final_files["file_sizes"].update({path_of_file: size_of_file_mb})
        return final_files

    file_types = get_file_type()
    file_sizes = get_file_size()
    # Merging two dictionaries
    final_files = {**file_types, **file_sizes}
    final_files_to_json = json.dumps(final_files)

    logger.info(final_files_to_json)
    try:
        messageBroker.send_message("letterbox", final_files_to_json)
        print("Files were sent!")
    except Exception as ex:
        print(f"Files were not sent {ex}")
