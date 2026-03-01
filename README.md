# docker-rabbitmq-python

A mini Docker-based system containing three modules: `password`, `analyzer`, and `controller`.

The `password` module job is searching inside a folder (theHarvester) containing multiple files and folders and extracting a password contained in one of them.

The `analyzer` module is analyzing the files as so:
Finds the number of files from each type (e.g. .py, .txt, etc...) and lists the top 10 files by size sorted.

The final module is the `controller` whose job is to execute the other modules and output the results (from both modules) to a JSON file.
The communication between the controller and the module is conducted through a message broker—RabbitMQ.

There is no communication between the analyze and password modules due to they are connected to different network. The modules communicate directly to the `controller` as the `controller` is the one who creates the JSON file based on the information from the other two modules.

## Consumer-producer concept

The `controller` is the consumer. `password` and `analyzer` are the producers—sending to the queue. Hence `password` and `analyzer` modules produce (publish) to the queue, while `controller` consumes from the queue.

## Usage

 To build each image, run the following one-liner:

```bash
docker build -f analyze/Dockerfile -t analyze . && docker build -f password/Dockerfile -t password . && docker build -f controller/Dockerfile -t controller .
```

To run all containers after all the images were built, run the following command:

```bash
docker-compose up --build
```

**Note** that all three modules should run as a docker container, and will communicate via rabbitMQ. Next, the controller will produce a JSON file when it gets the information from the other containers.
