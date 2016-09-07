# docker-virtual-sensor
A shippable 'virtual sensor' for the iQAS platform.

### Building the docker-virtual-sensor image
Inside the resources root directory (virtual_sensors), type the following command:
```
$ docker build -t antoineog/docker-virtual-sensor .
```

Note: by default, the virtual sensor use a raw temperature dataset from Aarhus. 
If you want to specify your own raw observation file, copy it in the `virtual_sensors/data` directory and add the `--build-arg obsFile=[YOUR-FILENAME]` option.
For instance:
```
$ docker build --build-arg obsFile=my_data_file.txt -t antoineog/docker-virtual-sensor .
```

### Running from the docker-virtual-sensor image
The generic command is:
```
$ docker run -p 127.0.0.1:[PORT]:8080 antoineog/docker-virtual-sensor [SENSOR-ID] [URL-TO-PUBLISH]
```

You should specify 3 arguments:

* PORT: The port you want the virtual sensor will be listening to on localhost (to send API requests)
* SENSOR-ID: The name of the virtual sensor
* URL-TO-PUBLISH: The URL where the virtual sensor has to send its observations

For instance, you could type:
```
$ docker run -p 127.0.0.1:9092:8080 antoineog/docker-virtual-sensor "s01" "http://10.161.3.183:8081/publish/observation"
```
To exit the container, just press `CTRL` + `C`.

Instead, if you prefer to run the docker container in background (in detached mode), just add the `-d` option:
```
$ docker run -dp 127.0.0.1:9092:8080 antoineog/docker-virtual-sensor "s01" "http://10.161.3.183:8081/publish/observation"
```

### Managing the docker-virtual-sensor container

The following are a quick remainder of basic docker commands.

You can see docker containers and their statuses by running `docker ps`. 
```
$ docker ps
CONTAINER ID        IMAGE                             COMMAND                  CREATED             STATUS              PORTS                      NAMES
0657fb1624c3        antoineog/docker-virtual-sensor   "/usr/bin/python3 /ho"   47 seconds ago      Up 51 seconds       127.0.0.1:9092->8080/tcp   prickly_roentgen
```
Note: use the command `docker ps -a` if the list is empty or if you do not find your container.

To stop a given container, just type the following command:
```
$ docker stop prickly_roentgen
prickly_roentgen
```

Now the container is stopped, you can remove it:
```
$ docker rm prickly_roentgen
prickly_roentgen
```
