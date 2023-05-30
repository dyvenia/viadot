## Unit tests

Unit tests were created for each source in the viadot repository. It is very important to run the unit tests when you have finished working on viadot. This increases the likelihood of detecting any errors that our changes may have caused.

### Installation of packages

Running unit tests requires the installation of several necessary packages. These can be found in the file `requirements-dev.txt`.

### Running tests inside the container

#### Entry into the container

```bash 
docker exec -it viadot_2 bash
```
Perform the above command  to work inside the container. Inside the container, the root directory is mapped to `/home/viadot` by default. You can add your own paths inside `docker/docker-compose.yml` or uncomment one of the default ones.

#### Installation of development packages 

```bash
pip install -r requirements-dev.txt
```
#### Running all tests
```bash
pytest tests/unit
```

### Running tests from the command line

```bash 
docker exec -it viadot_2 sh -c 'pytest tests/unit/'
```