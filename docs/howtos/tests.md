## Unit tests

Unit tests were created for each source in the viadot repository. It is very important to run the unit tests when you have finished working on viadot. This increases the likelihood of detecting any errors that may have caused our changes.

### Installation of packages

Running unit tests requires the installation of several necessary packages. These can be found in the file `requirements-dev.txt`

### Installation of packages inside an existing container

Entry into the container

```docker 
docker exec -it viadot_2 bash
```
After executing the above command, you will find yourself inside a container. The structure of the "viadot" library is the same as on your computer, because it is mapped. This means that changes you make to the repository files in the container will also affect the repository files on your computer. The same will work in reverse.

Installation of development packages 

```bash
pip install -r requirements-dev.txt
```
Once the packages are installed, we can move to the directory with the tests and start running them

```bash
cd tests/unit/ && ls -la
```
After executing the command, we will be shown all possible tests to run. 

```bash
total 60
drwxr-xr-x 3 viadot viadot  4096 Mar 23 09:28 .
drwxr-xr-x 6 viadot viadot  4096 Mar 16 09:35 ..
-rw-r--r-- 1 viadot viadot     0 Oct 12 09:35 __init__.py
drwxr-xr-x 2 viadot viadot  4096 Mar 23 08:42 __pycache__
-rw-r--r-- 1 viadot viadot   407 Oct 12 09:35 credentials.json
-rw-r--r-- 1 viadot viadot  2255 Mar 16 09:35 test_config.py
-rw-r--r-- 1 viadot viadot 10007 Mar 16 09:35 test_databricks.py
-rw-r--r-- 1 viadot viadot  6710 Mar 16 09:35 test_databricks2.py
-rw-r--r-- 1 viadot viadot  2267 Mar 16 09:35 test_exchange_rates.py
-rw-r--r-- 1 viadot viadot  1762 Mar 16 09:35 test_redshift_spectrum.py
-rw-r--r-- 1 viadot viadot  3141 Mar 16 09:35 test_s3.py
-rw-r--r-- 1 viadot viadot   504 Mar 16 09:35 test_sharepoint.py
-rw-r--r-- 1 viadot viadot  1959 Mar 16 09:35 test_utils.py
```
Running the selected test 
```bash
pytest test_exchange_rates.py 
```

Running all tests (inside the /tests/unit directory)
```bash
pytest
```

### Installation of packages from command line

Installation of development packages 

```docker
docker exec -it viadot_2 sh -c 'pip install -r requirements-dev.txt'
```
Running the selected test 

```docker
docker exec -it viadot_2 sh -c 'pytest tests/unit/test_exchange_rates.py'
```
Running all tests
```docker 
docker exec -it viadot_2 sh -c 'pytest tests/unit/'
```