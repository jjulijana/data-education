# Processing ROOT Data

Datasets can be found here: [ATLAS Open Data](https://opendata.atlas.cern/docs/documentation/data_format/access/)
Additional information: [ATLAS Open Data Overview](https://opendata.atlas.cern/docs/documentation/overview_data/data_education_2016/)

## Using `process.py`

### Download and Install ROOT

```bash
wget https://root.cern/download/root_v6.30.06.Linux-almalinux9.3-x86_64-gcc11.4.tar.gz
tar -xzvf root_v6.30.06.Linux-almalinux9.3-x86_64-gcc11.4.tar.gz
```

Set up environment variables:

```bash
export ROOTSYS=/path/to/root/
export PATH=$ROOTSYS/bin:$PATH
export LD_LIBRARY_PATH=$ROOTSYS/lib:$LD_LIBRARY_PATH
export PYTHONPATH=$ROOTSYS/lib:$PYTHONPATH
```

Verify the ROOT installation:

```bash
root -l
```

### Set Up Python Environment

Create a virtual environment and install the required packages:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
Note: The latest version of ROOT is compatible with Python version 3.10.

### Configure Database

db_config.ini should look like this:
```bash
[postgresql]
host=your_postgres_host
port=your_postgres_port
database=your_database_name
user=your_database_user
password=your_database_password
```

### Running the Script

```bash
python process.py
```

### Logging

The script appends logs to processing.log with a separator between different runs for easy identification.