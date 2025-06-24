
# MongoDB Log Parser

This project provides a Python script to parse MongoDB cluster configuration and database names from log files and output the extracted information in CSV format.

## Prerequisites

- [Devbox](https://www.jetpack.io/devbox/) installed
- Git (optional, but probably needed to cloning the repo outside of devbox shell)

## Setup

1. Clone the repository:
   ```bash
   git clone git@github.com:aidataowl/mongo_parse.git
   ```

2. Navigate to the project directory:
   ```bash
   cd mongo_parse
   ```

3. Enter the Devbox shell:
   ```bash
   devbox shell
   ```
   This will set up the development environment with all required packages, including Python 3.12, and activate a venv.

## Usage

Run the `mongo_parse.py` script with the following arguments:
- `parse_type`: Either "cluster" or "database"
- `input_file`: Path to the log file to parse

```bash
./mongo_parse.py <parse_type> <input_file>
```

The script will parse the specified information and output the results in CSV format to stdout. To save the output to a file, redirect stdout:

```bash
./mongo_parse.py <parse_type> <input_file> > output.csv
```

**Note:** The script outputs log messages to stderr, providing information about the parsing process and any errors. The CSV data is written to stdout. Append `2>/dev/null` if they're distracting.

## Parse Types

### `cluster`
Extracts the following information from the log file:
- Replica set name
- List of hosts (separated by " | ")
- Primary host

The log file should contain the output of a MongoDB command that includes the replica set configuration, such as `rs.conf()`.

### `database`
Extracts database names from lines that start with "** DATABASE:".

Each matching line should be in the format:
```
** DATABASE: database_name
```
The output will include the line number and the database name.

## Examples

### Parsing Cluster Information
Given a file `cluster.log` with:
```
setName: 'myReplicaSet'
hosts: [
  'host1:27017',
  'host2:27017',
  'host3:27017'
]
primary: 'host1:27017'
```
Running:
```bash
./mongo_parse.py cluster cluster.log
```
Outputs:
```
replica_set_name,hosts,primary_host
myReplicaSet,host1:27017 | host2:27017 | host3:27017,host1:27017
```

### Parsing Database Names
Given a file `databases.log` with:
```
** DATABASE: admin
** DATABASE: config
** DATABASE: local
```
Running:
```bash
./mongo_parse.py database databases.log
```
Outputs:
```
line_number,database_name
1,admin
2,config
3,local
```


**Note:** Replace `git@github.com:aidataowl/mongo_parse.git` with whatever internal repository ends up being created.