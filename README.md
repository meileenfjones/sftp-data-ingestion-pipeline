# EPIC-Appointment-Extract

__Background__  

This project supports a transition between data source systems by adapting how data is delivered and processed. Previously, data was received on a predictable overnight schedule, allowing sufficient time for ingestion and downstream processing before the start of the business day.

With the new system, data delivery occurs later and with variable timing, reducing the available processing window and impacting data availability for operational workflows.

To address this, a supplemental data extract is delivered via SFTP on a consistent schedule. This project automates the retrieval of that extract, processes and loads the data into a target system, and enables timely availability for downstream use cases.

__Deliverables__  
The final solution is an automated Python-based process that runs on a scheduled basis to retrieve data from a remote source and load it into a target system.

## Prerequisites

### Usage

List all items needed for the program to work

- Windows OS
- Task Scheduler
- KeePass database
  - database credentials stored securely
- Python >=(3.11.3)
- pykeepass (>=4.0.3)
- paramiko (>=3.4.0)
- pandas (>=2.0.0)
- SQLAlchemy (>=2.0.8)
- psycopg2 (>=2.9.6)
- prefect (==3.1.0)

## Distribution


### Setup

List all packages/technologies needed to build the program

1. Download most recent python on C drive [latest version for Windows](https://www.python.org/downloads/)
2. Download the [latest release](https://github.com/OleHealth/Crossroads-Survey-Automation/releases/latest) and unzip.
3. Open a Windows Command Prompt Terminal
4. Add attachment known_hosts.txt to keepass entry
    ```
    cd Desktop>ssh-keyscan *host_name* > known_hosts.txt
    Ex: C:
    Ex: cd Users\User\Desktop
    Ex: ssh-keyscan -p 2222 hostname.com > known_hosts.txt
    ```
5. Change directory to the target folder

  ```
  cd target_folder
  ```

6. Create virtual environment

  ```
  python -m venv venv
  ```

7. Activate the virtual environment

  ```
  venv\Scripts\activate.bat
  ```

8. Install Python packages

  ```
  pip install -r requirements.txt
  ```

9. Run Necessary Commands for Prefect

  ```
  pip install "prefect[email]"
  prefect cloud login -k "block key"
  prefect block register -m prefect_email

  ```

10. Create config.py and fill out with following paths

```
from pathlib import Path
############################################################################################################
# specified keepass file and key path for this project

KDBX_FILE = 
KEY_PATH = 

OUTPUT_DIR = 
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
```

10. To automate project, create tasks with Windows Task Scheduler directed at the bat files set to run at specified
    times



### Files

List important files to know about and how they work together

`config.py`
> Includes global variables and any file paths to be used in the script 

`extract_file.py`
> Main script to pull extract from SFTP site and upload to target platform

`analytics_platform_connection.py`
> Helper module to connect to analytics platform


### Methodology

List out steps of how the program works

1. Load credentials for SFTP and target platform 
2. Connect to SFTP and filter only for specified file
3. Download most recent file and validate
4. Upload data into analytics platform and disconnect from SFTP
