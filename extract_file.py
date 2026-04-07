import config
import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime as dt, timedelta
from pykeepass import PyKeePass
from paramiko.client import SSHClient
from paramiko.hostkeys import HostKeyEntry
from paramiko.ssh_exception import AuthenticationException
from relevant_connection import RelevantConnection

from prefect import flow
from prefect_email import EmailServerCredentials, email_send_message


##############################################################################################

@flow
def prefect_email(error_message):
    email_server_credentials = EmailServerCredentials.load("prefect-ddbot-email")
    email_addresses = [email_to_notify]
    print(error_message)
    for email_address in email_addresses:
        email_send_message.with_options(name=f"email {email_address}").submit(
            email_server_credentials=email_server_credentials,
            subject="EPIC Appointment Extract - Script Error",
            msg=error_message,
            email_to=email_address,
        )


try:
    # open keepass
    keepass_db = PyKeePass(filename=config.KDBX_FILE, keyfile=config.KEY_PATH)

    # get credentials to sftp site
    sftp_credentials = keepass_db.find_entries(title=keepass_entry_title, first=True)
    username = sftp_credentials.username
    password = sftp_credentials.password
    hostname = sftp_credentials.url

    # search for known_hosts.txt attachment, decode, and grab first line available in case multiple lines
    known_hosts = keepass_db.find_attachments(element=sftp_credentials, filename='known_hosts.txt', first=True)
    public_keys = known_hosts.data.decode()
    public_key = public_keys.split('\n')[0]

    print('Creating SSH Client')
    # create an ssh client / "console"
    ssh_client = SSHClient()
    # create a hostkey entry using public key and add to client
    hostkey_entry = HostKeyEntry.from_line(public_key)
    ssh_client._host_keys._entries.append(hostkey_entry)
    print('Connecting to SFTP')
    # connect to the remote server and open a sftp session
    ssh_client.connect(username=username, password=password, hostname=hostname)
    sftp_client = ssh_client.open_sftp()
    sftp_client.chdir(file_directory)
    # filter to only appointment extract files
    sftp_extract_list = [x for x in sftp_client.listdir_iter() if file_string in x.filename]
    if len(sftp_extract_list) > 0:
        # get the last extract
        last_extract = max(sftp_extract_list, key=lambda x: x.st_mtime)
        # check when last file was modified / uploaded to sftp
        if dt.fromtimestamp(last_extract.st_mtime).date() == dt.today().date():
            try:
                # download file, close connection
                print('Downloading most recent file and closing SFTP connection: ', last_extract.filename)
                sftp_client.get(remotepath=last_extract.filename,
                                localpath=config.OUTPUT_DIR.joinpath(last_extract.filename))
                # Clean up Extract files in SFTP that are older than a month old
                for extract in sftp_extract_list:
                    sftp_upload_time = dt.fromtimestamp(extract.st_mtime)
                    is_retired = sftp_upload_time < dt.now() - timedelta(days=30)
                    if is_retired:
                        print(f'Deleting File from SFTP: {extract.filename}')
                        sftp_client.remove(extract.filename)
                sftp_client.close()
            except PermissionError as err:
                error = f'SFTP Operation Failed: ({str(err)}) due to a permissions error on the remote server'
                print(error)
                sftp_client.close()
                prefect_email(error)
                sys.exit()
            except ValueError as err:
                error = f'DataFrame Error: {str(err)}'
                print(error)
                sftp_client.close()
                prefect_email(error)
                sys.exit()
            except Exception as err:
                error = f'Other Error: ({str(err)})'
                print(error)
                sftp_client.close()
                prefect_email(error)
                sys.exit()
        else:
            error = 'No data file from today. Closing SFTP connection'
            print(error)
            sftp_client.close()
            prefect_email(error)
            sys.exit()
    else:
        error = 'No extract files available. Closing SFTP connection'
        print(error)
        sftp_client.close()
        prefect_email(error)
        sys.exit()
except AuthenticationException as err:
    error = f'Cannot connect via SFTP due to authentication error ({str(err)})'
    print(error)
    prefect_email(error)
    sys.exit()
except Exception as err:
    error = f'Cannot connect via SFTP due to other error ({str(err)})'
    print(error)
    prefect_email(error)
    sys.exit()

try:
    # Connect to SQL database
    db = AnalyticsPlatformConnection(keepass_db=keepass_db, keepass_entry=entry_name)
    db.create_relevant_connection(use_ssl=True)
    db_engine = db.can_connect()

    # columns expected from the extract file
    cols_expected = ['col1','col2', 'date1','date2','bool_col']

    # format data from last extracted appointment file
    df = pd.read_csv(config.OUTPUT_DIR.joinpath(last_extract.filename), usecols=cols_expected,
                     parse_dates=['date1', 'date2'], na_values=np.nan)
    df['date1'] = df['date1'].dt.date
    df['date2'] = df['date2'].dt.date
    df.rename(columns={'bool_col': 'col3'}, inplace=True)
    df['col3'] = np.where(df['col3'] == 'Yes', True,
                                              np.where(df['col3'] == 'No', False, None))
    print('Uploading data to Analytics Platform')
    df.to_sql(con=db_engine, schema='custom', name=table_name, if_exists='replace',
              index=False)
    # Clean up exported Extract files that are older than a month old
    exported_extract_files = [file for file in config.OUTPUT_DIR.glob('*.csv') if file.is_file()]
    for file in exported_extract_files:
        creation_date = dt.fromtimestamp(os.path.getctime(file))
        modify_date = dt.fromtimestamp(os.path.getmtime(file))
        is_retired = min(creation_date, modify_date) < dt.now() - timedelta(days=30)
        if is_retired:
            print(f'Deleting File from Directory: {file.name}')
            os.remove(file)
except:
    error = 'Failed uploading updated extracted appointment data'
    print(error)
    prefect_email(error)
finally:
    print('Closing connection')
    db.kill_connection()
    sys.exit()
