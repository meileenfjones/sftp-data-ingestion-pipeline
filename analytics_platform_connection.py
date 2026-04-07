import tempfile
from pathlib import Path
from sqlalchemy import create_engine
############################################################################################################
class AnalyticsPlatformConnection:
    def __init__(self, keepass_db, keepass_entry):
        self.__keepass_db = keepass_db
        self.__temp_directory = None
        self.temp_directory_created = False
        self.connection_engine = None
        self.keepass_entry = self.__keepass_db.find_entries(title=keepass_entry, first=True)
        if self.keepass_entry is None:
            raise Exception("FAILURE: Entry does not exist")

    def __create_connection_string(self):
        """
        set the database connection string attribute
        """
        # grabs different credentials from keepass and saves them to a variable
        try:
            driver = self.keepass_entry.custom_properties['driver']
            user = self.keepass_entry.username
            password = self.keepass_entry.password
            host = self.keepass_entry.url
            port = self.keepass_entry.custom_properties['port']
            # db_name = self.database_name
            return f'{driver}://{user}:{password}@{host}:{port}/analytics_platform_production'
        except KeyError as err:
            raise Exception("FAILURE: Credential not found: " + str(err))

    def initialize_temp_directory(self):
        """
        creates and sets a temporary directory attribute to store ssl files
        """
        self.__temp_directory = tempfile.TemporaryDirectory(dir=Path(__file__).parent)

        self.temp_directory_created = True

    def kill_connection(self):
        """
        deletes temporary directory if initialized along with ssl files inside
        :return:
        """
        if self.temp_directory_created:
            self.__temp_directory.cleanup()
            self.temp_directory_created = False
        else:
            pass

    def create_analytics_platform_connection(self, use_ssl):
        """
        connects to analytics_platform database using connection string attribute and ssl credentials if needed
        :param use_ssl: Boolean for whether ssl credentials will be needed, default to True
        :return: SQLAlchemy engine
        """
        if self.temp_directory_created is True:
            self.kill_connection()

        connection_string = self.__create_connection_string()
        # do not create ssl files if not needed to connect
        if not use_ssl:
            engine = create_engine(connection_string)
        else:
            self.initialize_temp_directory()
            # creates and populates 3 files in temp directory with decoded binary data and saves path to dictionary
            ssl_path = {}
            for ssl_file in ['client.crt', 'client.key', 'root.crt']:
                binary = self.__keepass_db.find_attachments(element=self.keepass_entry, filename=ssl_file, first=True)
                new_ssl_path = Path(self.__temp_directory.name).joinpath(ssl_file)
                new_ssl_path.write_text(binary.data.decode())
                ssl_path[ssl_file] = str(new_ssl_path)
            engine = create_engine(connection_string, connect_args={
                'sslmode': 'verify-ca', 'sslcert': ssl_path['client.crt'], 'sslkey': ssl_path['client.key'],
                'sslrootcert': ssl_path['root.crt']
            })
        self.connection_engine = engine

    def can_connect(self):
        try:
            self.connection_engine.connect()
            return self.connection_engine
        except Exception as err:
            raise Exception("FAILURE: " + str(err))
