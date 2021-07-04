import csv
import os.path
import shutil
from os import listdir

from logger.logger import Logger
import sqlite3


class DBOperations:
    """ Class to handle all SQL operations """

    def __init__(self):
        self.path = "training_db/"
        self.bad_file_path = "training_raw_files/bad_raw/"
        self.good_file_path = "training_raw_files/good_raw/"
        self.logger = Logger()

    def db_conn(self, database_name):
        """
        Method Name: db_conn
        Description: This method creates a database with given name and if the database already exists
        then it will open the connection to the DB.
        Output: Connection to the DB
        on Failure: Raise ConnectionError
        """
        try:
            conn = sqlite3.connect(self.path + database_name + '.db')
            file = open("training_logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Opened %s database successfully" % database_name)
            file.close()
        except ConnectionError:
            file = open("training_logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error while connecting to the database: %s" % ConnectionError)
            file.close()
            raise ConnectionError
        return conn

    def db_create_table(self, database_name, column_names):
        """
        Method Name: db_create_table
        Description: This method creates a table in the database and populate it with the data from good data file
        Output: None
        On Failure: Raise ConnectionError
        """
        try:
            conn = self.db_conn(database_name)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(NAME) FROM SQLITE_MASTER WHERE TYPE = 'table' \
                           AND NAME = 'good_raw_data'")
            if cursor.fetchone()[0] == 1:
                conn.close()
                file = open("training_logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()
            else:
                for key in column_names.keys():
                    col_type = column_names[key]

                    try:
                        conn.execute(
                            'ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,
                                                                                                     dataType=col_type))
                    except:
                        conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key,
                                                                                                     dataType=col_type))

                conn.close()
                file = open("training_Logs/DbTableCreateLog.txt", 'a+')
                self.logger.log(file, "Tables created successfully!!")
                file.close()

                file = open("training_Logs/DataBaseConnectionLog.txt", 'a+')
                self.logger.log(file, "Closed %s database successfully" % database_name)
                file.close()

        except Exception as e:
            file = open("training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            conn.close()
            file = open("training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed %s database successfully" % database_name)
            file.close()
            raise e

    def db_insert_good_data(self, database_name):
        """
        Method Name: db_insert_good_data
        Description: This method inserts the Good data files from the Good_Raw folder into the above
        created table.
        Output : None
        On Failure: Raise Exception
        """
        conn = self.db_conn(database_name)
        good_file_path = self.good_file_path
        bad_file_path = self.bad_file_path
        onlyfiles = [f for f in listdir(good_file_path)]
        log_file = open("training_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:
            try:
                with open(good_file_path + '/' + file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="/n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file, " %s: File loaded successfully!!" % file)
                                conn.commit()
                            except Exception as e:
                                raise e
            except Exception as e:

                conn.rollback()
                self.logger.log(log_file, "Error while creating table: %s " % e)
                shutil.move(good_file_path + '/' + file, bad_file_path)
                self.logger.log(log_file, "File Moved Successfully %s" % file)
                log_file.close()
                conn.close()
        conn.close()
        log_file.close()

    def export_from_db_to_csv(self, database_name):
        """
        Method Name: export_from_db_to_csv
        Description: This method exports data from the good data file as a CSV file.
        Output : None
        On failure : Raise Exception
        """
        self.file_from_db = 'training_file_from_db/'
        self.filename = 'input_file.csv'
        log_file = open("training_Logs/DbInsertLog.txt", 'a+')
        try:
            conn = self.db_conn(database_name)
            sql = "SELECT * FROM good_raw_data"
            cursor = conn.cursor()
            cursor.execute(sql)
            query_results = cursor.fetchall()
            headers = [i[0] for i in cursor.description]

            if not os.path.isdir(self.file_from_db):
                os.makedirs(self.file_from_db)

            csv_file = csv.writer(open(self.file_from_db + self.filename, 'w', newline='', delimiter=',',
                                       linetermination='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\'))

            csv_file.writerow(headers)
            csv_file.writerow(query_results)

            self.logger.log(log_file, 'File Exported Successfully !')
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, "File Exporting Failes with error: %s" % e)
            log_file.close()
