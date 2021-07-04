from data_ingestion.data_transform import DataTransform
from data_ingestion.db_operations import DBOperations
from data_ingestion.raw_validation import RawDataValidation
from logger.logger import Logger


class TrainValidation:
    def __init__(self, path):
        self.raw_data = RawDataValidation(path)
        self.dataTransform = DataTransform()
        self.dBOperation = DBOperations()
        self.file_object = open("training_Logs/Training_Main_Log.txt", 'a+')
        self.log_writer = Logger()

    def train_validation(self):
        try:
            self.log_writer.log(self.file_object, 'Start of Validation on files!!')
            # extracting values from prediction schema
            date_stamp_len, time_stamp_len, column_names, no_of_columns = self.raw_data.values_from_schema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manual_regex_creation()
            # validating filename of prediction files
            self.raw_data.validate_raw_data_filename(regex, date_stamp_len, time_stamp_len)
            # validating column length in the file
            self.raw_data.validate_col_len(no_of_columns)
            # validating if any column has all values missing
            self.raw_data.validate_missing_values_in_col()
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object, "Starting Data Transforamtion!!")
            # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replace_missing_values_with_null()

            self.log_writer.log(self.file_object, "DataTransformation Completed!!!")

            self.log_writer.log(self.file_object,
                                "Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.db_create_table('Training', column_names)
            self.log_writer.log(self.file_object, "Table creation Completed!!")
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dBOperation.db_insert_good_data('Training')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.del_dir_existing_good_data()
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.archive_bad_data()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
            # export data in table to csvfile
            self.dBOperation.export_from_db_to_csv('Training')
            self.file_object.close()

        except Exception as e:
            raise e
