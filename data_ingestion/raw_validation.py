import json
import os
import re
import shutil
from datetime import datetime
from os import listdir
import pandas as pd
from logger.logger import Logger


class RawDataValidation:
    """ Class to handle the checks performed on the raw data and move files
    to good or bad data folders accordingly"""

    def __init__(self, path):
        self.batch_directory = path
        self.schema_path = 'schema_training.json'
        self.logger = Logger()

    def values_from_schema(self):
        """
        Method Name: valuesFromSchema
        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
        Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
        On Failure: Raise ValueError,KeyError,Exception
        """
        try:
            with open(self.schema_path, 'r') as json_file:
                schema_training = json.load(json_file)
                json_file.close()

            pattern = schema_training["SampleFileName"]
            date_stamp_file_len = schema_training["LengthOfDateStampInFile"]
            time_stamp_file_len = schema_training["LengthOfTimeStampInFile"]
            column_names = schema_training["ColName"]
            no_of_columns = schema_training["NumberofColumns"]

            file = open("training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = "LengthOfDateStampInFile:: %s" % date_stamp_file_len + "\t" + \
                      "LengthOfTimeStampInFile:: %s" % time_stamp_file_len + "\t " + \
                      "NumberofColumns:: %s" % no_of_columns + "\n"
            self.logger.log(file, message)

            file.close()

        except ValueError:
            file = open("training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return date_stamp_file_len, time_stamp_file_len, column_names, no_of_columns

    def manual_regex_creation(self):
        """
        Method Name :  manual_regex_creation
        Description : This method contains a manually defined regex based on the "FileName" given in "Schema"
        file. This Regex is used to validate the filename of the training data.
        Output: Regex pattern
        On Failure: None
        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def create_dir_good_bad_raw_data(self):
        """
        Method Name: createDirectoryForGoodBadRawData
        Description: This method creates directories to store the Good Data and Bad Data
        after validating the training data.
        Output: None
        On Failure: OSError

        """
        try:

            path = os.path.join("training_raw_files/", "good_raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

            path = os.path.join("training_raw_files/", "bad_raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while creating directory %s:" % ex)
            file.close()
            raise OSError

    def del_dir_existing_good_data(self):
        """
        Method Name: deleteExistingGoodDataTrainingFolder
        Description: This method deletes the directory made to store the Good Data
        after loading the data in the table. Once the good files are
        loaded in the DB,deleting the directory ensures space optimization.
        Output: None
        On Failure: OSError

        """
        try:

            path = "training_raw_files/"
            if os.path.isdir(path + "good_raw/"):
                shutil.rmtree(path + "good_raw/")
                file = open("training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Good Raw directory deleted successfully!!!")
                file.close()

        except OSError as ex:
            file = open("training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while deleting directory %s:" % ex)
            file.close()
            raise OSError

    def del_dir_existing_bad_data(self):
        """
               Method Name: deleteExistingGoodDataTrainingFolder
               Description: This method deletes the directory  to store the bad Data
               after loading the data in the table. Once the good files are
               loaded in the DB,deleting the directory ensures space optimization.
               Output: None
               On Failure: OSError

               """
        try:

            path = "training_raw_files/"
            if os.path.isdir(path + "bad_raw/"):
                shutil.rmtree(path + "bad_raw/")
                file = open("training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad Raw directory deleted successfully!!!")
                file.close()

        except OSError as ex:
            file = open("training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while deleting directory %s:" % ex)
            file.close()
            raise OSError

    def archive_bad_data(self):
        """
        Method Name: moveBadFilesToArchiveBad
        Description: This method deletes the directory made  to store the Bad Data
        after moving the data in an archive folder. We archive the bad
        files to send them back to the client for invalid data issue.
        Output: None
        On Failure: OSError
        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:

            source = 'training_raw_files/bad_raw/'
            if os.path.isdir(source):
                path = "bad_data_archive"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'bad_data_archive/bad_data_' + str(date) + "_" + str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad files moved to archive")
                path = 'training_raw_files/'
                if os.path.isdir(path + 'bad_raw/'):
                    shutil.rmtree(path + 'bad_raw/')
                self.logger.log(file, "Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e

    def validate_raw_data_filename(self, regex, date_stamp_file_len, time_stamp_file_len):
        """
        Method Name: validationFileNameRaw
        Description: This function validates the name of the training csv files as per given name in the schema!
        Regex pattern is used to do the validation.If name format do not match the file is moved to Bad Raw Data
        folder else in Good raw data.
        Output: None
        On Failure: Exception

        """
        self.del_dir_existing_bad_data()
        self.del_dir_existing_bad_data()
        self.create_dir_good_bad_raw_data()

        onlyfiles = [f for f in listdir(self.batch_directory)]

        try:
            f = open("training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if re.Match(regex, filename):
                    split_at_dot = re.split('.csv', filename)
                    split_at_dot = re.split('_', split_at_dot[0])
                    if len(split_at_dot[1]) == date_stamp_file_len:
                        if len(split_at_dot[2]) == time_stamp_file_len:
                            shutil.copy("training_batch_files" + filename, "training_raw_files/good_raw/")
                            self.logger.log(f, "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("training_batch_files" + filename, "training_raw_files/bad_raw/")
                            self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("training_batch_files" + filename, "training_raw_files/bad_raw/")
                        self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("training_batch_files" + filename, "training_raw_files/bad_raw/")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

            f.close()

        except Exception as e:
            f = open("training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def validate_col_len(self, no_of_columns):
        """
        Method Name: validateColumnLength
        Description: This function validates the number of columns in the csv files.
        It is should be same as given in the schema file.
        If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
        If the column number matches, file is kept in Good Raw Data for processing.
        The csv file is missing the first column name, this function changes the missing name to "Wafer".
        Output: None
        On Failure: Exception


        """
        try:
            f = open("training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")
            for file in listdir("training_raw_files/good_raw/"):
                csv = pd.read_csv("training_raw_files/good_raw/" + file)
                if csv.shape[1] == no_of_columns:
                    pass
                else:
                    shutil.move("training_raw_files/good_raw/" + file, "training_raw_files/bad_raw/")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validate_missing_values_in_col(self):
        """
        Method Name: validate_missing_values_in_col
        Description: This function validates if any column in the csv file has all values missing.
        If all the values are missing, the file is not suitable for processing.
        SUch files are moved to bad raw data.
        Output: None
        On Failure: Exception

        """
        try:
            f = open("training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir("training_raw_files/good_raw/"):
                csv = pd.read_csv("training_raw_files/good_raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count += 1
                        shutil.move("training_raw_files/good_raw/" + file,
                                    "training_raw_files/bad_raw/")
                        self.logger.log(f,
                                        "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count == 0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("training_raw_files/good_raw/" + file, index=None, header=True)
        except OSError:
            f = open("training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()
