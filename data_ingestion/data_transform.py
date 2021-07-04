from os import listdir

import pandas

from logger.logger import Logger


class DataTransform:

    def __init__(self):
        self.goodDataPath = "training_raw_files/good_raw/"
        self.logger = Logger()

    def replace_missing_values_with_null(self):
        """
        Method Name: replaceMissingWithNull
        Description: This method replaces the missing values in columns with "NULL" to
        store in the table. We are using substring in the first column to
        keep only "Integer" data for ease up the loading.
        This column is anyways going to be removed during training.

        """

        log_file = open("training_Logs/dataTransformLog.txt", 'a+')
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                csv = pandas.read_csv(self.goodDataPath + "/" + file)
                csv.fillna('NULL', inplace=True)
                # #csv.update("'"+ csv['Wafer'] +"'")
                # csv.update(csv['Wafer'].astype(str))
                csv['Wafer'] = csv['Wafer'].str[6:]
                csv.to_csv(self.goodDataPath + "/" + file, index=None, header=True)
                self.logger.log(log_file, " %s: File Transformed successfully!!" % file)
        except Exception as e:
            self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
            log_file.close()
        log_file.close()
