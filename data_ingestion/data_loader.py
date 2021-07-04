import pandas as pd


class DataLoader:
    """
    Class used to obtain the data from the csv file.
    """

    def __init__(self, file_object, logger):
        self.training_file = 'training_file_from_db/input_file.csv'
        self.file_object = file_object
        self.logger = logger

    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from the source.
        Output: A Pandas Dataframe
        On Failure: Raise Exception

        """
        self.logger.log(self.file_object, 'Entered the get_data method of the data_getter class')
        try:
            self.data = pd.read_csv(self.training_file)
            self.logger.log(self.file_object, 'Data Load Successful.Exited the get_data method of the '
                                              'Data_Getter class')
            return self.data

        except Exception as e:
            self.logger.log(self.file_object,
                            'Exception occured in get_data method of the Data_Getter class. Exception message: '
                            + str(e))
            self.logger.log(self.file_object,
                            'Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise Exception()
