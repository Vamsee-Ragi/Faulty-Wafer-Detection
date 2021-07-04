from datetime import datetime


class Logger:
    """ Logger to log all the training and testing processes """

    def __init__(self):
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")

    def log(self, file_object, log_message):
        file_object.write(str(self.date) + "/" + str(self.current_time) +
                          "\t\t" + log_message + "\n")
