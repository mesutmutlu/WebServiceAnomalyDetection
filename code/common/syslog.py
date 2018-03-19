import logging
import inspect
import os
import configparser


class SysLog:
    def __init__(self):

        config = configparser.ConfigParser()
        config.read(r"..\config\log.cfg")
        self.logPath = config.get('FilePaths', 'logPath')

#
    def create(self, msg, level):

        if not os.path.exists(self.logPath):
            print("Making subset directory")
            os.mkdir(self.logPath)

        stack = inspect.stack()
        frame = stack[1]
        module = inspect.getmodule(frame[0])
        filenamewithpath = module.__file__
        filestartindex = module.__file__.rfind("/") + 1

        logging.basicConfig(filename=self.logPath+filenamewithpath[filestartindex:]+'.log', level=logging.DEBUG,format='%(asctime)s %(message)s'
                            , datefmt='%m/%d/%Y %I:%M:%S %p')

        if level == 'info':
            logging.info(msg)
        elif level == 'err':
            logging.error(msg)
        elif level == 'debug':
            logging.debug(msg)


if __name__ == "__main__":
    syslog=SysLog()
    print(syslog.getcaller())
    #syslog.create(msg="test", level="info", file="test.py")