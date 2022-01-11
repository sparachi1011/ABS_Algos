"""
Created on Thu Nov 22 15:53:10 2018

@author: sparachi
"""

from ALgorithm1_Wrapper import *
from conf import *
from connectToDb import *
from validationScript_algo1 import *

my_logger = logging.getLogger(__name__)


if __name__ == '__main__':
        try:
            start_time = datetime.datetime.now().replace(microsecond=0)
            algorithm_processing(algorithm_name, historical_flag, historical_start_date, historical_end_date)
            end_time = datetime.datetime.now().replace(microsecond=0)
            time_taken = end_time - start_time
            my_logger.info(f'Execution completed - total time: {time_taken}')
        except Exception as e:
            my_logger.error(e)
            my_logger.error("PY-ERROR: Executable main function Failed! ")

