import time
import os
from functools import wraps

from pathlib import Path
import os
import time
from enum import Enum
from typing import Union, Optional

import pandas as pd

"""
MYPYTHON_ROOT=os.environ['ONEDRIVECONSUMER']+'\\Python Scripts\\'
"""



class Color(Enum):
    HEADER = '\033[95m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    WARNING = '\033[93m'

    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    RED = '\033[91m'
    PINK = '\033[95m'
    
    RESULT  = GREEN
    COMMENT = BLUE

    def __str__(self):
        return self.value


def print_color(text:str, color:Union[str,Color], logger=None):
    """
    The function `print_color` takes a string and a color as input and prints the string in the
    specified color.

    :param text: A string that represents the text to be printed in color
    :param color: The color parameter is a Union of either a string or a Color enum. It is used to
    specify the color in which the text should be printed. If a string is provided, it is converted to
    the corresponding Color enum value
    :type color: Union[str,Color]
    """
    if isinstance(color, str):
        col=Color[color.upper()]
    else:
        col=color
    if logger:
        logger.info(f"{col}{text}{Color.ENDC}")
    else:
        print(f"{col}{text}{Color.ENDC}")



def timer(func):
    """A decorator that prints the execution time for the decorated function"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        print_color(f"[I]Function {func.__name__} from module {func.__module__} took {end_time - start_time:,.1f} seconds",'COMMENT'))
        return result
    return wrapper


if os.environ.get('HOMEPATH')=='\\Users\\clevy':
    LOCATION='LOCAL'
else:
    LOCATION='SERVER'

def isLocal():
    return (LOCATION=="LOCAL")


if __name__ == '__main__':
    print_color('Test test testing', Color.HEADER)