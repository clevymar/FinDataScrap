from dataclasses import dataclass
from typing import Callable
import sys
sys.path.insert(0, '..')

from common import last_bd
@dataclass()
class Scrap():
    name:str
    func_scrap:Callable
    func_last_date:Callable    
    datetoCompare:str = last_bd
    
