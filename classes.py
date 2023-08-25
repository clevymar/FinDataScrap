from dataclasses import dataclass
from typing import Callable

from common import last_bd
@dataclass()
class Scrap():
    name:str
    func_scrap:Callable
    func_last_date:Callable    
    datetoCompare:str = last_bd
    
