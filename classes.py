from dataclasses import dataclass
from typing import Callable

@dataclass()
class Scrap():
    name:str
    func_scrap:Callable
    func_last_date:Callable    
    
