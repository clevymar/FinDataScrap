import os
import sys
parentdir = os.path.dirname(os.path.abspath(__file__))
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)

import databases.classes

for el in sys.path:
    print(el) 