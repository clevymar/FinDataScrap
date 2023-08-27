import os
import sys
currentdir = os.path.dirname(os.path.abspath(__file__))
parentdir = os.path.dirname(currentdir)
if currentdir not in sys.path:
    sys.path.insert(0, currentdir)
if parentdir not in sys.path:
    sys.path.insert(0, parentdir)
    
