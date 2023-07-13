"""
really only useful for GCP itself
"""
from scrap_simple import import_govies
from import_yahoo import import_yahoo



def run_import(argument=None):
    """ need the argument for GCP
    choose which function to call 
    """
    # import_yahoo()
    import_govies()
