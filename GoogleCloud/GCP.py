"""
really only useful for GCP itself
"""
from import_govies import import_govies
from import_yahoo import import_yahoo
from scrap_selenium import selenium_scrap

# URL_REQUEST_ROOT = "https://europe-west6-hybrid-elixir-392513.cloudfunctions.net/test_import?param="


dictFunc = {
    1: import_govies,
    2: import_yahoo,
    3: selenium_scrap,
    
}



def run_import(argument=None):
    """ need the argument for GCP
    choose which function to call 
    """

    if argument:
        try:
            param = argument.args.get('param')
            # param = argument
            func = dictFunc[int(param)]
            print(f'{param=}, {func=}')
            return func()
        except Exception as e:
            raise ValueError(f'param {param} not valid - should be in {list(dictFunc.keys())}') from e
    else:
        raise ValueError('No argument passed in the http request')

    
# run_import('2')