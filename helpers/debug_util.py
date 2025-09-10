from .debug_config import verbose

def debug(*args):
    """Print debug messages only when verbose is True."""
    if verbose == True:
        print(*args)

def debug_df(df):
    """Print debug messages for dataframes only when verbose is True."""
    if verbose == True:
        df.info()
