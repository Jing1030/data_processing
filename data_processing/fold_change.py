import math

def log_fold_change(df, control_list, experimental_col):
    """
        Calculates the log2 fold change between experimental column(s) and
        the average of control columns and returns dataframe

        df: pandas dataframe with values
        control_list: list of columns to use as base
        experimental_col: column name or list of columns to calculate fold change of
    """
    controlSer = df.loc[:,control_list].mean(axis=1)
    experDF = df.loc[:,experimental_col]
    fcDF = experDF.div(controlSer, axis=0)
    fcDF= fcDF.applymap(lambda x: math.log(x,2))
    return fcDF

def fold_change(df, control_list, experimental_col):
    """
        Calculates the fold change between experimental column and
        the average of control columns and returns series

        df: pandas dataframe with values
        control_list: list of columns to use as base
        experimental_col: column name or list of columns to calculate fold change of
    """
    controlSer = df.loc[:, control_list].mean(axis=1)
    exper = df.loc[:, experimental_col]
    fc = exper.div(controlSer)
    return fc