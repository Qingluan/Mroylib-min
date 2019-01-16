from qlib.log import LogControl
try
    import pandas as pd
    import numpy as np
except ImportError as e:
    LogControl.err("pip3 install pandas numpy")

def data_from(t, f):
	fun = "from_"+ t
	if hasattr(pd.DataFrame, fun):
		func = getattr(pd.DataFrame, fun)
		return func(f)

