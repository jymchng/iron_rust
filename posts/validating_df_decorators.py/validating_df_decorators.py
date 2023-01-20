import pandas as pd
import numpy as np
from functools import partial, wraps


series_one = pd.Series(
    np.random.choice(range(100), 20),
    name='Value'
)

values = np.random.choice(range(20), 10, replace=False)

df = pd.concat([series_one, series_one.isin(values=values)],
               axis=1)

df.columns = [series_one.name, f'Is Value in Allowed values: {values}?']

values = np.random.choice(range(20), 10, replace=False)

validating_function = partial(pd.Series.isin, values=values)

pd.Series.isin.hello = 'how are you?'

validating_function = wraps(pd.Series.isin)(validating_function)
