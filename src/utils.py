import numpy as np 
import pandas as pd

# Fonction intermédiaire pour la présentation des résultats
def sharpe_ratio(df, num_period_per_year=365):
    if num_period_per_year is None:
        return np.nan
    else:
        return df.mean() / df.std() * np.sqrt(num_period_per_year)

