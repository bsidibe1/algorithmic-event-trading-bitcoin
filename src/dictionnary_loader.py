import pandas as pd
import numpy as np
import gdown

from pathlib import Path
import sys
import os
import logging
from tqdm.auto import tqdm


logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger(__name__)

def load_loughran_mcdonald_dictionary(cache_dir=None, force_reload=False, quiet=True): 
    if cache_dir is None:
        cache_dir = Path(os.getcwd()) 
    if isinstance(cache_dir, str):
        cache_dir = Path(cache_dir)
        
    filename = cache_dir / 'Loughran-McDonald_MasterDictionary_1993-2021.csv'
    if (filename.exists()) & (~force_reload):
        logger.info(f"logging from cache file: {filename}")
    else: 
        logger.info("loading from external source")
        url = 'https://drive.google.com/uc?id=17CmUZM9hGUdGYjCXcjQLyybjTrcjrhik'
        output = str(filename)
        gdown.download(url, output, quiet=quiet, fuzzy=True)
        
    return pd.read_csv(filename)