import logging
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def setup_notebook(log_level: int = logging.INFO) -> int:
    project_root = Path(__file__).resolve().parents[1]
    project_root_str = str(project_root)
    if project_root_str not in sys.path:
        sys.path.append(project_root_str)

    from logger import setup_logging

    setup_logging(level=log_level, log_filename="notebook.log")

    plt.style.use("ggplot")
    pd.set_option("display.max_columns", 100)
    pd.set_option("display.float_format", "{:,.4f}".format)

    return 0


setup = setup_notebook()
