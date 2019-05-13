import subprocess
import os


def pnds_to_csv():
    """Converts pnds from a sas7bdat to a csv. If the csv already exists, do nothing."""
    if os.path.isfile("input_datasets/pnds.csv") == False:
        subprocess.run(
            "sas7bdat_to_csv input_datasets/pnds.sas7bdat input_datasets/pnds.csv",
            shell=True,
        )
    else:
        pass
