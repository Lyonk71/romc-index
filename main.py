# import modules
from modules.data_ingestion.pnds import pnds_to_csv
from modules.preprocess.preprocess_pnds import preprocess_pnds
from modules.deduplication.deduplicate_pnds import deduplicate_pnds
from modules.linkage.sed_to_pnds import sed_to_pnds

# convert pnds to csv
# pnds_to_csv()

# create a standardized pnds subset
# preprocess_pnds()

# cluster duplicates in preprocessed pnds
deduplicate_pnds()

# merge sed age onto pnds
sed_to_pnds()
