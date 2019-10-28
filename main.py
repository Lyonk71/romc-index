# import modules
from modules.data_ingestion.pnds import pnds_to_csv
from modules.preprocess.preprocess_pnds import preprocess_pnds
from modules.deduplication.deduplicate_pnds import deduplicate_pnds
from modules.linkage.sed_to_pnds import sed_to_pnds
from modules.preprocess.preprocess_pcmh import preprocess_pcmh
from modules.linkage.pcmh_to_pnds import pcmh_to_pnds

##  # convert pnds to csv
##  print('section 1')
##  pnds_to_csv()
##  # create & preprocess pnds subset
##  print('section 2')
##  preprocess_pnds()
##  # cluster duplicates in preprocessed pnds
##  print('section 3')
##  deduplicate_pnds()
##  # merge sed age onto pnds
##  print('section 4')
##  sed_to_pnds()
# preprocess pcmh
print('section 5')
preprocess_pcmh()
# merge pcmh onto pnds
print('section 6')
pcmh_to_pnds()

#
