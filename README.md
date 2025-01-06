

## SetUp

### Download raw data

Call a function `get_crsp_data()` which creates a `data/crsp/` folder and downloads the raw data from the CRSP database using the `get_wrds_table()` function:

- MSF
- MSFHDR
- MSEDELIST
- MSEEXCHDATES
- CCMXPF_LNKHIST
- STOCKNAMES

The MSF dataset is the main dataset from the CRSP monthly data. The MSEDELIST dataset has delisting returns. The rest are used for identifying information and merges wiht COMPUSTAT.