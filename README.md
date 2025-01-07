

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

### Organize and store

The function `make_crsp_monthly_data()` reads in and stores the raw CRSP data and creates the matrices that we'll use for asset pricing later. Most of our variables of intereset will be stored as matrices with the same dimensions: number of dates (nMonths or nDays) \times number of stocks (nStocks). The dimensions will be determined by the number of unique permnos in the CRSP MSF and dates in the MSF/DSF files after filtering based on sample start and end dates and the flag for domestic common equity. The function creates and stores the dates (nMonths \times 1) and CRSP's permno identifier (nStocks \times 1) vecotrs which contain the unique months and permnos, as well as the following matrices (all nMonths \times nStocks):
- shrcd: Share Code
- exchcd: Exchange Code
- siccd: Standard Industrial Classification Code
- prc: Price 
- bid: closing bid price
- ask: closing ask price
- bidlo: low bid price
- askhi: high ask price
- vol: monthly share volumne (in hundreds)
- ret_x_dl: holding period return without adjusting for delisting returns
- shrout: shares outstanding (in thousands)
- cfacpr: cumulative factor to adjust price
- cfashr: cumulative factor to adjust shares outstanding
- spread: realized closing bid-ask spread
- retx: holding period return without dividends and without adjusting for delisting returns