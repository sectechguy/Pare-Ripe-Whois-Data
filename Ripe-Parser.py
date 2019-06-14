#Import libraries
import pandas as pd

#Read in APNIC data
ripe = pd.read_table('/home/cdsw/BulkWhois/RIPE-NCC/ripe.db.inetnum')

# Clean the data
ripe.columns = ['data'] #Rename column
ripe = ripe[ripe.data.str.contains("remarks") == False]  #Remove all rows where string contains "remarks" or "%"
ripe = ripe[ripe.data.str.contains("%") == False]  #Remove all rows where string contains "remarks" or "%"
inetnum_loc = ripe.data.str.contains('inetnum').idxmax()  #Find the first occurance of partial string "inetnum"
ripe = ripe[inetnum_loc:]  #Use every row after and including "inetnum"
ripe = ripe.reset_index(drop=True)  #Reset index

# Parse the data
ripe = ripe['data'].str.split(': ', 1, expand=True)  #Split into two columns
ripe = ripe.set_index([0,(ripe[0] == 'inetnum').cumsum().rename('row')])  #Add new column and seperate into rows where inetnum is seen
ripe = ripe.set_index(ripe.groupby([0,'row']).cumcount(), append=True)  #Group by distinct row name
ripe = ripe.reset_index('row')  #Reset index on row - move up to index row
ripe.index = ripe.index.map('{0[0]}_{0[1]}'.format)  #Reset index and group by row column
ripe = ripe.set_index(['row'], append=True)[1].unstack(0)
ripe = ripe.rename(columns=lambda x: x.split('_0')[0]).reset_index()

# Write out to csv
ripe.to_csv('/home/cdsw/BulkWhois/RIPE-NCC/parsedRIPE.csv', index=False, header=True)
