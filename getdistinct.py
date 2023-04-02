import pandas as pd

df = pd.read_csv("clrdata.csv")

distinctproducts = df['Product'].unique()
distinctissues = df['Issue'].unique()
distinctcompanies = df['Company'].unique()
distinctstates = df['State'].unique()
distinctids = df['Complaint ID'].unique()
distinctzipcodes = df['ZIP code'].unique()


# pad the arrays with nan
def padarray(array, length):
    array = list(array)
    while len(array) < length:
        array.append(None)
    return array


length = max(len(distinctproducts), len(distinctissues), len(distinctcompanies), len(distinctstates), len(distinctids),
             len(distinctzipcodes))
distinctproducts = padarray(distinctproducts, length)
distinctissues = padarray(distinctissues, length)
distinctcompanies = padarray(distinctcompanies, length)
distinctstates = padarray(distinctstates, length)
distinctids = padarray(distinctids, length)
distinctzipcodes = padarray(distinctzipcodes, length)

# create distincts file
distinctdf = pd.DataFrame(
    {'Product': distinctproducts, 'Issue': distinctissues, 'Company': distinctcompanies, 'State': distinctstates,
     'Complaint ID': distinctids, 'ZIP code': distinctzipcodes})
distinctdf.to_csv('distinct.csv', index=False)
print('Distinct file created')