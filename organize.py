import pandas as pd
import re
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords


# Getting specific columns
df = pd.read_csv("rows.csv", usecols = ['Product','Issue','Company','State','Complaint ID','ZIP code'])

# Removing Null values
df = df.dropna(axis=0)

# Removing punctuation
df['Product']=[re.sub('[^\w\s]+', '', s) for s in df['Product'].tolist()]
df['Issue']=[re.sub('[^\w\s]+', '', s) for s in df['Issue'].tolist()]
df['Company']=[re.sub('[^\w\s]+', '', s) for s in df['Company'].tolist()]

#Removing stop words
stop_words = stopwords.words('english')
df['Product'] = df['Product'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop_words)]))
df['Issue'] = df['Issue'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop_words)]))
df['Company'] = df['Company'].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop_words)]))


print(df)

# Transfering data to another csv file
df.to_csv('clrdata.csv', index=False)

data = pd.read_csv("clrdata.csv")
print(data)
