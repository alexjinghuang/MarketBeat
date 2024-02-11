import pandas as pd
import numpy as np
from web_scraper import CustomScraper
import time
import tqdm

def preprocess_data(data):
    NUM_ROWS = 2500 #number of rows to process
    df = pd.read_csv(data, nrows=NUM_ROWS)
    df.drop_duplicates(subset=['url'], inplace=True)
    scraper = CustomScraper()
    prices = []
    rows_to_drop = []
    for i, row in tqdm.tqdm(df.iterrows(), total=NUM_ROWS, desc="Processing rows"):
        url = row['url']
        info = scraper.get_detail(url) #calls on web scraper to get information on item
        if info is None:
            rows_to_drop.append(i)
            prices.append(np.nan)
            continue
        item_price = info['price']
        prices.append(item_price)
        if (i + 1) % 50 == 0:
            temp = prices + [np.nan] * (NUM_ROWS - len(prices))
            df2 = df.assign(price=temp)
            df2 = df2.drop(rows_to_drop) #dropping rows with invalid links
            rows_to_drop.clear()
            df2.to_csv('out.csv', index=False)
    df2 = df.assign(price=prices)
    df2 = df2.drop(rows_to_drop)
    rows_to_drop.clear() #clears rows_to_drop list
    df2.to_csv('out.csv', index=False)

df = pd.read_csv('complete_data.csv')
print(len(df))
df.drop_duplicates(subset=['url'], inplace=True)
print(len(df))
df.to_csv('complete_data.csv', index=False)
#preprocess_data('clean.csv')
