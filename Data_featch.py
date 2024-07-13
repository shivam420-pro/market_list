import re
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from mongo_utils import connect_to_mongo, store_data_in_mongo

def scrape_table_data(base_url, max_pages, table_class='data-table'):
    data = []
    page_number = 1

    while page_number <= max_pages:
        url = "{}?page={}".format(base_url, page_number)
        print("Fetching data from: {}".format(url))
        response = requests.get(url)

        if response.status_code != 200:
            print("Failed to retrieve {}. Status code: {}".format(url, response.status_code))
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', class_=table_class)

        if not table:
            print("No table found on page {}. Exiting loop.".format(page_number))
            break

        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all(['td', 'th'])
            cols = [col.text.strip() for col in cols]
            if cols:  # Check if row is not empty
                data.append(cols)

        # Move to the next page
        page_number += 1

    if data:
        headers = data[0]  # Assuming the first row contains headers
        df = pd.DataFrame(data[1:], columns=headers)
        return df
    else:
        print("No data extracted.")
        return pd.DataFrame()  # Return an empty DataFrame if no data

# Example usage:
if __name__ == "__main__":
    base_url = "https://www.screener.in/screens/638306/all-nse-stocks/"
    max_pages = 190
    df = scrape_table_data(base_url, max_pages)

    if not df.empty:
        print(df.head())  # Print the first 5 rows

        # MongoDB connection details
        connection_string = "mongodb+srv://jadoo_shivam:211101@sharemarket.q0zep9q.mongodb.net/"
        db_name = "jadoo_data"
        collection_name = "Data"

        # Connect to MongoDB
        collection = connect_to_mongo(connection_string, db_name, collection_name)

        # Store data in MongoDB
        store_data_in_mongo(df, collection)
    else:
        print("No data to display.")
