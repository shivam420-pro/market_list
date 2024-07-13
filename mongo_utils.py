from pymongo import MongoClient
import pandas as pd

def connect_to_mongo(connection_string, db_name, collection_name):
    """Connect to MongoDB and return the collection."""
    client = MongoClient(connection_string)
    db = client[db_name]
    collection = db[collection_name]
    return collection

def store_data_in_mongo(df, collection):
    """Store the DataFrame data into the specified MongoDB collection."""
    # Convert DataFrame to dictionary records and insert into MongoDB
    records = df.to_dict('records')
    print(records)
    collection.insert_many(records)
    print("Data inserted successfully into MongoDB.")


def get_data_from_mongo(connection_string, db_name, collection_name):
    client = MongoClient.MongoClient(connection_string)
    db = client[db_name]
    collection = db[collection_name]

    # Fetch data from MongoDB collection into a DataFrame
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    return df