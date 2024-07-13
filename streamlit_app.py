import streamlit as st
import pymongo
import pandas as pd




def get_data_from_mongo():
    # Replace with your MongoDB connection string
    connection_string = "mongodb+srv://jadoo_shivam:211101@sharemarket.q0zep9q.mongodb.net/"
    client = pymongo.MongoClient(connection_string)
    
    # Replace 'your_database' and 'your_collection' with actual database and collection names
    db = client['jadoo_data']
    collection = db['Data']
    
    # Fetch data from MongoDB collection into a DataFrame
    cursor = collection.find({})
    df = pd.DataFrame(list(cursor))
    
    return df
def main():
    st.title('MongoDB Data Viewer')
    
    # Connect to MongoDB and fetch data
    df = get_data_from_mongo()
    
    # Display the DataFrame
    st.write("### Data from MongoDB:")
    st.write(df)

if __name__ == "__main__":
    main()
