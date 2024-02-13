import json
import mysql.connector
from cassandra.cluster import Cluster
from pymongo import MongoClient
import uuid
from cassandra.query import SimpleStatement,BoundStatement
from datetime import datetime
import csv

def process_data(data, collection, cursor, cassandra_session, dataset, mysql_conn):
    
    for item in data:

        # Insert for MongoDB
        collection.insert_one(item)

        # Perform operations specific to the "BestBuy" dataset
        if dataset == "BestBuy":            
            # MySQL
            insert_query = "INSERT INTO Product (category, name_of_product, price, saving, rating, reviews) VALUES (%s, %s, %s, %s, %s, %s)"
            values = (
                item["category"],
                item.get("name_of_product", None),
                float(item["price"]),
                item.get("saving", None),
                float(item.get("rating")),
                item.get("reviews", None)
            )
            values = [str(value) if isinstance(value, list) else value for value in values]
            cursor.execute(insert_query, values)
            mysql_conn.commit()
            
            # Cassandra
            insert_query = """
            INSERT INTO bestbuy.products (product_id, category, name_of_product, price, saving, rating, reviews)
            VALUES (?, ?, ?, ?, ?, ?, ?)"""
            cassandra_query = cassandra_session.prepare(insert_query)
            product_id = uuid.uuid4()
            cassandra_session.execute(cassandra_query, (
                product_id,
                item["category"],
                item["name_of_product"],
                float(item["price"]),
                item["saving"],
                float(item["rating"]),
                item["reviews"]
            ))
            
        elif dataset == "Reddit":
            # Perform operations specific to the "Reddit" dataset
            
            # MySQL
            insert_query = "INSERT INTO Articles (title, poster, time, subreddit, initial_post) VALUES (%s, %s, %s, %s, %s)"
            values = (
                item["title"],
                item.get("poster", None),
                item.get("time", None),
                item.get("subreddit", None),
                item.get("initial_post", None)
            )
            values = [str(value) if isinstance(value, list) else value for value in values]
            cursor.execute(insert_query, values)
            mysql_conn.commit()
            
            # Cassandra
            insert_query = """
            INSERT INTO reddit.articles (article_id, title, poster, time, subreddit, initial_post)
            VALUES (?, ?, ?, ?, ?, ?)"""
            cassandra_query = cassandra_session.prepare(insert_query)
            article_id = uuid.uuid4()
            values = (
                article_id,
                item["title"],
                item.get("poster", None),
                item.get("time", None),
                item.get("subreddit", None),
                item.get("initial_post", None)
            )
            values = [str(value) if isinstance(value, list) else value for value in values]
            cassandra_session.execute(cassandra_query, values)


def search_data(dataset, cursor, collection, cassandra_session):
    if dataset == "Reddit":
        # Perform SQL search for Reddit
        cursor.execute(f"SELECT * FROM Articles WHERE subreddit LIKE '%MachineLearning%'")
        result_sql = cursor.fetchall()
        
        # Perform MongoDB search for Reddit
        result_mongo = list(collection.find({"subreddit": {"$regex": f'.*artificial.*'}}))
        
        # Perform Cassandra search for Reddit
        cassandra_search_query = f"SELECT * FROM Reddit.articles WHERE subreddit = 'r/explainlikeimfive' ALLOW FILTERING"
        result_cassandra = list(cassandra_session.execute(cassandra_search_query))
        
        return result_sql, result_mongo, result_cassandra

    elif dataset == "BestBuy":
        # Perform SQL search for BestBuy
        cursor.execute(f"SELECT * FROM Product WHERE category LIKE '%Mobile%'")
        result_sql = cursor.fetchall()
        
        # Perform MongoDB search for BestBuy
        result_mongo = list(collection.find({"category": {"$regex": f'.*Computers.*'}}))
        
        # Perform Cassandra search for BestBuy
        cassandra_search_query = f"SELECT * FROM bestbuy.products WHERE category = 'TV & Home Theatre' ALLOW FILTERING"
        result_cassandra = list(cassandra_session.execute(cassandra_search_query))
        
        return result_sql, result_mongo, result_cassandra

# Define a function to calculate average price in MongoDB and Cassandra for BestBuy
def calculate_average_price(mongo_collection, mysql_cursor, cassandra_session):
    mongo_pipeline = [
        {"$group": {"_id": "$category", "avg_price": {"$avg": "$price"}}}
    ]
    average_price_mongo = list(mongo_collection.aggregate(mongo_pipeline))

    mysql_cursor.execute("SELECT category, AVG(price) FROM Product GROUP BY category")
    average_price_mysql = mysql_cursor.fetchall()

    cassandra_avg_query = "SELECT category, AVG(price) FROM bestbuy.products GROUP BY category"
    average_price_cassandra = cassandra_session.execute(cassandra_avg_query)

    return average_price_mongo, average_price_mysql, list(average_price_cassandra)

data_collections = [
    {
        "schema": "BestBuy",
        "collection" : "Product",
        "file": "BestBuyOutput.json"
    },
    {
        "schema": "Reddit",
        "collection" : "Articles",
        "file": "RedditOutput.json"
    }
]

for data_collection in data_collections:
    schema = data_collection["schema"]
    collection_name = data_collection["collection"]
    json_file = data_collection["file"]

    # Connect to MongoDB
    mongo_client = MongoClient(host="localhost", port=27017, username="root", password="example")
    mongo_db = mongo_client[schema]
    mongo_collection = mongo_db[collection_name]

    # Connect to MySQL
    mysql_conn = mysql.connector.connect(
        host="127.0.0.1",
        user="root",
        password="AMOD5410H",
        database=schema
    )
    mysql_cursor = mysql_conn.cursor()

    #Connect to Cassandra
    cassandra_cluster = Cluster(['localhost'], port=9042)
    cassandra_session = cassandra_cluster.connect(schema.lower())

    # Load data from the JSON file
    with open(json_file) as file:
        data = json.load(file)

    # Process data for the dataset
    process_data(data, mongo_collection, mysql_cursor, cassandra_session, schema, mysql_conn)

    # Search data using the common method
    if schema == "Reddit":
        sql_result, mongo_result, cassandra_result = search_data(schema, mysql_cursor, mongo_collection, cassandra_session)
        # Print the search results
        print("\n SQL Result:")
        for row in sql_result[:2]:
            print(row)

        print("\nMongoDB Result:")
        for item in mongo_result[:2]:
            print(item)

        print("\nCassandra Result:")
        for row in cassandra_result[:2]:
            print(row)

    # Calculate average price in MongoDB and Cassandra for BestBuy
    if schema == "BestBuy":
        average_price_mongo, average_price_mysql, average_price_cassandra = calculate_average_price(mongo_collection, mysql_cursor, cassandra_session)

print("\nSQL Average Price:")
for category, avg_price in average_price_mysql:
    print(f"{category}: {avg_price}")

print("\nMongoDB Average Price:")
for result in average_price_mongo:
    category = result['_id']
    avg_price = result['avg_price']
    print(f"{category}: {avg_price}")

print("\nCassandra Average Price:")
for row in average_price_cassandra:
    category = row.category
    avg_price = row.system_avg_price
    print(f"{category}: {avg_price}")


# Close database connections
mongo_client.close()
mysql_conn.close()
cassandra_cluster.shutdown()
