from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import csv
from census.constant import *
from census.util.util import read_yaml_file

file_path = r"E:\Machine_Learning_Project\config\config.yaml"

db_credentials = read_yaml_file(file_path=file_path)

client_id = db_credentials[DATA_INGESTION_CONFIG_KEY][DB_CLIENT_ID_KEY]
client_secret = db_credentials[DATA_INGESTION_CONFIG_KEY][DB_CLIENT_SECRET_KEY]

# Set up the Cassandra cluster and authentication
cloud_config = {'secure_connect_bundle': r'E:\Machine_Learning_Project\secure-connect-adult-census.zip'}
auth_provider = PlainTextAuthProvider(client_id, client_secret)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()


# Set the keyspace
session.set_keyspace('income')

# Set the table name and CSV file name
table_name = 'Census'
csv_file = r'E:\Machine_Learning_Project\Data File\adult.csv'

# Read the CSV file and insert the data into Cassandra
with open(csv_file) as file:
    csv_reader = csv.reader(file)
    next(csv_reader)  # Skip the header row
    for row in csv_reader:
        query = SimpleStatement(f"INSERT INTO {table_name} (age, workclass, fnlwgt, education, education-num,\
       marital-status, occupation, relationship, race, sex,\
       capital-gain, capital-loss, hours-per-week, country, salary) VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s)")
        session.execute(query, (int(row[0]), row[1], int(row[2]), row[3], int(row[4]), row[5],row[6],row[7],row[8],row[9], int(row[10]),int(row[11]) ,int(row[12]), row[13], row[14]))

# Close the Cassandra session and cluster
session.shutdown()
cluster.shutdown()
