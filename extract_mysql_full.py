# from configparser import ConfigParser
import configparser
import pymysql
import csv
import boto3
file="pipeline.conf"
config=configparser.ConfigParser()
config.read(file)
print(config.sections()) 
# Read the Mysql configuration values from a configuration file from pipeline.conf
hostname=config.get("mysql_config","hostname")
port=config.get("mysql_config","port")
username=config.get("mysql_config","username")
password=config.get("mysql_config","password")
database=config.get("mysql_config","database")

#connect to mysql using the above configuration 
connection = pymysql.connect(
    host=hostname,
    port=int(port),
    user=username,
    database=database,
    password=password
    )

if connection is None:
    print("error connecting")

else:
    print("Congrates MySQL connection established \n")

# Retrieve data from MySQL
query="SELECT * from orders"

# To query or ask the database for a specific data you'll need cursor.And a cursor is an object from the pymysql library 
# that is used to execute the SELECT query:
cursor=connection.cursor()
cursor.execute(query)

# To retrieve the data from the database you'll nedd the fetchall object
result=cursor.fetchall()
# print(result) #optional print result to make sure you've retrievable 

# Add your fetch data to a csv file
csv_file = "extracted_order.csv"
with open(csv_file,'w') as file: # open the file as write file
    file_writer=csv.writer(file, delimiter='|')  # by default delimiter will be a comma unless specified like we did
    file_writer.writerows(result)


#closing everything you opened 
cursor.close()
connection.close()

# Read the s3 configuration values from a configuration file from pipeline.conf
config = configparser.ConfigParser()
config.read('pipeline.conf')

access_key = config["aws_boto_credentials"]["access_key"]
secret_key=config["aws_boto_credentials"]["secret_key"]
bucket_name=config["aws_boto_credentials"]["bucket_name"]

#connect to s3 bucket
# To use boto3 you'll need a to pip install boto3
# boto3 helps you connect your Amazone S3 bucket 
s3 = boto3.client('s3',
                   aws_access_key_id=access_key,
                   aws_secret_access_key=secret_key,
                   
                        )

s3_file_name = csv_file


# Upload the data to S3
s3.upload_file(csv_file,bucket_name,s3_file_name)
if s3 is not None:
    print("Your file have uploaded successful into s3 bucket")


