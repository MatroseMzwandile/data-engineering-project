import logging
from datetime import datetime

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print('Keyspace created successfully!')


def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT
            )
        """)
    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address,
                postcode, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
                postcode, email, username, dob, registered_date, phone, picture))
        loggin.info(f"Data inserted for {first_name} {last_name}")
    except Exception as e:
        logging.error(f"Could not insert data due to : {e}")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = SparkSession.builder\
                    .appName('SparkDataStreaming') \
                    .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.13:3.41',
                            'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1') \
                    .config('spark.cassandra.connection.host', 'localhost') \
                    .getOrCreate()
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception : {e}")
    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('subscribe', 'users_created') \
                .option('startingOffsets', 'earliest') \
                .load()
        logging.info("kafka dataframe created succesfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")
    return spark_df


def create_cassandra_connection():
    cas_session = None
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()

    except Exception as e:
        logging.error("Could not create cassandra connection due to : {e}")
        return None
    return cas_session

if __name__ == '__main__':
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        df = connect_to_kafka(spark_conn)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)
            # insert_data(session)
