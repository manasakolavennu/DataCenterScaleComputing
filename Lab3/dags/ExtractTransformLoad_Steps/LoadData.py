import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine
import csv


class GCPDataLoader:

    def __init__(self):
        self.bucket_name = 'data_center_lab3'

    def getcredentials(self):
        credentials = {
                          "type": "service_account",
                          "project_id": "winter-joy-406123",
                          "private_key_id": "d74d0686045c6d771d03cb9a6a3568b006e9a0cc",
                          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC1tganLp7yA2L3\nz4jw8PWgqam8PA3XQyUT0JVaRdirnbXCh/G3THIwOIorNZmOsP7yWpucLcz0hzcF\nnNcoa0AX+DpIhpXfppaEVtmx4c0hHKKGUyHrk+zBspAaDYAM30YyIjUAvAO9RvLq\nMkr6hpJ5/EUPm2py3obgslAclQN7f4BiQj+CvOJb+JDmuG2x1oZ1mhE6B26G7Uwd\nnHqQLV/q6q0nUbz5y07/TZuN52V15wUJ6S3Ue1BaDYJKTEs8hrkDkyw33su3uCaH\nBNceBOwHn5fR+GHWcog9HAmArPaksVvXclqt0dDBKa9FkWHDNxdPK9Brdn13TPnR\nPc7prBxzAgMBAAECggEABqW9kZEhjG32SWWEt3fBLJr4VCQIR5czvIlVZWOHvSN8\nWjz56UA5Ly1qVJFV6EPuV7Rb2/dK96kYqLJnpplh016n1y5hPjjMadP5i8ncZLk0\n8uAIriMPtrhPEDztMctbOItK0BeQtXRqf4nOd2LD6gWCC0OevcwJOCAd0SXcZkCg\ni2yUkirTcsqnv23TL6al63Meemqwr1icafEA3hIpvn3KEhpRA7uJMkAdU07RSCYj\njsg+WNmzP0IzbY0i/aOeUPeVoB2576LTN3i/FdWMikxu6vlaUWMuMAOwf9zizHwi\nzawlol3i03QQfxFMPgI5qlwe/8qwOrQcUu/Rdyw2hQKBgQDch6Yia5WiFdN8YKfu\nCZCZ7pEOe1MKtKxj7g8KHipxufPhUc5XwAG2WbE3EcW/uJ0Iqz2d7BxqAjezsp2P\nUWEEjRNscbEO8f5shnPmHi5lIxugFYH3k8mv5ZjjifGEdHF/1n8U1ZZzRCM/YyJM\n3sWIb1RzVoXTMuLdp+19WNZypQKBgQDS8AFkmu2hzZTfVtxg9hQawHpBoh+jMKq/\n3xqsZKk0D5emhtR7DnvQ8RHXENuXb0vVFX7BSE47ZWbKnWNl03v8ebaEK6/XNu+B\nRX7Id/LjzkFQsSsorFEDI5P23oGAdQV5d9pOtKw1dryorR7OEN3dnS5nEyiB+Foy\neQ/IJMGfNwKBgQDQil4qcn4/llA1b9mdmeHqDtWRUkHG/+99WCNUuA3/GY9sZUWx\naVq1K8APiXjswhGNnxFXg22jOZGfFqs0WgpamWXiyOhcb67exY5X7/aDoV2AVpZe\nnpy8/2tC0LFZRhwGfboS45+wRKDoUkCfXJKDYHQF1a4beCVc4m4MeLPiGQKBgQDP\n1OHKekvArIoOM8sXTd4pLZRHrrF1XLIgMnZZfSSpwuMslJQuWurrx1pIiLeT0Xjq\nDi/ByLgsFZDd+YzB+0miTVnjiBfM+LeqqwpsAqMyiToZgzZ+8KkxapCTIFCAfMxU\nDh7uhV1XoBHqMAi2CDBR9liN/nZe+JAGQvmlvXF4qQKBgHg9nog6zaYVul59LLw7\nUgCnwFhdMf+0K9AZPEyMv93oHnyE3bGS4N8NxkSgDGFbwl/NVQNethv5b5IvwAV0\nfZwVnyX+i5+DCssNnI3v65U6alc719Y8X0celgO3284n59SwpHB9Gpj/qtuWmLq4\n3xpgP4EZ+QYx6vp159YF04tP\n-----END PRIVATE KEY-----\n",
                          "client_email": "manasak@winter-joy-406123.iam.gserviceaccount.com",
                          "client_id": "110599242930579426630",
                          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                          "token_uri": "https://oauth2.googleapis.com/token",
                          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/manasak%40winter-joy-406123.iam.gserviceaccount.com",
                          "universe_domain": "googleapis.com"
                        }
        return credentials

    def connect_to_gcp_and_get_data(self, file_name):
        gcp_file_path = f'transformed_data/{file_name}'

        credentials_info = self.getcredentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

        # Read the CSV file from GCP into a DataFrame
        blob = bucket.blob(gcp_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def get_data(self, file_name):
        df = self.connect_to_gcp_and_get_data(file_name)
        return df


class PostgresDataLoader:

    def __init__(self):
        self.db_config = {
            'dbname': 'shelter_outcomes_db',
            'user': 'postgres',
            'password': 'pgadmin',
            'host': '34.27.8.179',
            'port': '5432',
        }

    def get_queries(self, table_name):
        
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"

        if table_name =="animaldimension":
            query = """CREATE TABLE IF NOT EXISTS animaldimension (
                            animal_key INT PRIMARY KEY,
                            animal_id VARCHAR,
                            name VARCHAR,
                            dob DATE,
                            reprod VARCHAR,
                            gender VARCHAR, 
                            animal_type VARCHAR NOT NULL,
                            breed VARCHAR,
                            color VARCHAR,
                            datetime TIMESTAMP
                        );
                        """
            alter_table_query = """ALTER TABLE animaldimension
                                ADD CONSTRAINT animal_key_unique UNIQUE (animal_key);
                                """
        elif table_name =="outcomedimension":
            query = """CREATE TABLE IF NOT EXISTS outcomedimension (
                            outcome_type_key INT PRIMARY KEY,
                            outcome_type VARCHAR NOT NULL
                        );
                        """
            alter_table_query = """ALTER TABLE outcomedimension
                                ADD CONSTRAINT outcometype_key_unique UNIQUE (outcome_type_key);
                                """
        elif table_name =="datedimension":
            query = """CREATE TABLE IF NOT EXISTS datedimension (
                            date_key INT PRIMARY KEY,
                            year_recorded INT2  NOT NULL,
                            month_recorded INT2  NOT NULL
                        );
                        """
            alter_table_query = """ALTER TABLE datedimension
                                ADD CONSTRAINT date_key_unique UNIQUE (date_key);
                                """
        else:
            query = """CREATE TABLE IF NOT EXISTS outcomesfact (
                            outcome_id SERIAL PRIMARY KEY,
                            animal_key INT,
                            date_key INT,
                            outcome_type_key INT,
                            FOREIGN KEY (animal_key) REFERENCES animaldimension(animal_key),
                            FOREIGN KEY (date_key) REFERENCES datedimension(date_key),
                            FOREIGN KEY (outcome_type_key) REFERENCES outcomedimension(outcome_type_key)
                        );
                        """
            alter_table_query = ";"
        return f"{drop_table_query}\n{query}\n{alter_table_query}".strip()
        #return f"{query}"

    def connect_to_postgres(self):
        connection = psycopg2.connect(**self.db_config)
        return connection

    def create_table(self, connection, table_query):
        print("Executing Create Table Queries...")
        cursor = connection.cursor()
        cursor.execute(table_query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def load_data_into_postgres(self, connection, gcp_data, table_name):
        #cursor = connection.cursor()
        #print(f"Dropping Table {table_name}")
        #truncate_table = f"DROP TABLE {table_name};"
        #cursor.execute(truncate_table)
        #connection.commit()
        #cursor.close()
        
        print(f"Loading data into PostgreSQL for table {table_name}")
        # Specify the PostgreSQL engine explicitly
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        # Write the DataFrame to PostgreSQL using the specified engine
        gcp_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Number of rows inserted for table {table_name}: {len(gcp_data)}")
        
#def load_data_to_postgres(file_name, table_name):
#    gcp_loader = GCPDataLoader()
#    table_data_df = gcp_loader.get_data(file_name)

#    postgres_dataloader = PostgresDataLoader()
#    table_query = postgres_dataloader.get_queries(table_name)
#    postgres_connection = postgres_dataloader.connect_to_postgres()

#    postgres_dataloader.create_table(postgres_connection, table_query)
#    postgres_dataloader.load_data_into_postgres(postgres_connection, table_data_df, table_name)
    
def drop_existing_tables():
    postgres_dataloader = PostgresDataLoader()
    connection = postgres_dataloader.connect_to_postgres()
    drop_tables_query = """
        -- Drop tables if they exist
        DROP TABLE IF EXISTS outcomesfact CASCADE;
        DROP TABLE IF EXISTS animaldimension CASCADE;
        DROP TABLE IF EXISTS datedimension CASCADE;
        DROP TABLE IF EXISTS outcomedimension CASCADE;
        """
    with connection.cursor() as cursor:
            cursor.execute(drop_tables_query)
    connection.commit()


def get_gcs_credentials():
    credentials_info = {
                          "type": "service_account",
                          "project_id": "winter-joy-406123",
                          "private_key_id": "d74d0686045c6d771d03cb9a6a3568b006e9a0cc",
                          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC1tganLp7yA2L3\nz4jw8PWgqam8PA3XQyUT0JVaRdirnbXCh/G3THIwOIorNZmOsP7yWpucLcz0hzcF\nnNcoa0AX+DpIhpXfppaEVtmx4c0hHKKGUyHrk+zBspAaDYAM30YyIjUAvAO9RvLq\nMkr6hpJ5/EUPm2py3obgslAclQN7f4BiQj+CvOJb+JDmuG2x1oZ1mhE6B26G7Uwd\nnHqQLV/q6q0nUbz5y07/TZuN52V15wUJ6S3Ue1BaDYJKTEs8hrkDkyw33su3uCaH\nBNceBOwHn5fR+GHWcog9HAmArPaksVvXclqt0dDBKa9FkWHDNxdPK9Brdn13TPnR\nPc7prBxzAgMBAAECggEABqW9kZEhjG32SWWEt3fBLJr4VCQIR5czvIlVZWOHvSN8\nWjz56UA5Ly1qVJFV6EPuV7Rb2/dK96kYqLJnpplh016n1y5hPjjMadP5i8ncZLk0\n8uAIriMPtrhPEDztMctbOItK0BeQtXRqf4nOd2LD6gWCC0OevcwJOCAd0SXcZkCg\ni2yUkirTcsqnv23TL6al63Meemqwr1icafEA3hIpvn3KEhpRA7uJMkAdU07RSCYj\njsg+WNmzP0IzbY0i/aOeUPeVoB2576LTN3i/FdWMikxu6vlaUWMuMAOwf9zizHwi\nzawlol3i03QQfxFMPgI5qlwe/8qwOrQcUu/Rdyw2hQKBgQDch6Yia5WiFdN8YKfu\nCZCZ7pEOe1MKtKxj7g8KHipxufPhUc5XwAG2WbE3EcW/uJ0Iqz2d7BxqAjezsp2P\nUWEEjRNscbEO8f5shnPmHi5lIxugFYH3k8mv5ZjjifGEdHF/1n8U1ZZzRCM/YyJM\n3sWIb1RzVoXTMuLdp+19WNZypQKBgQDS8AFkmu2hzZTfVtxg9hQawHpBoh+jMKq/\n3xqsZKk0D5emhtR7DnvQ8RHXENuXb0vVFX7BSE47ZWbKnWNl03v8ebaEK6/XNu+B\nRX7Id/LjzkFQsSsorFEDI5P23oGAdQV5d9pOtKw1dryorR7OEN3dnS5nEyiB+Foy\neQ/IJMGfNwKBgQDQil4qcn4/llA1b9mdmeHqDtWRUkHG/+99WCNUuA3/GY9sZUWx\naVq1K8APiXjswhGNnxFXg22jOZGfFqs0WgpamWXiyOhcb67exY5X7/aDoV2AVpZe\nnpy8/2tC0LFZRhwGfboS45+wRKDoUkCfXJKDYHQF1a4beCVc4m4MeLPiGQKBgQDP\n1OHKekvArIoOM8sXTd4pLZRHrrF1XLIgMnZZfSSpwuMslJQuWurrx1pIiLeT0Xjq\nDi/ByLgsFZDd+YzB+0miTVnjiBfM+LeqqwpsAqMyiToZgzZ+8KkxapCTIFCAfMxU\nDh7uhV1XoBHqMAi2CDBR9liN/nZe+JAGQvmlvXF4qQKBgHg9nog6zaYVul59LLw7\nUgCnwFhdMf+0K9AZPEyMv93oHnyE3bGS4N8NxkSgDGFbwl/NVQNethv5b5IvwAV0\nfZwVnyX+i5+DCssNnI3v65U6alc719Y8X0celgO3284n59SwpHB9Gpj/qtuWmLq4\n3xpgP4EZ+QYx6vp159YF04tP\n-----END PRIVATE KEY-----\n",
                          "client_email": "manasak@winter-joy-406123.iam.gserviceaccount.com",
                          "client_id": "110599242930579426630",
                          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                          "token_uri": "https://oauth2.googleapis.com/token",
                          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/manasak%40winter-joy-406123.iam.gserviceaccount.com",
                          "universe_domain": "googleapis.com"
                        }
    return credentials_info


def read_data_from_gcs(bucket_name, file_path):
    credentials_info = get_gcs_credentials()
    print(credentials_info)
    client = storage.Client.from_service_account_info(credentials_info)
    bucket = client.bucket(bucket_name)

    #blobs = bucket.list_blobs()
    blob = bucket.blob(file_path)
    data = blob.download_as_text()
    return data



def create_table(connection, table_name):

    create_tables_query = " "
    drop_tables_query = " "
    if table_name =="animaldimension":
        create_tables_query = """
        
        CREATE TABLE IF NOT EXISTS animaldimension (
            
            animal_key INT PRIMARY KEY,
            animal_id VARCHAR,
            name VARCHAR,
            dob DATE,
            reprod VARCHAR,
            gender VARCHAR, 
            animal_type VARCHAR NOT NULL,
            breed VARCHAR,
            color VARCHAR,
            datetime TIMESTAMP
        );
            """
        
        drop_tables_query = drop_tables_query = """
        -- Drop tables if they exist
        
        DROP TABLE IF EXISTS animaldimension CASCADE;
        
        """
    elif table_name =="outcomedimension":
        create_tables_query = """
        CREATE TABLE IF NOT EXISTS outcomedimension (
            outcome_type_key INT PRIMARY KEY,
            outcome_type VARCHAR NOT NULL
        );
            """
        
        drop_tables_query = """
        -- Drop tables if they exist
        
        DROP TABLE IF EXISTS outcomedimension CASCADE;

        """
    elif table_name=="datedimension":
        create_tables_query = """
        CREATE TABLE IF NOT EXISTS datedimension (
            date_key INT PRIMARY KEY,
            year_recorded INT2  NOT NULL,
            month_recorded INT2  NOT NULL
        );
            """
        drop_tables_query = """
        -- Drop tables if they exist
        
        DROP TABLE IF EXISTS datedimension CASCADE;
        
        """  
    else:
        create_tables_query = """
        CREATE TABLE IF NOT EXISTS outcomesfact (
            outcome_id SERIAL PRIMARY KEY,
            animal_key INT,
            date_key INT,
            outcome_type_key INT,
            FOREIGN KEY (animal_key) REFERENCES animaldimension(animal_key),
            FOREIGN KEY (date_key) REFERENCES datedimension(date_key),
            FOREIGN KEY (outcome_type_key) REFERENCES outcomedimension(outcome_type_key)
        );
            """
        drop_tables_query = """
        -- Drop tables if they exist

        DROP TABLE IF EXISTS outcomesfact CASCADE;
        
        """   


    
    with connection.cursor() as cursor:
        cursor.execute(drop_tables_query)
        cursor.execute(create_tables_query)
    connection.commit()    


def load_data_into_table(connection, table_name, data):
    

    queries_string = " "
    if table_name =="animaldimension":
        animal_dim_data_csv = csv.reader(StringIO(data))
        next(animal_dim_data_csv)
        for animal_row in animal_dim_data_csv:
            animal_id, name, dob, reprod, gender, animal_type, breed, color, datetime, animal_key  = animal_row
            animal_dimension_insert_query = f"""INSERT INTO animaldimension (animal_key, animal_id, name, dob, reprod, gender, animal_type, breed, color, datetime) VALUES ('{animal_key}', '{animal_id}', '{name}', '{dob}', '{reprod}', '{gender}', '{animal_type}', '{breed}', '{color}', '{datetime}');"""       
            queries_string += animal_dimension_insert_query + ' '

    elif table_name =="datedimension":
        date_dim_data_csv = csv.reader(StringIO(data))
        next(date_dim_data_csv)
        for date_row in date_dim_data_csv:
            month_recorded, year_recorded, date_key = date_row
            date_dimension_insert_query = f"""INSERT INTO datedimension (date_key, month_recorded, year_recorded) VALUES ('{date_key}', '{month_recorded}', '{year_recorded}');"""
            queries_string += date_dimension_insert_query + ' '

    
    elif table_name=="outcomedimension":
        outcome_type_dim_data_csv = csv.reader(StringIO(data))
        next(outcome_type_dim_data_csv)
        for outcome_type_row in outcome_type_dim_data_csv:
            outcome_type, outcome_type_key = outcome_type_row
            outcome_dimension_insert_query = f"""INSERT INTO outcomedimension (outcome_type_key, outcome_type) VALUES ('{outcome_type_key}', '{outcome_type}');"""
            queries_string += outcome_dimension_insert_query + ' '


    else:
        fact_outcomes_data_csv = csv.reader(StringIO(data))
        next(fact_outcomes_data_csv)
        for fact_outcome_row in fact_outcomes_data_csv:
            date_key, animal_key, outcome_type_key = fact_outcome_row
            fact_outcome_insert_query = f"""INSERT INTO outcomesfact (date_key, animal_key, outcome_type_key) VALUES ('{date_key}', '{animal_key}', '{outcome_type_key}');"""
            queries_string += fact_outcome_insert_query + ' '

    with connection.cursor() as cursor:
        cursor.execute(queries_string)
    connection.commit()

def get_sql_connection_details():
    postgres_dataloader = PostgresDataLoader()
    connection = postgres_dataloader.connect_to_postgres()
    return connection

def load_animal_dimension_data():
    connection = get_sql_connection_details()
    bucket_name = 'data_center_lab3'
    animal_dimension_csv_path = "transformed_data/dim_animal.csv"

    # Read data from GCS
    animal_dim_data = read_data_from_gcs(bucket_name, animal_dimension_csv_path)

    create_table(connection,"animaldimension")

    load_data_into_table(connection, "animaldimension", animal_dim_data)

    connection.close()

def load_date_dimension_data():
    connection = get_sql_connection_details()
    bucket_name = 'data_center_lab3'
    dates_dimension_csv_path = "transformed_data/dim_dates.csv"

    # Read data from GCS
    date_dim_data = read_data_from_gcs(bucket_name, dates_dimension_csv_path)

    create_table(connection,"datedimension")

    load_data_into_table(connection, "datedimension", date_dim_data)

    connection.close()

def load_outcome_type_dimesion_data():
    connection = get_sql_connection_details()
    bucket_name = 'data_center_lab3'
    outcome_types_dimesion_csv_path = "transformed_data/dim_outcome_types.csv"

    # Read data from GCS
    outcome_type_dim_data = read_data_from_gcs(bucket_name, outcome_types_dimesion_csv_path)

    create_table(connection,"outcomedimension")

    load_data_into_table(connection, "outcomedimension", outcome_type_dim_data)

    connection.close()

def load_fact_outcomes_data():
    connection = get_sql_connection_details()
    bucket_name = 'data_center_lab3'
    fact_outcomes_path = "transformed_data/fct_outcomes.csv"

    # Read data from GCS
    fact_outcomes_data = read_data_from_gcs(bucket_name, fact_outcomes_path)

    create_table(connection,"outcomesfact")

    load_data_into_table(connection, "outcomesfact", fact_outcomes_data)

    connection.close()

def load_data_to_postgres(file_name, table_name):
    if table_name =="animaldimension":
        load_animal_dimension_data()
    elif table_name == "datedimension":
        load_date_dimension_data()
    elif table_name == "outcomedimension":
        load_outcome_type_dimesion_data()
    else:
        load_fact_outcomes_data()            