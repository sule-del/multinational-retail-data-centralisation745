import tabula 
import boto3
import pandas as pd
from io import StringIO

class DataExtractor: 
   def __init__(self):

      pass
   
   def read_rds_table(self, table_name):
        engine = self.db_connector.init_db_engine()
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, engine)
   
   def retrieve_pdf_data(self, pdf_link):
        pdf_data = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
        df = pd.concat(pdf_data, ignore_index=True)
        return df
   
   def upload_cleaned_data_to_db(self, cleaned_df, table_name):
        self.db_connector.upload_to_db(cleaned_df, table_name)
        data_extractor = DataExtractor(db_connector)
        data_cleaner = DataCleaning()
        pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        pdf_data = data_extractor.retrieve_pdf_data(pdf_link)
        cleaned_card_data = data_cleaner.clean_card_data(pdf_data)
        data_extractor.upload_cleaned_data_to_db(cleaned_card_data, "dim_card_details")

   def list_number_of_stores(self,endpoint):
         response = requests.get(endpoint, headers=self.api_headers)
         if response.status_code == 200:
            return response.json()['number_of_stores']
         else:
            # Handle error, you can raise an exception or return a default value
            return 0
         
   def retrieve_stores_data(self, endpoint):
        response = requests.get(endpoint, headers=self.api_headers)
        if response.status_code == 200:
            return response.json()['store_data']
        else:
            # Handle error, you can raise an exception or return an empty dictionary
            return {}
        
   def extract_from_s3(self, s3_address):
        s3 = boto3.client('s3')
        bucket, key = s3_address.replace('s3://data-handling-public/products.csv').split('/', 1)
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(content))

        return df
   
   
def upload_cleaned_data_to_db(self, cleaned_df, table_name):
        self.db_connector.upload_to_db(cleaned_df, table_name)
        data_extractor = DataExtractor(db_connector)
        data_cleaner = DataCleaning()

       s3_address = "s3://data-handling-public/products.csv"
       products_data = data_extractor.extract_from_s3(s3_address)


       converted_products_data = data_cleaner.convert_product_weights(products_data)


       cleaned_products_data = data_cleaner.clean_products_data(converted_products_data)
       data_extractor.upload_cleaned_data_to_db(cleaned_products_data, "dim_products")

       data_extractor = DataExtractor()
       all_tables = data_extractor.list_db_tables()
       print("All Tables in the Database:", all_tables)
       product_orders_table = 'product_orders_table'
        data_extractor = DataExtractor()
         orders_data = data_extractor.read_rds_table(product_orders_table)

def upload_cleaned_data_to_db(self, cleaned_df, table_name):
        
        self.db_connector.upload_to_db(cleaned_df, table_name)
        data_extractor = DataExtractor(db_connector)
        data_cleaner = DataCleaning()
        orders_data = data_extractor.read_rds_table(product_orders_table)
        cleaned_orders_data = data_cleaner.clean_orders_data(orders_data)
        data_extractor.upload_cleaned_data_to_db(cleaned_orders_data, "orders_table")

def extract_from_s3(self, s3_address):
              
        pass

       data_extractor = DataExtractor()
       json_s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
       date_details_data = data_extractor.extract_from_s3(json_s3_address)