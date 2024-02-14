import pandas as pd

class DataCleaning:
def clean_user_data(self, df):
        cleaned_df = df.dropna()
        return cleaned_df

def upload_to_db(self, df, table_name):
        engine = self.init_db_engine()
        df.to_sql(table_name, engine, if_exists='replace', index=False)

def clean_card_data(self, df):
        cleaned_df = df  
        return cleaned_df

def clean_store_data (self,store_data):
        cleaned_df = pd.DataFrame(store_data)
        return cleaned_df
    
     from data_cleaning import DataCleaning
     from database_connector import DatabaseConnector
     cleaner = DataCleaning()
     cleaned_store_data = [cleaner.clean_store_data(store) for store in stores_data]
     connector = DatabaseConnector()
        for idx, store_df in enumerate(cleaned_store_data):
     table_name = f'dim_store_details_{idx + 1}'
     connector.upload_to_db(store_df, table_name) 

def convert_product_weights(self, products_df):
       converted_df = products_df.copy()
       converted_df['weight'] = converted_df['weight'].apply(self.clean_and_convert_weight)
       return converted_df
def clean_and_convert_weight(self, weight_str):
        weight_str = weight_str.replace('ml', 'g')
        weight_str = ''.join(char for char in weight_str if char.isdigit() or char in ['.', ','])
        try:
            weight_float = float(weight_str.replace(',', '.'))
        except ValueError:
            weight_float = None  

        return weight_float

def clean_products_data(self, products_df):
        cleaned_df = products_df  
        return cleaned_df

def clean_orders_data(self, orders_df):
        cleaned_orders_df = orders_df.drop(columns=['first_name', 'last_name', '1'], errors='ignore')
        return cleaned_orders_df

def clean_date_details_data(self, date_details_df):        
        cleaned_date_details_df = date_details_df  
        return cleaned_date_details_df

     data_cleaner = DataCleaning()
     cleaned_date_details_data = data_cleaner.clean_date_details_data(date_details_data)


     data_extractor = DataExtractor()
     data_extractor.upload_cleaned_data_to_db(cleaned_date_details_data, "dim_date_times")