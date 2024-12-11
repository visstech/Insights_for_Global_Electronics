
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from psycopg2.extras import execute_values
 

def customer_data():
        # Load dataset
        customers = pd.read_csv('C:\ML\Global_Electronics\DataSet\\Customers.csv', encoding='ISO-8859-1')

        # Handling Missing Values
        customers['City'].fillna('Unknown', inplace=True)
        customers['State'].fillna('Unknown', inplace=True)
        customers['State Code'].fillna('Unknown', inplace=True)  
        customers['Zip Code'].fillna(0, inplace=True)
        customers['Country'].fillna('Unknown', inplace=True)
        customers['Continent'].fillna('Unknown', inplace=True)

        # Correcting Data Types
        customers['Birthday'] = pd.to_datetime(customers['Birthday'], errors='coerce')

        # Removing Duplicates (excluding primary key and unique key)
        customers.drop_duplicates(subset=[col for col in customers.columns if col not in ['CustomerKey']], inplace=True)

        # Renaming Columns for Consistency
        customers.rename(columns={'CustomerKey': 'Customer_ID', 'Zip Code': 'Zip_Code', 'State Code': 'State_Code'}, inplace=True)

        # Handling Categorical Data
        customers['Gender'] = customers['Gender'].astype('category')

        # Calculate Age
        customers['Age'] = (pd.Timestamp.now() - customers['Birthday']).dt.total_seconds() / (60*60*24*365.25)
        customers['Age'] = customers['Age'].astype(int)

        # Save cleaned dataset
        customers.to_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Customers.csv', index=False)
        print(customers)
        print("Customers data cleaning completed.")

def sales_data():
    # Load dataset
    sales = pd.read_csv('C:\ML\Global_Electronics\DataSet\\Sales.csv', encoding='ISO-8859-1')

    # Define primary and unique key columns
    primary_keys_sales = ['Order Number', 'Line Item', 'CustomerKey', 'StoreKey', 'ProductKey']

    # Handling Missing Values
    sales.dropna(subset=['Order Date'], inplace=True)

    # Correcting Data Types
    sales['Order Date'] = pd.to_datetime(sales['Order Date'], errors='coerce')

    # Handling Outliers (e.g., capping extreme values in Quantity)
    sales['Quantity'] = np.where(sales['Quantity'] > sales['Quantity'].quantile(0.99), sales['Quantity'].quantile(0.99), sales['Quantity'])

    # Removing Duplicates
    sales.drop_duplicates(inplace=True)

    # High Percentage of Missing Values
    threshold = 0.5
    columns_to_drop = [col for col in sales.columns if sales[col].isnull().sum() / len(sales) > threshold and col not in primary_keys_sales]

    # Drop irrelevant, low variance, redundant, high correlation, and privacy-sensitive columns as per conditions
    sales.drop(columns=set(columns_to_drop), inplace=True)

    # Save cleaned dataset
    sales.to_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Sales.csv', index=False)

    print("Sales data cleaning completed.")
    

def stores_data():
    # Load dataset
    stores = pd.read_csv('C:\ML\Global_Electronics\DataSet\\Stores.csv', encoding='ISO-8859-1')

    # Display the first few rows to understand the data
    print(stores.head())

    # Handling Missing Values

    stores['Country'].fillna('Unknown', inplace=True)
    stores['State'].fillna('Unknown', inplace=True)
    stores['Square Meters'].fillna(0, inplace=True)

    # Correcting Data Types
    stores['Open Date'] = pd.to_datetime(stores['Open Date'], errors='coerce')

    # Removing Duplicates (excluding primary key)
    stores.drop_duplicates(subset=[col for col in stores.columns if col not in ['StoreKey']], inplace=True)

    # Handle Inconsistent Data (if any)

    # Save cleaned dataset
    stores.to_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Stores.csv', index=False)

    print("Stores data cleaning completed.")

def product_data():
    # Load dataset
    products = pd.read_csv('C:\ML\Global_Electronics\DataSet\\Products.csv', encoding='ISO-8859-1')

    # Handling Missing Values
    products['Color'].fillna('Unknown', inplace=True)

    # Removing Duplicates
    products.drop_duplicates(inplace=True)

    # Remove currency symbols before converting to numeric
    for column in ['Unit Cost USD', 'Unit Price USD']:
        products[column] = products[column].str.replace('$', '').str.replace(',', '')

    # Correcting Data Types (e.g., converting cost and price to numeric)
    for column in ['Unit Cost USD', 'Unit Price USD']:
        try:
            products[column] = pd.to_numeric(products[column], errors='coerce')
            print(f"Converted {column} to numeric successfully.")
        except Exception as e:
            print(f"Error converting {column} to numeric: {e}")

    # Additional step to handle missing values in cost and price columns
    for column in ['Unit Cost USD', 'Unit Price USD']:
        if products[column].isnull().any():
            products[column].fillna(products[column].median(), inplace=True)
            print(f"Filled missing values in {column} with the median.")

    # Save cleaned dataset
    products.to_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Products.csv', index=False)

    print("Products data cleaning completed.")
    
    print(products.info())
    
def exchange_rate_data():
    # Load dataset
    exchange_rates = pd.read_csv('C:\ML\Global_Electronics\DataSet\\Exchange_Rates.csv', encoding='ISO-8859-1')

    # Correcting Data Types
    exchange_rates['Date'] = pd.to_datetime(exchange_rates['Date'], errors='coerce')

    # Removing Duplicates
    exchange_rates.drop_duplicates(inplace=True)

    # Save cleaned dataset
    exchange_rates.to_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Exchange_Rates.csv', index=False)

    print("Exchange rates data cleaning completed.") 
    
def merge_data():
    # Load cleaned datasets
    customers = pd.read_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Customers.csv', encoding='ISO-8859-1')
    sales = pd.read_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Sales.csv', encoding='ISO-8859-1')
    stores = pd.read_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Stores.csv', encoding='ISO-8859-1')
    products = pd.read_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Products.csv', encoding='ISO-8859-1')
    exchange_rates = pd.read_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Cleaned_Exchange_Rates.csv', encoding='ISO-8859-1')

    # Merge sales with customers
    sales_customers = pd.merge(sales, customers, how='left', left_on='CustomerKey', right_on='Customer_ID')

    # Merge sales_customers with stores
    sales_customers_stores = pd.merge(sales_customers, stores, how='left', left_on='StoreKey', right_on='StoreKey')

    # Merge sales_customers_stores with products
    sales_customers_stores_products = pd.merge(sales_customers_stores, products, how='left', left_on='ProductKey', right_on='ProductKey')

    # Optionally, merge with exchange rates if needed (e.g., if there are columns that need exchange rates)
    # Assuming you might want to join on a common date column, which isn't directly mentioned in the given datasets
    # sales_customers_stores_products = pd.merge(sales_customers_stores_products, exchange_rates, how='left', left_on='Date', right_on='Date')

    # Save the merged dataset
    sales_customers_stores_products.to_csv('C:\ML\Global_Electronics\DataSet\Cleaned_dataset\Merged_data.csv', index=False)

    print("Data merging completed.")

# PostgreSQL connection details
DB_NAME = 'postgres' #' database_name'
DB_USER = 'postgres' #'username'
DB_PASSWORD = 'PostViss' #'password'
DB_HOST = 'localhost'
DB_PORT = '5432'   
# Function to connect to PostgreSQL
def connect_to_db():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    print('Database connected.....')
    return conn
 
def load_all_data():
        # Load the CSV data
        file_path = 'C:\ML\Global_Electronics\DataSet\Cleaned_dataset\merged_data.csv'
        merged_data = pd.read_csv(file_path)

        # Replace NaN values with None to handle nulls in MySQL
        merged_data = merged_data.replace({np.nan: None})

        # Drop the extra columns that are not part of the insert query
        merged_data = merged_data.drop(['Name', 'State_Code'], axis=1)

        # Check if the number of columns matches the expected number (30)
        print(f"DataFrame columns: {len(merged_data.columns)}")  # Should print 30

        # Establish MySQL connection
        #conn = pymysql.connect(
            #host="127.0.0.1",
            #user="root",
            #password="Appaamma@123",
            #database="final_analysis"
        #)
       
        
        conn = connect_to_db()
        cursor = conn.cursor()
  

        # Create the table if it does not exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS GLOBAL_ELECTRONICS_DATA (
                Order_Number INT,
                Line_Item INT,
                Order_Date DATE,
                CustomerKey INT,
                StoreKey INT,
                ProductKey INT,
                Quantity FLOAT,
                Currency_Code VARCHAR(10),
                Customer_ID INT,
                Gender VARCHAR(10),
                City VARCHAR(100),
                State_x VARCHAR(100),
                Zip_Code VARCHAR(20),
                Country_x VARCHAR(100),
                Continent VARCHAR(50),
                Birthday DATE,
                Age INT,
                Country_y VARCHAR(100),
                State_y VARCHAR(100),
                Square_Meters FLOAT,
                Open_Date DATE,
                Product_Name VARCHAR(255),
                Brand VARCHAR(100),
                Color VARCHAR(50),
                Unit_Cost_USD FLOAT,
                Unit_Price_USD FLOAT,
                SubcategoryKey INT,
                Subcategory VARCHAR(100),
                CategoryKey INT,
                Category VARCHAR(100)
            )
        ''')

        # Insert query template
        # insert_query = """
        # INSERT INTO merged_data (
        #     Order_Number, Line_Item, Order_Date, CustomerKey, StoreKey, ProductKey, 
        #     Quantity, Currency_Code, Customer_ID, Gender, City, State_x, 
        #     Zip_Code, Country_x, Continent, Birthday, Age, Country_y, State_y, Square_Meters, 
        #            Open_Date, Product_Name, Brand, Color, Unit_Cost_USD, Unit_Price_USD, SubcategoryKey, 
        #     Subcategory, CategoryKey, Category
        # ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            #"""

        # Loop through each row and insert into the database
        # for index, row in merged_data.iterrows():
        #     # Convert the row to a tuple
        #     row_tuple = tuple(row)
            
        #     # Execute the insert query
        #     cursor.execute(insert_query, row_tuple)

        # Commit changes and close the connection
        
        for column in merged_data.columns:
            if column in ('Order Number','Line Item','CustomerKey','StoreKey','ProductKey','Customer_ID','Age','SubcategoryKey','CategoryKey'):
                 converted_array = np.array(merged_data[column])
                 merged_data[column]  = [(int(x),) for x in converted_array]
        
 
        print('after conversion:\n',merged_data.info())
        insert_query = """
        INSERT INTO GLOBAL_ELECTRONICS_DATA (
            Order_Number, Line_Item,Order_Date,
            CustomerKey,
            StoreKey,
            ProductKey,
            Quantity,
            Currency_Code,
            Customer_ID,
            Gender,
            City,
            State_x ,
            Zip_Code,
            Country_x,
            Continent,
            Birthday,
            Age,
            Country_y,
            State_y,
            Square_Meters,
            Open_Date,
            Product_Name,
            Brand,
            Color,
            Unit_Cost_USD,
            Unit_Price_USD,
            SubcategoryKey,
            Subcategory,
            CategoryKey,
            Category
                    ) VALUES %s 
        """

        # Convert DataFrame to a list of tuples
        data_tuples = merged_data.to_records(index=False)
        print(data_tuples)
        
        # Insert data efficiently using execute_values
        execute_values(cursor, insert_query, data_tuples)

        # Commit the transaction
        conn.commit()
        print("Data inserted successfully!")
        conn.commit()
        cursor.close()
        conn.close()

print("Data inserted successfully.")    


if __name__ == "__main__":
    
    customer_data()
    sales_data()
    stores_data()
    product_data()
    exchange_rate_data()
    connect_to_db()
    merge_data()
    load_all_data()
    
