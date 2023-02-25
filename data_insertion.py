import streamlit as st
import snowflake.connector
import pandas as pd





def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user="SHAHPARV",
        password="Parvashah@123",
        account='dhjnpjm-xv03884',
        database="DAMG_ASS_2",
        schema="KPMG_DATASET"
    )
    cur = conn.cursor()
    return cur
    
def create_table(cur,kwargs):
    table_vals = []
    for i in kwargs.items():
        col = "{} {}".format(i[0],i[1])
        table_vals.append(col)

    sql_comm = ','.join(table_vals)

    create_table_sql = "CREATE OR REPLACE TABLE kpmg_bike ({})".format(sql_comm)
    cur.execute(create_table_sql)
    return "Table Created!"


# Load the data into the table
def insert_data(cur):
    # Load the data from the CSV file into a Pandas DataFrame
    csv_file = "./kpmg_cleaned_dataset_transactions.csv"
    df = pd.read_csv(csv_file)

    # Insert the data into Snowflake
        
        # Insert the data into the table
    for i, row in df.iterrows():
        insert_sql = "INSERT INTO kpmg_bike VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s )"
        cur.execute(insert_sql, tuple(row))
    return "Data Inserted"

table_format = {
    "transaction_id" : "NUMBER", 	
    "product_id" : "NUMBER",	
     "customer_id" : "NUMBER",
     "transaction_date"	: "TIMESTAMP_NTZ",
     "online_order"	: "NUMBER",
     "order_status"	: "STRING",
     "brand" : "STRING",
     "product_line"	: "STRING",
     "product_class" : "STRING",
     "product_size"	: "STRING",
     "list_price" : "NUMBER",
     "standard_cost" : "NUMBER",
     "product_first_sold_date" : "TIMESTAMP_NTZ"   
}

cur = connect_to_snowflake()
create_table(cur,table_format)
insert_data(cur)


