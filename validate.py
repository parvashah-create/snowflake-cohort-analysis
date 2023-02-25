
import snowflake.connector
import pandas as pd
import os



def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user="SHAHPARV",
        password="Parvashah@123",
        account='YF08181',
        database="DAMG_ASS_2"
    )

    try:
        cur = conn.cursor()
        return "Snowflake Connected Successfully!"
    except:
        return "Connection Error!" 
    

def create_table(**kwargs):
    table_vals = []
    for i in kwargs.items():
        col = "{} {}".format(i[0],i[1])
        table_vals.append(col)

    sql_comm = ','.join(table_vals)

    create_table_sql = "CREATE OR REPLACE TABLE kpmg_bike ({})".format(sql_comm)

    return cur.execute(create_table_sql)
