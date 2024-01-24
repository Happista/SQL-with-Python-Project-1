#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pandas as pd
import os
import pyodbc

def establish_database_connection():
    # Set up the database connection
    server_name = os.environ.get('server_name')
    database_name = os.environ.get('database_name')
    username = os.environ.get('user')
    password = os.environ.get('Db_Pass')

    conn = pyodbc.connect(f'DRIVER=ODBC Driver 17 for SQL Server;'
                         f'SERVER={server_name};'
                         f'DATABASE={database_name};'
                         f'UID={username};'
                         f'PWD={password}')
    
    return conn

def execute_sql_query(conn, query):
    # Execute SQL query and return the result as a DataFrame
    data = pd.read_sql(query, conn)
    return pd.DataFrame(data)

def main():
    # Load Inputdata from CSV into a DataFrame
    df = pd.read_csv(r"C:\Users\happy.tiwari\Desktop\Cohort Input.csv")

    # File path for the output CSV
    output_file_path = r"C:\Users\happy.tiwari\Desktop\CohortRevenueNov1-30.csv"

    # Delete the output file if it exists
    if os.path.exists(output_file_path):
        os.remove(output_file_path)

    # Establish database connection
    conn = establish_database_connection()

    # Query to retrieve data from the database
    query1 = '''select UserLoginId , sum(case when currency = 'USD' then Amount*70 else Amount end) as TotalAmount
                from [transaction].userrecharge
                where userrechargeorderid is not null
                and transactionstatusid = 1
                and cast(createddate as date) between '2024-01-11' and '2024-01-20'
                group by userloginid'''

    query2 = '''select UserLoginId,
                case when (month(max(CreatedDate)) > month(min(CreatedDate))
				or year(max(CreatedDate)) > year(min(CreatedDate)))
				then 'RepeatUser' else 'NewUser' end as [UserType],
                max(CreatedDate)LastRechargeDate,min(CreatedDate)FirstRechargeDate
                from [Transaction].UserRecharge
                where TransactionStatusId = 1 and UserRechargeOrderId is not null
                group by UserLoginId'''

    # Retrieve data from the database and create DataFrames
    data1 = execute_sql_query(conn, query1)
    data2 = execute_sql_query(conn, query2)

    # Close the database connection
    conn.close()

    dff = pd.DataFrame(data1)
    dfff = pd.DataFrame(data2)

    # Apply joins to get the data
    merged_df = pd.merge(df, dff, on='UserLoginId', how='left')
    merged_df = pd.merge(merged_df, dfff, on='UserLoginId', how='left')

    # Save the merged DataFrame to a new CSV file
    merged_df.to_csv(output_file_path, index=False)

if __name__ == "__main__":
    main()


# In[ ]:





# In[ ]:





# In[ ]:




