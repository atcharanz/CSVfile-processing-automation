import mysql.connector
import pandas as pd
from functools import reduce

#if rootserver user not  working create new SQLserver


#connecting to SQLserver using mysql.connector
mydb = mysql.connector.connect(host='localhost', user='charanc', passwd='Open@g50',
                               database='mydata')
mycursor = mydb.cursor()


#pass file names while calling the funtion
#EX:processed_df('2020VAERSData.csv','2020VAERSSYMPTOMS.csv','2020VAERSVAX.csv')

#funtion to merge mutiple csv file into a single dataframe
def processed_df(file1='', file2='', file3=''):
    # Reading csv file as data frames
    df1 = pd.read_csv(file1, encoding='ISO-8859-1', dtype=object, index_col=0)
    df2 = pd.read_csv(file2, encoding='ISO-8859-1', dtype=object, index_col=0)
    df3 = pd.read_csv(file3, encoding='ISO-8859-1', dtype=object, index_col=0)
    data_frames = [df1, df2, df3]

    # Merging multiple dataframes into one
    df_merged = reduce(lambda left, right: pd.merge(left, right, on=['VAERS_ID'],
                                                    how='outer'), data_frames).fillna('None')
    return df_merged


def create_db():

  #SQL querry for creating table with required fields
    mycursor.execute("""CREATE TABLE sample 
                            (VAERS_ID INT ,AGE_YRS INT,SEX VARCHAR(2),VAX_NAME VARCHAR(100),
                            VAX_DATE VARCHAR(20), VAX_ROUTE VARCHAR(100),
                            SYMPTOM1 VARCHAR(300),DIED VARCHAR(20))""")


def dataupload_into_db(df_merged):

    req_fields=['VAERS_ID', 'AGE_YRS','SEX','VAX_NAME','VAX_DATE','VAX_ROUTE','SYMPTOM1','DIED']

   # Fetching required columns from merged dataframe  and convert into list
    df = pd.read_csv("Processed.csv", encoding='ISO-8859-1', dtype=object)
    req_val =  df[req_fields]
    db_values = req_val.head(10).values.tolist()

    #SQL querry to insert fetched data to corresponding columns in database
    query = """INSERT INTO sample (VAERS_ID,AGE_YRS,SEX,VAX_NAME,VAX_DATE,VAX_ROUTE,SYMPTOM1,DIED) 
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"""

   #Uploading data into database
    mycursor.executemany(query, db_values)
    mydb.commit()



if __name__ == "__main__":

    processed_df('2020VAERSData.csv','2020VAERSSYMPTOMS.csv','2020VAERSVAX.csv')
    create_db()
    dataupload_into_db(processed_df)




