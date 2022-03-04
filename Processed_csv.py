import pandas as pd
from functools import reduce

#pass file names while calling the funtion
#Example:final_csv('2020VAERSData.csv','2020VAERSSYMPTOMS.csv','2020VAERSVAX.csv')

def processed_csv(file1='',file2='',file3=''):

    # Reading csv file as data frames
     df1 = pd.read_csv(file1, encoding='ISO-8859-1', dtype=object, index_col=0)
     df2 = pd.read_csv(file2, encoding='ISO-8859-1', dtype=object, index_col=0)
     df3 = pd.read_csv(file3, encoding='ISO-8859-1', dtype=object, index_col=0)
     data_frames=[df1,df2,df3]

    #Merging multiple dataframes into one
     df_merged = reduce(lambda left, right: pd.merge(left, right, on=['VAERS_ID'],
                    how='outer'), data_frames).fillna('None')

    # Fetching required columns from merged dataframe and write into into new csv file
     req_colums=df_merged[['VAX_MANU','RECVDATE','STATE','AGE_YRS', 'CAGE_YR','CAGE_MO','SEX','SYMPTOM1',
             'SYMPTOM2','SYMPTOM3', 'SYMPTOM4','SYMPTOM5','RECOVD','DIED','L_THREAT','ER_VISIT',
             'DISABLE','BIRTH_DEFECT','VAX_NAME','OTHER_MEDS','VAX_ROUTE','NUMDAYS','VAX_DATE',
             'CUR_ILL','HISTORY','SYMPTOM_TEXT']]

     req_colums.to_csv('Processed.csv')

processed_csv('2020VAERSData.csv','2020VAERSSYMPTOMS.csv','2020VAERSVAX.csv')
