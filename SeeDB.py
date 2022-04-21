#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 11 21:58:41 2022

@author: krutikasarode
"""

import psycopg2
import pandas as pd
import numpy as np
import math
import matplotlib.pyplot as plt
from SeeDBUtils import SeeDB
# import SeeDBUtils




# psql -h cs645db.cs.umass.edu -p 7645
                       
#You are connected to database "ksarode" as user "ksarode" on host "cs645db.cs.umass.edu" (address "128.119.243.149") at port "7645".
#SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, bits: 256, compression: off)

# conn = psycopg2.connect(dbname="ksarode", port="7645" , host="cs645db.cs.umass.edu")
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Kruti2306!")
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing an MYSQL function using the execute() method
cursor.execute("select version()")

# Fetch a single row using fetchone() method.
data = cursor.fetchone()

print(data)

select_data_query = "select * from census"

cursor.execute(select_data_query) 
concensus_records = cursor.fetchall()


        
        
# raw_df=pd.DataFrame(concensus_records,columns =['age', 'workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary_range'])

# q="select * from census where marital_status like 'Married%'"
# cursor.execute(select_data_query) 
# qr_df=pd.DataFrame(qr_records,columns =['age', 'workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary_range'])


# q="select * from census where marital_status not like 'Married%'"

# cursor.execute(select_data_query) 
# qd_records = cursor.fetchall()
# qd_df=pd.DataFrame(qd_records,columns =['age', 'workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary_range'])

#

q="drop view reference_data cascade;drop view actual_data cascade;create view reference_data as select * from census where marital_status not like 'Married%';create view actual_data as select * from census where marital_status like 'Married%';"
    
cursor.execute(q) 
conn.commit()
      
# cursor.execute("ROLLBACK")
# conn.commit()

num_phases=10
# print(len(concensus_records))
records_per_phase=int(len(concensus_records)/num_phases)
# print(concensus_records[-1][0])
div=np.linspace(0, len(concensus_records)-1, num_phases+1,dtype=int)
# print(div)
 
see_db=SeeDB() 
sorted_dict=[]
for i in range(0,num_phases):#len(div)-1):
    
    # print(div[i],div[i+1])
    kl_divergence_dic=see_db.sharing_optimization_modified(cursor,div[i],div[i+1])
    # print(kl_divergence_dic)
    conn.commit()
    print(len(see_db.prunning_dict.keys()))
    ret=see_db.prunning(cursor,kl_divergence_dic,i+1,num_phases)
    print("Prunning list")
    see_db.cleaning()
    # print(see_db.prunning_dict)
    # print(len(see_db.prunning_dict.keys()))
    sorted_dict=dict(sorted(see_db.prunning_dict.items(), key=lambda item: item[1][-1], reverse=True))
    print(sorted_dict)
    # top_k_values=[k for k,v in sorted_dict.items()][]
    # print(top_k_values) 
    # see_db.plot(top_k_values[0])

# sorted_dict=['education-fnlwgt-avg', 'workclass-fnlwgt-avg', 'relationship-education_num-avg', 'relationship-fnlwgt-avg', 'native_country-fnlwgt-avg']

# sorted_dict=['relationship-hours_per_week-avg', 'relationship-fnlwgt-avg', 'race-hours_per_week-avg', 'race-fnlwgt-avg', 'relationship-hours_per_week-sum']

for r in sorted_dict:
    see_db.plot(r,cursor)
# see_db.plot(sorted_dict[4],cursor)
# conn.commit()


    
    

    
# see_db=SeeDB()   
# kl_divergence_dic=see_db.sharing_optimization_modified(cursor)
# print(kl_divergence_dic)
# conn.commit()

# top5_pairs=kl_divergence_dic[0:5]
# print(top5_pairs)
    
# for v in top5_pairs:
#     plot(v[0])
    
# plot(top5_pairs[1][0])
    
            
            
            
            
            
     
    
# kl_divergence([i[1] for i in sorted(workclass_reference_records)],[i[1] for i in sorted(workclass_actual_records)])
    
    







        

        
