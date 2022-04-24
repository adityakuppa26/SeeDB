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

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Kruti2306!")
#Creating a cursor object using the cursor() method
cursor = conn.cursor()


select_data_query = "select * from census"

cursor.execute(select_data_query) 
concensus_records = cursor.fetchall()

q="drop view reference_data cascade;drop view actual_data cascade;create view reference_data as select * from census where marital_status not like 'Married%';create view actual_data as select * from census where marital_status like 'Married%';"
    
cursor.execute(q) 
conn.commit()


num_phases=10
records_per_phase=int(len(concensus_records)/num_phases)
div=np.linspace(0, len(concensus_records)-1, num_phases+1,dtype=int)
# print(div)
 
see_db=SeeDB() 
sorted_dict=[]
for i in range(0,num_phases):#len(div)-1):
    
    kl_divergence_dic=see_db.sharing_optimization_modified(cursor,div[i],div[i+1])
    conn.commit()
    # print(len(see_db.prunning_dict.keys()))
    ret=see_db.prunning(cursor,kl_divergence_dic,i+1,num_phases)
    see_db.cleaning()
    # print("length of deleted dict: ",len(see_db.deleted_attr))
    # print("length of prunning dict: ",len(see_db.prunning_dict))
    sorted_dict=dict(sorted(see_db.prunning_dict.items(), key=lambda item: item[1][-1], reverse=True))
    
    # print(top_k_values) 
    # see_db.plot(top_k_values[0])
print("Length:",sorted_dict)
top_k_values=[k for k,v in sorted_dict.items()]
print(top_k_values)
print(len(top_k_values))
for r in top_k_values[:10]:
    see_db.plot(r,cursor)
# see_db.plot(sorted_dict,cursor)
conn.commit()


    
    

    
    







        

        
