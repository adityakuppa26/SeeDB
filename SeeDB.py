#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 11 21:58:41 2022

@author: krutikasarode
"""

import psycopg2
import pandas as pd
from collections import defaultdict
import numpy as np
import scipy.stats
import math

# psql -h cs645db.cs.umass.edu -p 7645
                       
#You are connected to database "ksarode" as user "ksarode" on host "cs645db.cs.umass.edu" (address "128.119.243.149") at port "7645".
#SSL connection (protocol: TLSv1.3, cipher: TLS_AES_256_GCM_SHA384, bits: 256, compression: off)

# conn = psycopg2.connect(dbname="ksarode")
conn = psycopg2.connect(dbname="ksarode", port="7645" , host="cs645db.cs.umass.edu")
# conn = psycopg2.connect(user='ksarode', password='Kruti2306!',dbname="papers")
#Creating a cursor object using the cursor() method
cursor = conn.cursor()

#Executing an MYSQL function using the execute() method
cursor.execute("select version()")

# Fetch a single row using fetchone() method.
data = cursor.fetchone()

print(data)

select_data_query = "select * from concensus"

cursor.execute(select_data_query) 
concensus_records = cursor.fetchall()

print("Print each row and it's columns values")
for row in concensus_records:
        print(type(row))
        print("Model = ", row[1])
        print("Price  = ", row[2], "\n")
        
        
raw_df=pd.DataFrame(concensus_records,columns =['age', 'workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary_range'])

q="select * from concensus where marital_status like 'Married%'"
cursor.execute(select_data_query) 
qr_records = cursor.fetchall()
qr_df=pd.DataFrame(qr_records,columns =['age', 'workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary_range'])


q="select * from concensus where marital_status not like 'Married%'"
cursor.execute(select_data_query) 
qd_records = cursor.fetchall()
qd_df=pd.DataFrame(qd_records,columns =['age', 'workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary_range'])

# print("Print each row and it's columns values")
# for row in qr_records[:5]:
#         print("Workclass = ", row[1])
#         print("fnlwgt  = ", row[2], "\n")
        
# col_min=raw_df["capital_loss"].min()
# col_max=raw_df["capital_loss"].max()
# normalization_query="select capital_loss, (capital_loss-{})/({}-{}) as normalized_capital_loss from concensus ".format(col_min,col_max,col_min)
# print(normalization_query)
# cursor.execute(normalization_query) 
# q_records = cursor.fetchall()

# print("Print each row and it's columns values")
# for row in q_records[:5]:
#         print("CaptalLoss = ", row[0])
#         print("value  = ", row[1], "\n")
        
# raw_df["normalized_capital_loss"]=(raw_df["capital_loss"]-col_min)/(col_max-col_min)
# raw_df.head()
# raw_df['hours_per_week'].unique()
# raw_df["normalized_hours_per_week"]=(raw_df["hours_per_week"]-raw_df["hours_per_week"].min())/(raw_df["hours_per_week"].max()-raw_df["hours_per_week"].min())
# raw_df.head()

# qr_df["normalized_capital_loss"]=(raw_df["capital_loss"]-col_min)/(col_max-col_min)
# qr_df['hours_per_week'].unique()
# qr_df["normalized_hours_per_week"]=(raw_df["hours_per_week"]-raw_df["hours_per_week"].min())/(raw_df["hours_per_week"].max()-raw_df["hours_per_week"].min())

# qd_df["normalized_capital_loss"]=(raw_df["capital_loss"]-col_min)/(col_max-col_min)
# qd_df.head()
# qd_df['hours_per_week'].unique()
# qd_df["normalized_hours_per_week"]=(raw_df["hours_per_week"]-raw_df["hours_per_week"].min())/(raw_df["hours_per_week"].max()-raw_df["hours_per_week"].min())

# q1_df=qr_df.groupby(['workclass'])['normalized_hours_per_week'].agg('sum')
# q1_df.head()

q="drop view reference_data;drop view actual_data;create view reference_data as select * from concensus where marital_status not like 'Married%';\
    create view actual_data as select * from concensus where marital_status like 'Married%';"
    
cursor.execute(q) 
conn.commit()

q="select workclass,sum(capital_loss) as capital_loss from reference_data group by workclass"
cursor.execute(q) 
workclass_reference_records = cursor.fetchall()
for row in workclass_reference_records:
        print("Model = ", row[0])
        print("Price  = ", row[1], "\n")
        
# cursor.execute("ROLLBACK")
# conn.commit()
q="select workclass,sum(capital_loss) as capital_loss from actual_data group by workclass"
cursor.execute(q) 
workclass_actual_records = cursor.fetchall()
print([i[1] for i in workclass_actual_records])


def normalize_data(data):
    minn=min(data)
    maxx=max(data)
    return [round((i-minn)/(maxx-minn),3) for i in data]


def kl_divergence(ref_data,actual_data):
    ref_data=normalize_data(ref_data)
    print("Normalized reference data: ",ref_data)
    actual_data=normalize_data(actual_data)
    print("Normalized actual data: ",actual_data)
    p_ref_data=[round(i/sum(ref_data),3) for i in ref_data]
    print("probability of reference data: ",p_ref_data)
    p_act_data=[round(i/sum(actual_data),3) for i in actual_data]
    print("probability of actual  data: ",p_act_data)
    return sum([(p_act_data[i]*(math.log(p_act_data[i]/p_ref_data[i]))) for i in range(len(p_ref_data)) if p_ref_data[i]>0 and p_act_data[i]>0])


def sharing_optimization():
    groupby_col=['workclass','education','marital_status','occupation','relationship','race','sex','native_country']
    aggregate_col=['fnlwgt','education_number','capital_gain','capital_loss','hours_per_week']
    group_by=['sum','count','avg']


def pruning_optimization(views, phase_num, utility_measures, confidence=0.95, k=10):
    confidence_intervals = defaultdict(list)
    for view in views:
        a, m, f = view  # tuple unpacking (a, m, f)
        # TODO: update the below queries
        query_target = f"select {a}, {f}({m}) from target_data where phase=phase_num group by {a};"
        query_ref = f"select {a}, {f}({m}) from ref_data where phase=phase_num group by {a};"
        # TODO: make a call to postgres and GET the data
        query_ref_data, query_target_data = None, None
        # calculate utility and confidence intervals
        utility_measure = kl_divergence(query_ref_data, query_target_data)
        utility_measures[phase_num][view] = utility_measure
        mean_utility_measure = sum(utility_measures[:][view])/phase_num
        std_err = scipy.stats.sem(np.array(utility_measures[:][view]))
        h = std_err * scipy.stats.t.ppf((1 + confidence) / 2., len(utility_measures[:][view]) - 1)
        confidence_intervals[view] = [mean_utility_measure-h, mean_utility_measure+h]

    # prune the views if upper bound below lower bounds of 10 other views
    # TODO: optimize the code if possible
    final_views = views.copy()
    for ind, view in enumerate(views):
        count = 0
        for v in views:
            if confidence_intervals[view][1] < confidence_intervals[v][0]:
                count += 1
            if count >= k:
                break
        if count >= k:
            final_views.pop(ind)

    return final_views


# def mean_confidence_interval(data, confidence=0.95):
#     a = 1.0 * np.array(data)
#     n = len(a)
#     m, se = np.mean(a), scipy.stats.sem(a)
#     h = se * scipy.stats.t.ppf((1 + confidence) / 2., n-1)
#     return m, m-h, m+h
