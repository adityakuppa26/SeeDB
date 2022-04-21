#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 15 12:59:31 2022

@author: krutikasarode
"""
import math
import pandas as pd
import collections
import numpy as np
import scipy.stats
import matplotlib.pyplot as plt

class SeeDB():
    
    deleted_attr=[]
    view_cnt=0
    prunning_dict={}
    
    def normalize_data(self,data):
        if data and min(data)< max(data):
            # print(data)
            minn=min(data)
            maxx=max(data)
            return [round((i-minn)/(maxx-minn),3) for i in data]
        return data


    def kl_divergence(self,ref_data,actual_data):
        ref_data=self.normalize_data(ref_data)
        # print("Normalized reference data: ",ref_data)
        actual_data=self.normalize_data(actual_data)
        # print("Normalized actual data: ",actual_data)
        p_ref_data=[round(i/sum(ref_data),3) for i in ref_data] if sum(ref_data)>0 else ref_data
        # print("probability of reference data: ",p_ref_data)
        p_act_data=[round(i/sum(actual_data),3) for i in actual_data] if sum(actual_data)>0 else actual_data
        # print("probability of actual  data: ",p_act_data)
        return sum([(float(p_act_data[i])*(math.log(p_act_data[i]/p_ref_data[i]))) for i in range(len(p_ref_data)) if p_ref_data[i]>0 and p_act_data[i]>0])

    # def _make_view_query( selections, table_name, cond, attribute,
    #             start, end):
    #         return ' '.join(['with attrs as (select distinct('
    #                             + attribute + ') as __atr__',
    #                             'from', table_name, ')',
    #                         'select', selections,
    #                         'from', 'attrs left outer join', table_name,
    #                             'on', '__atr__ =', attribute,
    #                             'and', cond,
    #                             'and id>=' + str(start), 'and', 'id<' + str(end),
    #                         'group by __atr__',
    #                         'order by __atr__'])
        
    # res=_make_view_query( "colease", "actual_data","a<10", "marital_status", 5, 10)
    
    def column_class_identification(self,cursor):
        query="select * from actual_data"
        cursor.execute(query)
        records=cursor.fetchall()
        df=pd.DataFrame(records,columns =['id','age', 'workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary_range'])
        for c in df.columns():
            print(type(c))
        
    def sharing_optimization(self,cursor):
        # age | workclass | fnlwgt |  education   | education_num |   marital_status   |    occupation    | relationship  | race  | sex  | capital_gain | capital_loss | hours_per_week | native_country | salaray_range
        groupby_col,aggregate_col=self.column_class_identification(cursor)
        groupby_col=['workclass','education','occupation','relationship','race','sex','native_country']
        # groupby_col=['workclass','education']
        aggregate_col=['fnlwgt','education_num','capital_gain','capital_loss','hours_per_week']
        # aggregate_col=['capital_gain']
        # group_by=['sum','count','avg']
        kl_divergence_dict={}
        for gb_col in groupby_col:
            for agg_col in aggregate_col:
                qr="select {}, {}({}),{}({}),{}({}) from reference_data group by {}".format(gb_col,"sum",agg_col,"count",agg_col,"avg",agg_col,gb_col)
                act_sum={}
                act_cnt={}
                act_avg={}
                ref_sum={}
                ref_cnt={}
                ref_avg={}
                cursor.execute(qr) 
                ref_records = cursor.fetchall()
                
                for row in ref_records:
                        ref_sum[row[0]]=row[1]
                        ref_cnt[row[0]]=row[2]
                        ref_avg[row[0]]=row[3]

                        
                qa="select {}, {}({}),{}({}),{}({}) from actual_data group by {}".format(gb_col,"sum",agg_col,"count",agg_col,"avg",agg_col,gb_col)
                print(qa)
                cursor.execute(qa) 
                actual_records = cursor.fetchall()
                for row in actual_records:
                        act_sum[row[0]]=row[1]
                        act_cnt[row[0]]=row[2]
                        act_avg[row[0]]=row[3]

                print("For testing: ",gb_col,"-",agg_col,[v for k,v in ref_sum.items() if k in ref_sum and k in act_sum ])

        
    def sharing_optimization_modified(self,cursor,start,end):

        groupby_col=['workclass','education','occupation','relationship','race','sex','native_country']
        aggregate_col=['fnlwgt','education_num','capital_gain','capital_loss','hours_per_week']

        kl_divergence_dict={}
        
        for gb_col in groupby_col:
            for agg_col in aggregate_col:
                qr="create view {} as select {}, {}({}),{}({}),{}({}) from reference_data where id between {} and {} group by {}".format("ref_view_"+str(self.view_cnt),gb_col,"sum",agg_col,"count",agg_col,"avg",agg_col,start,end,gb_col)
                cursor.execute(qr) 
                        
                qa="create view {} as select {}, {}({}),{}({}),{}({}) from actual_data where id between {} and {} group by {}".format("act_view_"+str(self.view_cnt),gb_col,"sum",agg_col,"count",agg_col,"avg",agg_col,start,end,gb_col)
                cursor.execute(qa) 
                kl_divergence_dict[gb_col+"-"+agg_col]=["ref_view_"+str(self.view_cnt),"act_view_"+str(self.view_cnt)]
                self.view_cnt+=1
        return kl_divergence_dict

                # print("For testing: ",gb_col,"-",agg_col,[v for k,v in ref_sum.items() if k in ref_sum and k in act_sum ])
                
    def prunning(self,cursor,kl_divergence_dict,phase_num,adi=10):
        
        
        for k,v in kl_divergence_dict.items():
            gb_col,agg_col=k.split("-")
            qr="select * from {}".format(v[0])
            act_sum={}
            act_cnt={}
            act_avg={}
            ref_sum={}
            ref_cnt={}
            ref_avg={}
            cursor.execute(qr) 
            ref_records = cursor.fetchall()
            
            for row in ref_records:
                    ref_sum[row[0]]=row[1]
                    ref_cnt[row[0]]=row[2]
                    ref_avg[row[0]]=row[3]

                    
            qa="select * from {}".format(v[1])
            cursor.execute(qa) 
            actual_records = cursor.fetchall()
            for row in actual_records:
                    act_sum[row[0]]=row[1]
                    act_cnt[row[0]]=row[2]
                    act_avg[row[0]]=row[3]
            
            if gb_col+"-"+agg_col+"-"+"sum" in self.prunning_dict.keys() and gb_col+"-"+agg_col+"-"+"sum" not in self.deleted_attr:
                self.prunning_dict[gb_col+"-"+agg_col+"-"+"sum"].append(self.kl_divergence([v for k,v in sorted(ref_sum.items()) if k in ref_sum and k in act_sum ],[v for k,v in sorted(act_sum.items()) if k in ref_sum and k in act_sum ]))
            elif gb_col+"-"+agg_col+"-"+"sum" not in self.deleted_attr :
                lst=[]
                lst.append(self.kl_divergence([v for k,v in sorted(ref_sum.items()) if k in ref_sum and k in act_sum ],[v for k,v in sorted(act_sum.items()) if k in ref_sum and k in act_sum ]))
                self.prunning_dict[gb_col+"-"+agg_col+"-"+"sum"]=lst
                
            if gb_col+"-"+agg_col+"-"+"cnt" in self.prunning_dict.keys() and gb_col+"-"+agg_col+"-"+"cnt" not in self.deleted_attr:
                self.prunning_dict[gb_col+"-"+agg_col+"-"+"cnt"].append(self.kl_divergence([v for k,v in sorted(ref_cnt.items()) if k in ref_cnt and k in act_cnt ],[v for k,v in sorted(act_cnt.items()) if k in ref_cnt and k in act_cnt ]))
            elif gb_col+"-"+agg_col+"-"+"cnt" not in self.deleted_attr:
                lst=[]
                lst.append(self.kl_divergence([v for k,v in sorted(ref_cnt.items()) if k in ref_cnt and k in act_cnt ],[v for k,v in sorted(act_cnt.items()) if k in ref_cnt and k in act_cnt ]))
                self.prunning_dict[gb_col+"-"+agg_col+"-"+"cnt"]=lst
                
            if gb_col+"-"+agg_col+"-"+"avg" in self.prunning_dict.keys() and gb_col+"-"+agg_col+"-"+"avg" not in self.deleted_attr:
                self.prunning_dict[gb_col+"-"+agg_col+"-"+"avg"].append(self.kl_divergence([v for k,v in sorted(ref_avg.items()) if k in ref_avg and k in act_avg ],[v for k,v in sorted(act_avg.items()) if k in ref_avg and k in act_avg ]))
            elif gb_col+"-"+agg_col+"-"+"avg" not in self.deleted_attr:
                lst=[]
                lst.append(self.kl_divergence([v for k,v in sorted(ref_avg.items()) if k in ref_avg and k in act_avg ],[v for k,v in sorted(act_avg.items()) if k in ref_avg and k in act_avg ]))
                self.prunning_dict[gb_col+"-"+agg_col+"-"+"avg"]=lst

        confidence_intervals = collections.defaultdict(list)
        
        for k,v in self.prunning_dict.items():
            # import pdb;pdb.set_trace()
            print(k,v)
            delta=0.05
            numerator = (1 - ((phase_num - 1)/adi))*(2*np.log(np.log(phase_num)) + np.log((np.pi*np.pi) / (3*delta)))
            epsilon_m = np.sqrt(numerator / (2 * phase_num))
            # epsilon_m = self.get_confidence(len(v), phase_num, 0.05)
            mean_utility_measure=sum(v)/phase_num
            #std_err = scipy.stats.sem(np.array(v))
            #h = std_err * scipy.stats.t.ppf((1 + confidence) / 2., len(v) - 1)
            # import pdb;pdb.set_trace()
            confidence_intervals[k] = [mean_utility_measure-epsilon_m, mean_utility_measure+epsilon_m]
        # return confidence_intervals
        for kk,v in confidence_intervals.items():
            count=0
            # import pdb;pdb.set_trace()
            for key,value in confidence_intervals.items():
                if confidence_intervals[kk][1] < confidence_intervals[key][0]:
                    count += 1
                if count >= adi:
                    break
            if count>=adi:
                self.deleted_attr.append(kk)
        # print(self.deleter_attr)
        return self.deleted_attr
    
    def get_confidence(self, m, N, delta):
        t1=1-((m-1)/N)
        print(m)
        m=abs(m)
        t2=2*math.log(math.log(m))
        t3=math.log(math.pi**2/(3*delta))
        numerator=t1*t2+t3
        denominator=2*m
        epsilon_m=(numerator/denominator)**0.5
        return epsilon_m

    # def update_stats(dict_kl):
    #     dict_stats=defaultdict(dict)
    #     for key,vals in dict_kl.items():
    #         dict_stats[key]['mean']=sum(vals)/len(vals)
    #         CI = get_confidence(len(dict_kl[key]), num_partitions, delta)
    #         dict_stats[key]['lower_CI'] = dict_stats[key]['mean'] - CI
    #         dict_stats[key]['upper_CI'] = dict_stats[key]['mean'] + CI
    #     return dict_stats
                

        
                            
    def cleaning(self):
        for k,v in self.prunning_dict.copy().items():
            if k in self.deleted_attr:
                self.prunning_dict.pop(k)
                
                
    
    def plot(self,values,cursor):
        a=values.split("-")[0]
        m=values.split("-")[1]
        a,m,fm=values.split("-")
        ref_query="select {}, {}({}) from reference_data group by {}".format(a,fm,m,a)
        cursor.execute(ref_query) 
        ref_records = cursor.fetchall()
        print("Print each row and it's columns values")
        ref_index=[]
        ref_data=[]
        for row in ref_records:
            ref_index.append(row[0])
        act_query="select {}, {}({}) from actual_data group by {}".format(a,fm,m,a)
        cursor.execute(act_query) 
        act_records = cursor.fetchall()
        print("Print each row and it's columns values")
        act_index=[]
        act_data=[]
        for row in act_records:
            act_index.append(row[0])
        n_groups = len(act_index)
        groups=list(set(act_index).intersection(set(ref_index)))
        print(groups)
        print(len(groups))
        for row in ref_records:
            if row[0] in groups:
                ref_data.append(row[1])
        for row in act_records:
            if row[0] in groups:
                act_data.append(row[1])
        fig, ax = plt.subplots()
        import numpy as np
        index = np.arange(n_groups)
        
        X = groups
        print("X: ",groups)
        Ygirls = act_data
        print("Act data: ",act_data)
        Zboys = ref_data
        print("Ref data: ",ref_data)
        print(len(X)," ",len(act_data)," ",len(ref_data))
          
        X_axis = np.arange(len(X))
          
        plt.bar(X_axis - 0.2, Ygirls, 0.4, label = 'Married')
        plt.bar(X_axis + 0.2, Zboys, 0.4, label = 'NotMarried')
          
        plt.xticks(X_axis, X)
        plt.xlabel(a)
        plt.ylabel(m)
        plt.title("Number of {} in each {}".format(m,a))
        plt.legend()
        plt.show()
        print("plotted data")


        # bar_width = 0.35
        # opacity = 0.8
        # rects1 = plt.bar(index, act_data, bar_width,
        #                  alpha=opacity,
        #                  color='b',
        #                  label='Married')
        # rects2 = plt.bar(index + bar_width, ref_data, bar_width,
        #                  alpha=opacity,
        #                  color='g',
        #                  label='Unmarried')
        # plt.xlabel(a)
        # plt.ylabel('{}({})'.format(fm,m))
        # plt.title('{} by {} for married and unmarried.'.format(m,a))
        # plt.xticks(index + bar_width,act_index,rotation=45,horizontalalignment="right")
        # plt.legend()
        # plt.tight_layout()
        