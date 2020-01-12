# coding:utf-8
# !/usr/bin/python
# -*- coding:UTF-8 -*-




import sys
import math
from pyspark import SparkContext
from pyspark.sql import HiveContext
from math import radians,asin,sqrt,cos,sin
from pyspark.sql import Row
import numpy as np
import pandas as pd
import os
reload(sys)
sys.setdefaultencoding('utf-8')
#family_name
product_family_size = 11
product_family_inverse_index = [(0, 'VQWAaLm'), (1, 'VQzAaLm'), (2, 'VW0AaLm'), (3, 'VW1AaLm'), (4, 'VWdAaLm'), (5, 'VWpAaLm'), (6, 'Vp3AaLm'), (7, 'Vp9AaLm'), (8, 'VpWAaLm'), (9, 'VpdAaLm'), (10, 'VzWAaLm')]
product_family_index   = [('VQWAaLm', 0), ('VQzAaLm', 1), ('VW0AaLm', 2), ('VW1AaLm', 3), ('VWdAaLm', 4), ('VWpAaLm', 5), ('Vp3AaLm', 6), ('Vp9AaLm', 7), ('VpWAaLm', 8), ('VpdAaLm', 9), ('VzWAaLm', 10)]

#model_name
product_model_size = 64
product_model_inverse_index =[(0, 'jQ9Q0U9z3WS'), (1, 'jQU90dU03dS'), (2, 'jQU90QpQ9QS'), (3, 'jQU999p9zW5'), (4, 'jQU999p9zWB'), (5, 'jQU999p9zWO'), (6, 'jQU99p9dpQ5'), (7, 'jQ9Q0U90W1S'), (8, 'jQU901UpWUh'), (9, 'jQU901UpWUS'), (10, 'jQU91ppQU0S'), (11, 'jQU903dpd3S'), (12, 'jQU90Qp31pS'), (13, 'jQU90Qp3UzS'), (14, 'jQU90QpdQ0S'), (15, 'jQU90QpdzQh'), (16, 'jQU9103p9pS'), (17, 'jQU9109910S'), (18, 'jQU99013d0F'), (19, 'jQU99013d3h'), (20, 'jQU9UdQd0dF'), (21, 'jQU9UQdQp05'), (22, 'jQU9UUpW90F'), (23, 'jQU90011WUh'), (24, 'jQU90dQWQUS'), (25, 'jQU90Q9U90S'), (26, 'jQU90Q9U91S'), (27, 'jQU90Q9UUzS'), (28, 'jQU90QpQ93S'), (29, 'jQU990p1Q0O'), (30, 'jQU99101UQF'), (31, 'jQU99Ud09du'), (32, 'jQU9Uzpzp0u'), (33, 'jQU901W1WUB'), (34, 'jQU901W1WUc'), (35, 'jQU901Wp91c'), (36, 'jQU901Wp91i'), (37, 'jQU903939Uu'), (38, 'jQU90QU3pQO'), (39, 'jQU91pdWzQ5'), (40, 'jQU91pdWzQF'), (41, 'jQU91pdWzQx'), (42, 'jQU919UUUUi'), (43, 'jQU91Q1d03u'), (44, 'jQU91Wp30Qx'), (45, 'jQU9pd9d1WS'), (46, 'jQU9pUW9ddu'), (47, 'jQU9pUW9pQO'), (48, 'jQU9UdQUd3O'), (49, 'jQU9UdQUd3OuBcRS'), (50, 'jQU9UdQUdUO'), (51, 'jQU9UdQUdUOuBcRS'), (52, 'jQU9UUzzzWu'), (53, 'jQU90Qppd1h'), (54, 'jQU91Q1dUQu'), (55, 'jQU9UUzzpzh'), (56, 'jQU90Qppdph'), (57, 'jQU99101U3O'), (58, 'jQU99p0919u'), (59, 'jQU99UU0995'), (60, 'jQU99z100U5'), (61, 'jQU91pdzpz5'), (62, 'jQU91pdzpzF'), (63, 'jQU9p1dddQS')]
prodct_model_index = [('jQ9Q0U9z3WS', 0), ('jQU90dU03dS', 1), ('jQU90QpQ9QS', 2), ('jQU999p9zW5', 3), ('jQU999p9zWB', 4), ('jQU999p9zWO', 5), ('jQU99p9dpQ5', 6), ('jQ9Q0U90W1S', 7), ('jQU901UpWUh', 8), ('jQU901UpWUS', 9), ('jQU91ppQU0S', 10), ('jQU903dpd3S', 11), ('jQU90Qp31pS', 12), ('jQU90Qp3UzS', 13), ('jQU90QpdQ0S', 14), ('jQU90QpdzQh', 15), ('jQU9103p9pS', 16), ('jQU9109910S', 17), ('jQU99013d0F', 18), ('jQU99013d3h', 19), ('jQU9UdQd0dF', 20), ('jQU9UQdQp05', 21), ('jQU9UUpW90F', 22), ('jQU90011WUh', 23), ('jQU90dQWQUS', 24), ('jQU90Q9U90S', 25), ('jQU90Q9U91S', 26), ('jQU90Q9UUzS', 27), ('jQU90QpQ93S', 28), ('jQU990p1Q0O', 29), ('jQU99101UQF', 30), ('jQU99Ud09du', 31), ('jQU9Uzpzp0u', 32), ('jQU901W1WUB', 33), ('jQU901W1WUc', 34), ('jQU901Wp91c', 35), ('jQU901Wp91i', 36), ('jQU903939Uu', 37), ('jQU90QU3pQO', 38), ('jQU91pdWzQ5', 39), ('jQU91pdWzQF', 40), ('jQU91pdWzQx', 41), ('jQU919UUUUi', 42), ('jQU91Q1d03u', 43), ('jQU91Wp30Qx', 44), ('jQU9pd9d1WS', 45), ('jQU9pUW9ddu', 46), ('jQU9pUW9pQO', 47), ('jQU9UdQUd3O', 48), ('jQU9UdQUd3OuBcRS', 49), ('jQU9UdQUdUO', 50), ('jQU9UdQUdUOuBcRS', 51), ('jQU9UUzzzWu', 52), ('jQU90Qppd1h', 53), ('jQU91Q1dUQu', 54), ('jQU9UUzzpzh', 55), ('jQU90Qppdph', 56), ('jQU99101U3O', 57), ('jQU99p0919u', 58), ('jQU99UU0995', 59), ('jQU99z100U5', 60), ('jQU91pdzpz5', 61), ('jQU91pdzpzF', 62), ('jQU9p1dddQS', 63)]

#phase_name
phase_name_size = 29
phase_name = set(['UU9', 'UU1', 'UU3', 'UQQ', 'Uzd', 'UQU', 'Up3', 'UQW', 'Up1', 'U91', 'U99', 'U0p', 'Udd', 'U0Q', 'U9U', 'UUz', 'UUp', 'U09', 'U00', 'Ud0', 'UUU', '9UU', 'UQ9', 'Udz', 'U9d', 'Uzz', 'Up0', 'U1W', 'UpQ'])
phase_name_inverse_index = list(enumerate(phase_name))
phase_name_index = [ (item[1],item[0]) for item in phase_name_inverse_index]



def format_train(train_path):
    pd_list = []

    for f in os.listdir(train_path):
        if '~' not in f:
            file = train_path+f
            print file
            process_table = pd.read_excel(file,sheet_name='Process_Table')
            phase_table =  pd.read_excel(file,sheet_name='Phase_Table').query('ID_F_PHASE_S < 10')


            t = process_table.merge(phase_table,on='ID_F_PROCESS')[['ID_F_PROCESS','PROCESS_RESULT_STATE','TYPE_NUMBER','PRODUCTGROUP_NAME',
                                                                   'PHASE_RESULT_STATE','PHASE_NAME','ID_F_PHASE_S']]

            t.columns= ['product_id', 'label', 'model_name', 'family_name','phase_label','phase_name','phase_index']
            cureent_arr = []
            for k, v in t.groupby(['product_id', 'label', 'model_name', 'family_name']):

                tmp =  v[['phase_label','phase_name','phase_index']].sort_values(by = 'phase_index').values.tolist()
                #查看是否缺0~9phase
                phase_arr = [item[-1] for item in tmp]
                for i in range(10):
                    if i not in phase_arr:
                        tmp.insert(i,[None,None,None])
                ###以None 作为补充

                arr = [item for sublist in tmp for item in sublist]
                cureent_arr.append([k[1],k[0],k[3],k[2]]+ arr)
            cur_pd = pd.DataFrame(cureent_arr, columns =['label', 'product_id', 'family_name','model_name','phase_1_label','phase_1_name','phase_1_index',
                                                         'phase_2_label', 'phase_2_name', 'phase_2_index','phase_3_label','phase_3_name','phase_3_index',
                                                         'phase_4_label', 'phase_4_name', 'phase_4_index','phase_5_label','phase_5_name','phase_5_index',
                                                         'phase_6_label', 'phase_6_name', 'phase_6_index','phase_7_label','phase_7_name','phase_7_index',
                                                         'phase_8_label', 'phase_8_name', 'phase_8_index','phase_9_label','phase_9_name','phase_9_index',
                                                         'phase_10_label', 'phase_10_name', 'phase_10_index'])
            pd_list.append(cur_pd)
    all_data = pd.concat(pd_list)
    all_data.to_csv('data_format.csv',index=False)
    print 'done'


def foramt_test_4(file_path):
    df = pd.read_csv(file_path,sep=',')[['Product_ID','TYPE_NUMBER','PRODUCTGROUP_NAME','PHASE_RESULT_STATE','PHASE_NAME','ID_F_PHASE_S']].query('ID_F_PHASE_S < 3').drop_duplicates()
    df.columns = ['product_id', 'model_name', 'family_name', 'phase_label', 'phase_name', 'phase_index']
    cureent_arr = []
    for k, v in df.groupby(['product_id', 'model_name', 'family_name']):

        tmp = v[['phase_label', 'phase_name', 'phase_index']].sort_values(by='phase_index').values.tolist()
        # 查看是否缺0~9phase
        phase_arr = [item[-1] for item in tmp]
        for i in range(3):
            if i not in phase_arr:
                tmp.insert(i, [None, None, None])
        ###以None 作为补充

        arr = [item for sublist in tmp for item in sublist]
        cureent_arr.append([ k[0], k[2], k[1]] + arr)
    cur_pd = pd.DataFrame(cureent_arr,
                          columns=['product_id', 'family_name', 'model_name', 'phase_1_label', 'phase_1_name',
                                   'phase_1_index',
                                   'phase_2_label', 'phase_2_name', 'phase_2_index', 'phase_3_label', 'phase_3_name',
                                   'phase_3_index'])
    cur_pd.to_csv('valid_4_format.csv',index=False)
    print 'done'


def format_test_11(file_path):
    df = pd.read_csv(file_path, sep=',')[
        ['Product_ID', 'TYPE_NUMBER', 'PRODUCTGROUP_NAME', 'PHASE_RESULT_STATE', 'PHASE_NAME', 'ID_F_PHASE_S']].query(
        'ID_F_PHASE_S < 10').drop_duplicates()
    df.columns = ['product_id', 'model_name', 'family_name', 'phase_label', 'phase_name', 'phase_index']
    cureent_arr = []
    for k, v in df.groupby(['product_id', 'model_name', 'family_name']):

        tmp = v[['phase_label', 'phase_name', 'phase_index']].sort_values(by='phase_index').values.tolist()
        # 查看是否缺0~9phase
        phase_arr = [item[-1] for item in tmp]
        for i in range(10):
            if i not in phase_arr:
                tmp.insert(i, [None, None, None])
        ###以None 作为补充

        arr = [item for sublist in tmp for item in sublist]
        cureent_arr.append([k[0], k[2], k[1]] + arr)
    cur_pd = pd.DataFrame(cureent_arr,
                          columns=[ 'product_id', 'family_name','model_name','phase_1_label','phase_1_name','phase_1_index',
                                                         'phase_2_label', 'phase_2_name', 'phase_2_index','phase_3_label','phase_3_name','phase_3_index',
                                                         'phase_4_label', 'phase_4_name', 'phase_4_index','phase_5_label','phase_5_name','phase_5_index',
                                                         'phase_6_label', 'phase_6_name', 'phase_6_index','phase_7_label','phase_7_name','phase_7_index',
                                                         'phase_8_label', 'phase_8_name', 'phase_8_index','phase_9_label','phase_9_name','phase_9_index',
                                                         'phase_10_label', 'phase_10_name', 'phase_10_index'])
    cur_pd.to_csv('valid_11_format.csv', index=False)
    print 'done'





'''

def get_phase_name(train_path):
    ret =set()
    for f in os.listdir(train_path):
        if '~' not in f:
            file = train_path+f
            print file
            parameters_table = pd.read_excel(file, sheet_name='Phase_Table')
            tmp = parameters_table[['PHASE_NAME']].drop_duplicates().values.tolist()
            t = [str(item) for sublist in  tmp for item in sublist]
            ret.update(t)
    tt = enumerate(ret)
 



def get_product_family_model(train_path):
    product_family_map = {}
    for f in os.listdir(train_path):
        if '~' not in f:
            product_family = f.split(".")[0].split('_')[0].strip()
            product_model = f.split(".")[0].split('_')[1].strip()

            # print product_family,product_model
            if product_family not in product_family_map.keys():
                product_family_map[product_family] = [product_model]
            else:
                product_family_map[product_family].append(product_model)

    product_family_size = len(product_family_map.keys())
    product_family_inverse_index = list(enumerate(sorted(product_family_map.keys())))
    product_family_index = [(item[1],item[0])for item in product_family_inverse_index]



    product_model_size = sum([len(item) for item in product_family_map.values()])
    product_model_inverse_index  = list(enumerate([x for j in product_family_map.values() for x in j]))

    prodct_model_index = [(item[1],item[0])for item in product_model_inverse_index]
    print product_family_inverse_index
    print product_family_index
    print product_model_inverse_index
    print prodct_model_index


    return product_family_size,product_family_inverse_index,product_family_index,\
           product_model_size,product_model_inverse_index,prodct_model_index
'''



def trans_1(file_in,file_out):
    df =pd.read_csv(file_in)
    df['label'] = df['label']-1
    df.to_csv(file_out,index=False)

def trans_2(file_in,file_out):
    df = pd.read_csv(file_in)
    df['phase_1_label']=df['phase_1_label']-1
    df['phase_2_label']=df['phase_2_label']-1
    df['phase_3_label']=df['phase_3_label']-1
    df['phase_4_label']=df['phase_4_label']-1
    df['phase_5_label']=df['phase_5_label']-1
    df['phase_6_label']=df['phase_6_label']-1
    df['phase_7_label']=df['phase_7_label']-1
    df['phase_8_label']=df['phase_8_label']-1
    df['phase_9_label']=df['phase_9_label']-1
    df['phase_10_label']=df['phase_10_label']-1
    df[['label','product_id', 'family_name','model_name','phase_1_label','phase_1_name',
        'phase_2_label', 'phase_2_name', 'phase_3_label','phase_3_name',
        'phase_4_label', 'phase_4_name','phase_5_label','phase_5_name',
        'phase_6_label', 'phase_6_name','phase_7_label','phase_7_name',
        'phase_8_label', 'phase_8_name','phase_9_label','phase_9_name',
        'phase_10_label', 'phase_10_name']].to_csv(file_out,index=False)


def trans_2(file_in,file_out):
    df = pd.read_csv(file_in)
    df['phase_1_label']=df['phase_1_label']-1
    df['phase_2_label']=df['phase_2_label']-1
    df['phase_3_label']=df['phase_3_label']-1
    df['phase_4_label']=df['phase_4_label']-1
    df['phase_5_label']=df['phase_5_label']-1
    df['phase_6_label']=df['phase_6_label']-1
    df['phase_7_label']=df['phase_7_label']-1
    df['phase_8_label']=df['phase_8_label']-1
    df['phase_9_label']=df['phase_9_label']-1
    df['phase_10_label']=df['phase_10_label']-1
    df[['label','product_id', 'family_name','model_name','phase_1_label','phase_1_name',
        'phase_2_label', 'phase_2_name', 'phase_3_label','phase_3_name',
        'phase_4_label', 'phase_4_name','phase_5_label','phase_5_name',
        'phase_6_label', 'phase_6_name','phase_7_label','phase_7_name',
        'phase_8_label', 'phase_8_name','phase_9_label','phase_9_name',
        'phase_10_label', 'phase_10_name']].to_csv(file_out,index=False)

def test4_trans_2(file_in,file_out):
    df = pd.read_csv(file_in)
    df['phase_1_label']=df['phase_1_label']-1
    df['phase_2_label']=df['phase_2_label']-1
    df['phase_3_label']=df['phase_3_label']-1

    df[['product_id', 'family_name','model_name','phase_1_label','phase_1_name',
        'phase_2_label', 'phase_2_name', 'phase_3_label','phase_3_name']].to_csv(file_out,index=False)

def test11_trans_2(file_in,file_out):
    df = pd.read_csv(file_in)
    df['phase_1_label']=df['phase_1_label']-1
    df['phase_2_label']=df['phase_2_label']-1
    df['phase_3_label']=df['phase_3_label']-1
    df['phase_4_label']=df['phase_4_label']-1
    df['phase_5_label']=df['phase_5_label']-1
    df['phase_6_label']=df['phase_6_label']-1
    df['phase_7_label']=df['phase_7_label']-1
    df['phase_8_label']=df['phase_8_label']-1
    df['phase_9_label']=df['phase_9_label']-1
    df['phase_10_label']=df['phase_10_label']-1
    df[['product_id', 'family_name','model_name','phase_1_label','phase_1_name',
        'phase_2_label', 'phase_2_name', 'phase_3_label','phase_3_name',
        'phase_4_label', 'phase_4_name','phase_5_label','phase_5_name',
        'phase_6_label', 'phase_6_name','phase_7_label','phase_7_name',
        'phase_8_label', 'phase_8_name','phase_9_label','phase_9_name',
        'phase_10_label', 'phase_10_name']].to_csv(file_out,index=False)



if __name__ =='__main__':
    #train_path = '/Users/gaominghui/Downloads/INSPEC_Data/INSPEC_train/'
    file_4_path = '/Users/gaominghui/Downloads/INSPEC_Data/INSPEC_validation/validation_predict_4.csv'
    file_11_path = '/Users/gaominghui/Downloads/INSPEC_Data/INSPEC_validation/validation_predict_11.csv'
    #format_train(train_path)
    #foramt_test_4(file_4_path)
    #format_test_11(file_11_path)
    #get_product_family_model(train_path)
    #get_phase_name(train_path)
    #trans_1('./data_format.csv','./data_format_label01.csv')
    #trans_2('./data_format_label01.csv','./data_format_label01_2.csv')
    test4_trans_2('./valid_4_format.csv','./valid_4_format_2.csv')
    test11_trans_2('./valid_11_format.csv','./valid_11_format_2.csv')








