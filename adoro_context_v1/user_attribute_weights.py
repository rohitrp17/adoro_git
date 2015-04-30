'''
Created on 30-Apr-2015

@author: Nitesh
'''
### This code intend to fetch data from redshift and create user level attribute weights score ###
### Yet to do
# For each category
# Change hard coded category while insert
# Delete before insert


### NumPy is used to ease matrix operations. It can be installed using sudo pip install numpy ###
### Here NumPy is also used for statistical operations like finding variance ###
import numpy as np
import sys
import timeit
import csv

import RedShift_Db_Connect

def main():

    ### Calculate the time to run the code - Get store time ###
    start = timeit.default_timer()

    ### Connect to database ###
    cursor = RedShift_Db_Connect.DbConnect()
    
    ### Query user attribute preference, fetch each user and for each of then get all the attribute details and store it in a dictionary ### 
    query = "Select distinct(user_id) from user_attribute_preference where category_id ='{0}'".format("1")
    cursor[0].execute(query)
    res = cursor[0].fetchall()

    user_variance = {}
    attribute_list = []

    for each_user in res:
        uid = each_user[0]
        user_query = "Select attribute_id,attribute_val_id,preference_score from user_attribute_preference where category_id ='{0}' and user_id = '{1}'".format("1",uid)
        cursor[0].execute(user_query)
        user_res = cursor[0].fetchall()

        attr_dict = {}
        for each_row in user_res:
            if each_row[0] not in attr_dict:
                attr_dict[each_row[0]] = {}
            attr_dict[each_row[0]][each_row[1]] = each_row[2]
        
        # print attr_dict
        for each_attr in attr_dict.keys():
            attr_score_sum = 0
            attr_score_list = []
            for each_attr_val in attr_dict[each_attr].keys():
                attr_score_sum = attr_score_sum + attr_dict[each_attr][each_attr_val]
            for each_attr_val in attr_dict[each_attr].keys():
                attr_dict[each_attr][each_attr_val] = float(attr_dict[each_attr][each_attr_val])/attr_score_sum
                attr_score_list.append(attr_dict[each_attr][each_attr_val])
            attr_var = np.var(attr_score_list)
            # print "{0}--{1}--{2}".format(uid,each_attr,attr_var)
            if uid not in user_variance.keys():
                user_variance[uid] = {}
            user_variance[uid][each_attr] = attr_var
            attribute_list.append(each_attr)
    # print user_variance
    attr_list = list(set(attribute_list))
    # print attr_list
    for each_user in user_variance:
        for each_attr in attr_list:
            if each_attr not in user_variance[each_user].keys():
                user_variance[each_user][each_attr] = 0.0
    # print user_variance
    user_weights = {}
    for each_attr in attr_list:
        sum_avg = 0
        for each_user in user_variance:
            sum_avg = sum_avg + user_variance[each_user][each_attr]
        for each_user in user_variance:
            if each_user not in user_weights.keys():
                user_weights[each_user] = {}
            user_weights[each_user][each_attr] = abs(user_variance[each_user][each_attr] - sum_avg)
        # print each_attr + "," + str(sum_avg)
    # print user_weights
    ### Insert into Redshift table ###
    for each_user in user_weights.keys():
        for each_attr in user_weights[each_user].keys():
            insert_q = "insert into user_attribute_weights(user_id, attribute_id, attribute_weight,category_id) values ('{0}',{1},{2},{3})".format(each_user,each_attr,user_weights[each_user][each_attr],1)
            cursor[0].execute(insert_q)
            cursor[1].commit()
        
    cursor[1].close()
    ### To calculate the time taken to run the code and print it ###
    stop = timeit.default_timer()
    time_taken = stop -start
    print "Total time taken = {0} sec".format(time_taken)

if __name__ == "__main__":
    main()