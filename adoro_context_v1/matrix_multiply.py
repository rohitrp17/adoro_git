'''
Created on 30-Apr-2015

@author: Nitesh
'''
### This code intend to fetch data from redshift, create matrix and multiply them to get final user-product score ###
### Yet to do
# add attribute weights
# think over normalizationof score for each attribute
# storage of recos


### NumPy is used to ease matrix operations. It can be installed using sudo pip install numpy ###
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
    
    ### Query product details and fetchall in res ### 
    query = "Select product_id,attribute_id,attribute_val_id from prod_attribute_details1 where category_id ='{0}'".format("1")
    cursor[0].execute(query)
    res = cursor[0].fetchall()

    ### Process the result and create a dictionary to store list of attr.attr_value against prod_id ###
    ### Create column_list to store all attr.attr_value and then find its unique to know all the unique attr.attr_value ###
    column_list = []
    prod_dict = {}

    for row in res:
        attr_attrval = str(row[1])+"."+str(row[2])
        column_list.append(attr_attrval)
        if row[0] in prod_dict.keys():
            prod_dict[row[0]].append(attr_attrval)
        else:
            prod_dict[row[0]] = [attr_attrval]
    column_list = list(set(column_list))

    ### Create 0/1 2D list for product attributes ###
    prod_list = prod_dict.keys()
    prod_attr = []
    for pid in prod_list:
        pid_attr_list = []
        for col in column_list:
            if col in prod_dict[pid]:
                pid_attr_list.append(1)
            else:
                pid_attr_list.append(0)
        prod_attr.append(pid_attr_list)
    
    ### Convert 2D list to a numpy matrix, this will be used to multiply ###
    product_attribute_matrix = np.matrix(prod_attr)
    # print product_attribute_matrix

    ### Query user level attribute weights and multiply them with user score while storing ###
    user_attr_weights_query = "Select user_id, attribute_id, attribute_weight from user_attribute_weights where category_id = '{0}'".format("1")
    cursor[0].execute(user_attr_weights_query)
    user_attr_weights_res = cursor[0].fetchall()

    user_attr_weights = {}
    for row in user_attr_weights_res:
        if row[0] not in user_attr_weights.keys():
            user_attr_weights[row[0]] = {}
        user_attr_weights[row[0]][row[1]] = row[2]
    # print user_attr_weights

    ### Query user preference and fetchall in res ### 
    query = "Select user_id,attribute_id,attribute_val_id,preference_score from user_attribute_preference where category_id ='{0}'".format("1")
    cursor[0].execute(query)
    res = cursor[0].fetchall()

    ### Process the result and create a dictionary to store dictionary of attr.attr_value and its score against user_id ###
    user_dict = {}

    for row in res:
        attr_attrval = str(row[1])+"."+str(row[2])
        if row[0] not in user_dict.keys():
            user_dict[row[0]] = {}
        user_dict[row[0]][attr_attrval] = row[3]
    # print user_dict
        if row[0] in user_attr_weights.keys():
            if row[1] in user_attr_weights[row[0]].keys():
                user_dict[row[0]][attr_attrval] = user_dict[row[0]][attr_attrval] * user_attr_weights[row[0]][row[1]]
    # print user_dict
    

    ### Create score 2d list for user preference ###
    user_list = user_dict.keys()
    user_attr_pref = []
    for uid in user_list:
        user_attr_list = []
        for col in column_list:
            if col in user_dict[uid].keys():
                user_attr_list.append(user_dict[uid][col])
            else:
                user_attr_list.append(0)
        user_attr_pref.append(user_attr_list)
    # print user_attr_pref

    ### Convert 2D list to a numpy matrix, this will be used to multiply ###
    user_attribute_preference_matrix = np.matrix(user_attr_pref)
    # print user_attribute_preference_matrix

    ### To multiply matrices transform user preference matrix ###
    user_attribute_preference2T = user_attribute_preference_matrix.T
    # print user_attribute_preference2T
    
    ### Multiply both the matrices, transpose it to get each row for a user and then convert it into a python 2D list ###
    final = np.dot(product_attribute_matrix,user_attribute_preference2T)
    user_prod_list = final.T.tolist()

    ### Take max scored product and find its index and fetch product id using that index, this will be the top recommendation ###
    ### This logic is temporary, we need to arrange them in ascending order and the also handle tie case ###
    count = 0
    for each_user_prod in user_prod_list:
        outputfile = open("C:\\Users\\Nitesh\\Documents\\Nitesh\\contextual_workspace\\adoro_context_v1\\Recommendation\\"+user_list[count]+"_recos"+".csv","wb")
        writer = csv.writer(outputfile)
        writer.writerow(["User_id","Product_id","Score"])
        sorted_set = sorted(enumerate(each_user_prod), key=lambda x:-x[1])
        for value_set in sorted_set:
            value_list = list(value_set)
            writer.writerow([user_list[count],str(prod_list[value_list[0]]),str(value_list[1])])
        outputfile.close()
        count = count + 1

    ### To check the output, query user_id,product_id,interaction and count and write into files
    query = "Select user_id,product_id,interaction_id,time_stamp from user_prod_interaction where category_id ='{0}'".format("1")
    cursor[0].execute(query)
    res = cursor[0].fetchall()
    user_interaction = {}
    for row in res:
        if row[0] not in user_interaction.keys():
            user_interaction[row[0]] = []
        user_interaction[row[0]].append(tuple([row[1],row[2],row[3]]))
    for uid in user_interaction.keys():
        outputfile = open("C:\\Users\\Nitesh\\Documents\\Nitesh\\contextual_workspace\\adoro_context_v1\\Interaction\\"+uid+"_interaction.csv","wb")
        writer = csv.writer(outputfile)
        writer.writerow(["User_id","Product_id","Interaction_id","Timestamp"])
        temp_list = user_interaction[uid]
        for each_value in temp_list:
            value_list = list(each_value)
            writer.writerow([uid,value_list[0],value_list[1],value_list[2]])
        outputfile.close()
        

    ### To calculate the time taken to run the code and print it ###
    stop = timeit.default_timer()
    time_taken = stop -start
    print "Total time taken = {0} sec".format(time_taken)

if __name__ == "__main__":
    main()
