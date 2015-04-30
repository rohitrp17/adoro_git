# Code Details: Connects to RedShift Databse
# Input: User Interaction Table from RedShift
# Output: Recommendations Stored in RedShift

# YET TO HANDLE Error in Connection

import psycopg2
import psycopg2.extras
import sys
 
def DbConnect():
    #Define our connection string
    conn_string = "host='adoro-test.csgcsxyjqmsd.us-east-1.redshift.amazonaws.com' dbname='userinteraction' port='5439' user='adoro' password='Ad0r0#123'"
 
    # print the connection string we will use to connect
    print "Connecting to database\n    ->%s" % (conn_string)
 
    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)
 
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    # conn.cursor will return a cursor object, you can use this query to perform queries
    # note that in this example we pass a cursor_factory argument that will
    # dictionary cursor so COLUMNS will be returned as a dictionary so we
    # can access columns by their name instead of index.
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor) 
    
    print "Connected!\n"
    r = [cursor,conn]
    return r;

#####################################################################################################################################
############################################# Access Redshift and Query sample ######################################################
#####################################################################################################################################
### Select sample query - Start ###
# query = "Select attribute_val_id from attribute_val_ref where attribute_value ='{0}'".format(brand[i])
# cursor[0].execute(query)
# res = cursor[0].fetchall()
### Select sample query - End ###

### Insert sample query - Start ###
# insert_q = "insert into prod_attribute_details(product_id, attribute_id, attribute_val_id, category_id) values ('{0}',{1},{2},{3})".format(x,1,int(res[0][0]),1)
# cursor[0].execute(insert_q)
# cursor[1].commit()
### Insert sample query - End ###
