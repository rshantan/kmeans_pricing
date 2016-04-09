
# coding: utf-8

# Kmeans Pricing Tool
# DRAFT
# Q1 2016 
# 

# In[ ]:

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
import psycopg2
from sqlalchemy import create_engine
import numpy as np
import csv
import boto
import time

""" Config """
REDSHIFTUSER = ''
REDSHIFTPWD = ''
AWSKEY= '' 
AWSSECRETKEY= ''
SCHEMA = ''
DATABASE = ''
HOST = ''
BUCKET = ''
PROJECT = '' 
KEYNAME = ''



# Create call to set Default Schema

# In[ ]:




# Function to connect to DB and extract data

# In[ ]:

class PricingModel(object):

    def __init__(self):
        """ Connect to the database and fetch the Product Details.
        Two different libraries are used to connect to redshift. sqlalchemy is used for its integration with pandas
        while psycopg2 is used to write the results to the db.
        """
        self.conn = psycopg2.connect(
            , 
            host=HOST,
            user=REDSHIFTUSER, 
            port=5439, 
            password=REDSHIFTPWD,
            database=DATABASE)
        self.cur = self.conn.cursor() # create a cursor for executing queries

        engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s"             % (REDSHIFTUSER, REDSHIFTPWD, HOST, 5439, DATABASE)
        self.engine = create_engine(engine_string)

        query_string = "select * from vw_product_details;"
        self.df = pd.read_sql_query(query_string, self.engine)

        ## Changed the queries to eliminate the trycatch statements, changed the froup by clause in string 3 and made csmetics changes
        q_string = """DROP TABLE IF EXISTS temp_table1;
                        select split_part(orderlinesku, '-', 1) as sku , orderlineqty, orderdate, totalorderlinegrossvalue,orderlinepromotioncodes, orderlineid, bhline.orderid
                        into temp_table
                        from vw_order_line  bhline 
                        join vw_order_header  bhheader 
                        on bhline.orderid=bhheader.orderid;"""
        
        q_string2 = """DROP TABLE IF EXISTS temp_table2;
                        select * 
                        into temp_table2 
                        from temp_table tt 
                        LEFT JOIN bh_returnheader ret 
                        ON ret.ordernumber = tt.orderid 
                        WHERE ret.ordernumber IS NULL;"""
        q_string3 = """DROP TABLE IF EXISTS temp_table3;
                        select sku, sum(orderlineqty) number_orders, 
                        sum(totalorderlinegrossvalue) as total_cost,
                        pgdate_part('week', orderdate) as week, 
                        pgdate_part('year', orderdate) as year 
                        into temp_table3
                        from temp_table2
                        group by sku,pgdate_part('week', orderdate),pgdate_part('year', orderdate);"""
        
        self.cur.execute(q_string)
        self.conn.commit()
        self.cur.execute(q_string2)
        self.conn.commit()
        self.cur.execute(q_string3)
        self.conn.commit()


# Clustering function

# In[1]:

def cluster(self):
    """ Perform clustering on the textual features describing the items of clothing.
    """
    df = self.df.fillna('') #  change all nans to empty strings
    # add all of the text features together
    text_features = df['name'] + ' ' + df['product_group'] + ' ' +                     df['category'] + ' ' + df['department'] + ' ' +                     df['length'] + ' ' + df['origin'] + ' ' +                     df['style'] + ' ' + df['neckline'] + ' ' +                     df['trend'] + ' ' + df['heelshape'] + ' ' +                     df['toe'] + ' ' + df['sole'] + ' ' +                     df['heelheight'] + ' ' + df['wash'] + ' ' +                     df['enduse']

    numerical_features = df[['cost_price', 'weight']]

    tf = TfidfVectorizer(min_df=2)
    self.X = tf.fit_transform(text_features)

    self.nbrs = NearestNeighbors()
    self.nbrs.fit(self.X)




# Pricing Function

# In[ ]:

def calculate_prices(self):
## Added comments, made the code readable. Also changed the order of variable creation for readability
    # res_df_cl_all is only used to be passed as as an argument in the calculate_clearance_price function.
    # Intialized it here in order to avoid multiple calls to the DB. Not related to the other res_df in the fn.
    query_string = """SELECT si.ItemID,Code,Price,sip.PriceBandId,pb.Name, bi.quantityonhand, si.manufacturer
                        FROM bh_StockItem si
                        join bh_StockItemPrice sip on si.ItemID = sip.ItemID
                        join bh_PriceBand pb on pb.PriceBandID = sip.PriceBandID
                        join bh_dwh_stock_levels bi on si.code = bi.productid
                        where pb.Name = 'Offer Price'""""

    res_df_cl_all = pd.read_sql_query(query_string, self.engine) 

    with open('boohoo-pricing.csv', 'wb') as csvfile:
        self.spamwriter = csv.writer(csvfile)

        for index, row in self.df.iterrows():
            # [(k, v.sku) for k,v in self.df.iterrows() if v.sku == 'DZZ90335']
            ## Initialize Values
            published = self.df['published'][index]
            in_clearance = self.df['in_clearance'][index]
            act_weeks_on_sale = self.df['act_weeks_on_sale'][index]
            original_standard_price = self.df['original_standard_price'][index]
            current_standard_price = self.df['current_standard_price'][index]
            cost_price = self.df['cost_price'][index]
            sku = self.df['sku'][index]

            self.name = self.df['name'][index]
            self.department = self.df['product_group'][index]
            self.current_price = current_standard_price ## Why are we doing this??

            # Find Similar Skus
            res = self.nbrs.kneighbors(self.X[index], return_distance=False)
            similar_skus = tuple([self.df.sku[a] for a in res[0]])

            # Get orders of similar skus, summarized by week
            query_string = "select * from  temp_table3 where sku  in ('{0}','{1}','{2}','{3}','{4}')".format(similar_skus[0], similar_skus[1], similar_skus[2], similar_skus[3], similar_skus[4]);
            res_df = pd.read_sql_query(query_string, self.engine)               

            total_weeks = len(res_df.groupby(['week', 'year']).count())
            # Calculate some values of Similar skus
            try:
                ideal_price = sum(res_df['total_cost'])/sum(res_df['number_orders'])
            except:
                ideal_price = self.df['cost_price'][index] + 10
            ## merged the try except statements
            try:
                sim_items_sold_per_week = sum(res_df['number_orders'])/len(res_df)
                self.sim_items_sold_per_week = sum(res_df['number_orders'])/len(res_df)
            except:
                sim_items_sold_per_week = 10 
                self.sim_items_sold_per_week = 10

            # Calculate values of actual sku
            sp = res_df[res_df['sku'] == sku]
            try:
                items_sold_per_week = sum(sp['number_orders'])/len(sp)
            except:
                items_sold_per_week = sim_items_sold_per_week 

            #implement pricing logic
            new_price = (ideal_price/sim_items_sold_per_week)*items_sold_per_week
            weeks_on_sale = sp.count()['sku']

            q_string = """select totalorderlinegrossvalue from temp_table2 where orderlineid = (select max(orderlineid) from  temp_table where sku = '{}' and orderlinepromotioncodes = '')""".format(sku)
            actual_df = pd.read_sql_query(q_string, self.engine)

            try:
                self.actual_price = actual_df['totalorderlinegrossvalue'].loc[0] 
            except:
                self.actual_price = 0

            try:
                ss = sp.loc[0]
                self.current_price = ss['total_cost']/ss['number_orders'] # practically, is this different from actual price?
            except Exception as e: 
                self.current_price = self.actual_price
            if self.current_price == 0:
                self.current_price = current_standard_price 

            # Introduce some rule based checks
            min_price = 0
            if act_weeks_on_sale < 5:
                min_price = self.price_point(self.df['product_group'][index].upper(), self.current_price)

            if new_price < min_price:
                new_price = min_price
            if new_price > (original_standard_price + original_standard_price/10):
                new_price = (original_standard_price + original_standard_price/10)

            res_df_cl = res_df_cl_all[res_df_cl_all['sku']==sku]    # to pass to the function below
            self.calculate_clearance_price(sku, res_df_cl, items_sold_per_week, new_price, ideal_price, sim_items_sold_per_week, cost_price, published, in_clearance, act_weeks_on_sale, original_standard_price, current_standard_price)
            ## Ideal price need not be passed



# Calculates the clearance price

# In[ ]:

def calculate_clearance_price(self, sku, res_df_cl, items_sold_per_week, new_price, ideal_price, sim_items_sold_per_week, cost_price, published, in_clearance, act_weeks_on_sale, original_standard_price, current_standard_price):
    ## Ideal price is not used
    try:
        price = res_df_cl['price'].loc[0]
    except: 
        price = 0
    clearance_price = price
    output_list = {}
    predicted_throughput = self.sim_items_sold_per_week 
    stock_level = sum(res_df_cl['confirmedqtyinstock'])
    try:
        weeks_of_stock = stock_level/items_sold_per_week
    except: 
        weeks_of_stock = 0
    current_velocity = items_sold_per_week ## What's the purpose of this?? Can be removed
    current_margin = self.current_price - cost_price
    for weeks in [2,4,6,8,10]:
        if stock_level < (weeks*items_sold_per_week):
            clearance_price = new_price
            tp = predicted_throughput 
        else:
            try:
                clearance_price = new_price*(items_sold_per_week/(stock_level/weeks))
                tp = (predicted_throughput - predicted_throughput * (items_sold_per_week/(stock_level/weeks))) + predicted_throughput 
            except: 
                clearance_price = new_price
                tp = predicted_throughput 

        if clearance_price < cost_price:
            clearance_price = cost_price
        if new_price < price:
            new_price = price                   
        output_list["week{0}".format(weeks)] = clearance_price
        output_list["margin{0}".format(weeks)] = clearance_price - cost_price
        output_list["tp".format(weeks)] = tp
        output_list["cd{0}".format(weeks)] = (clearance_price - self.current_price)*stock_level

    cost_decision = (new_price - self.current_price)*stock_level
    predicted_margin = new_price - cost_price
    other_outputvars = dict(sku = sku, new_price = new_price, current_price = self.current_price, actual_price = self.actual_price,weeks_of_stock = weeks_of_stock ,current_velocity = current_velocity,current_margin = current_margin,predicted_throughput = predicted_throughput,predicted_margin = predicted_margin, stock_level = stock_level, name = self.name, department = self.department,cost_decision = cost_decision)
    output_list.update(other_outputvars)


# In[ ]:

def price_point(self, group, current_price): ## What is the function supposed to do?
    query = """select * from price_architecture where sub_group = '{0}' order by price""".format(group)
    price_points = pd.read_sql_query(query, self.engine)
    prices = price_points['price']
    try:
        idx = np.where((np.abs(prices.tolist())-current_price)==((np.abs(prices.tolist())-current_price).argmin()))[0][0]
    except:
        return 0
    idx = idx - 2
    if idx < 0:
        idx = 0
    return price_points['price'][idx]


   


# Write function

# In[ ]:

def write_to_db(self):
   # removed try catch from sql and made some cosmetic changes.
   self.cur.execute("DROP TABLE IF EXISTS temp_table1;")
   self.conn.commit()

   self.cur.execute("DROP TABLE IF EXISTS temp_table2;")
   self.conn.commit()

   self.cur.execute("DROP TABLE IF EXISTS temp_table1;")
   self.conn.commit()


   s3 = boto.connect_s3(AWSKEY, AWSSECRETKEY)
   bucket = s3.get_bucket(BUCKET)
   key_name =  '%s%s' % (PROJECT,int(time.time()))
   print(key_name)
   key = bucket.new_key(key_name)
   key.set_contents_from_filename('%s.csv' %(PROJECT))

   self.cur.execute("DROP TABLE IF EXISTS pricing_latest;")
   self.conn.commit()

   self.cur.execute("create table pricing_latest (                    sku varchar,                    optimal_price float,                    week2_price float ,                     week4_price float,                     week6_price float,                     suggested_price float,\ 
                    week10_price float,                     current_price float,                     weeks_of_stock float,                     Current_weekly_velocity float,                     current_margin float,                     predicted_throughput float,                     predicted_margin float,                     stock_level float,                     name varchar, department varchar,                     week2_margin float,                     week4_margin float,                     week6_margin float,                     week8_margin float,                     week10_margin float,                     predicted_throughput_was_relevant_to_2_weeks float,                     predicted_throughput_was_relevant_to_4_weeks float,                     predicted_throughput_was_relevant_to_6_weeks float,                     predicted_throughput_was_relevant_to_8_weeks float,                     predicted_throughput_was_relevant_to_10_weeks float,                     cost_of_the_decision_was_relevant_to_2_weeks float,                     cost_of_the_decision_was_relevant_to_4_weeks float,                    cost_of_the_decision_was_relevant_to_6_weeks float,                     cost_of_the_decision_was_relevant_to_8_weeks float,                     cost_of_the_decision_was_relevant_to_10_weeks float,                     cost_of_the_decision float,                     parent_group varchar,                     season varchar,                     cost_price float,\ 
                    published varchar,                     in_clearance varchar,\ 
                    act_weeks_on_sale float,\ 
                    original_standard_price float,                     current_standard_price float,                     current_percentage_achieved_margin float,                     suggested_price_percentage_margin float,                     suggestion varchar,price_2_Weeks_Percentage_Margin float,                     price_4_Weeks_Percentage_Margin float,                     price_6_Weeks_Percentage_Margin float,                     price_8_Weeks_Percentage_Margin float,                     price_10_Weeks_Percentage_Margin float,                     demand_Pounds_Last_7_Days float,                     next_7_days_weekly_velocity float                    demand_pounds_next_7_days float,                    next_7_days_margin float,stock_level_next_7_days float,                     currentdate timestamp default '2016-10-01 00:00:00.0');")                                                                                                                                                                                                                                                                                            
   self.conn.commit()
   self.cur.execute("copy pricing_latest from 's3://%s/%s' CSV credentials 'aws_access_key_id=%s;aws_secret_access_key=%s';" % (BUCKET,key_name, AWSKEY, AWSSECRETKEY))
   self.conn.commit()


# Function Calls

# In[ ]:

## detached this from the write function
pm = PricingModel()
pm.cluster()
pm.calculate_prices()
pm.write_to_db()


# In[ ]:



