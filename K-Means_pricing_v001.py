
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

        q_string = """select split_part(orderlinesku, '-', 1) as sku , orderlineqty, orderdate, totalorderlinegrossvalue, orderlinepromotioncodes, orderlineid, bhline.orderid
into temp_table
from vw_order_line  bhline 
join vw_order_header  bhheader 
on bhline.orderid=bhheader.orderid;"""
        q_string2 = """select sku, sum(orderlineqty) number_orders, 
sum(totalorderlinegrossvalue) as total_cost,
pgdate_part('week', orderdate) as week, 
pgdate_part('year', orderdate) as year 
into temp_table3
from temp_table2
group by 1, 4, 5;"""
        q_string3 = """select * into temp_table2 from temp_table tt LEFT JOIN bh_returnheader ret ON ret.ordernumber = tt.orderid WHERE ret.ordernumber IS NULL;
        """
        try:
            self.cur.execute("drop table temp_table;")
            self.conn.commit()
        except:
            self.conn.commit()
        try:
            self.cur.execute("drop table temp_table2;")
            self.conn.commit()
        except:
            self.conn.commit()
        try:
            self.cur.execute("drop table temp_table3;")
            self.conn.commit()
        except:
            self.conn.commit()
        self.cur.execute(q_string)
        self.conn.commit()
        self.cur.execute(q_string3)
        self.conn.commit()
        self.cur.execute(q_string2)
        self.conn.commit()


# Clustering function

# In[ ]:

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

    with open('boohoo-pricing.csv', 'wb') as csvfile:
        self.spamwriter = csv.writer(csvfile)

        for index, row in self.df.iterrows():
            # [(k, v.sku) for k,v in self.df.iterrows() if v.sku == 'DZZ90335']
            res = self.nbrs.kneighbors(self.X[index], return_distance=False)
            similar_skus = tuple([self.df.sku[a] for a in res[0]])

            query_string = "select * from  temp_table3 where sku  in ('{0}','{1}','{2}','{3}','{4}')".format(similar_skus[0], similar_skus[1], similar_skus[2], similar_skus[3], similar_skus[4]);

            res_df = pd.read_sql_query(query_string, self.engine)

            sku = self.df['sku'][index]
            try:
                ideal_price = sum(res_df['total_cost'])/sum(res_df['number_orders'])
            except:
                ideal_price = self.df['cost_price'][index] + 10

            cost_price = self.df['cost_price'][index]
            total_weeks = len(res_df.groupby(['week', 'year']).count())
            self.name = self.df['name'][index]
            self.department = self.df['product_group'][index]
            sp = res_df[res_df['sku'] == sku]
            try:
                sim_items_sold_per_week = sum(res_df['number_orders'])/len(res_df)
            except:
                sim_items_sold_per_week = 10
            try:
                items_sold_per_week = sum(sp['number_orders'])/len(sp)
            except:
                items_sold_per_week = sim_items_sold_per_week 
            try:
                self.sim_items_sold_per_week = sum(res_df['number_orders'])/len(res_df)
            except:
                self.sim_items_sold_per_week = 10
            new_price = (ideal_price/sim_items_sold_per_week)*items_sold_per_week

            weeks_on_sale = res_df[res_df['sku'] == sku].count()['sku']
            
            published = self.df['published'][index]
            in_clearance = self.df['in_clearance'][index]
            act_weeks_on_sale = self.df['act_weeks_on_sale'][index]
            original_standard_price = self.df['original_standard_price'][index]
            current_standard_price = self.df['current_standard_price'][index]
                    
            self.current_price = current_standard_price
            q_string = """select totalorderlinegrossvalue from temp_table2 where orderlineid = (select max(orderlineid) from  temp_table where sku = '{}' and orderlinepromotioncodes = '')""".format(sku)
            actual_df = pd.read_sql_query(q_string, self.engine)
            try:
                self.actual_price = actual_df['totalorderlinegrossvalue'].loc[0]
            except:
                self.actual_price = 0

            try:
                ss = res_df[res_df['sku'] == sku].loc[0]
                self.current_price = ss['total_cost']/ss['number_orders']
            except Exception as e: 
                self.current_price = self.actual_price
            if self.current_price == 0:
                self.current_price = current_standard_price 

            min_price = 0
            if act_weeks_on_sale < 5:
                min_price = self.price_point(self.df['product_group'][index].upper(), self.current_price)
            if min_price > new_price:
                new_price = min_price

            if new_price > (original_standard_price + original_standard_price/10):
                new_price = (original_standard_price + original_standard_price/10)

                               
            self.calculate_clearance_price(sku, items_sold_per_week, new_price, ideal_price, sim_items_sold_per_week, cost_price, published, in_clearance, act_weeks_on_sale, original_standard_price, current_standard_price)



# Calculates the clearance price

# In[ ]:

def calculate_clearance_price(self, sku, items_sold_per_week, new_price, ideal_price, sim_items_sold_per_week, cost_price, published, in_clearance, act_weeks_on_sale, original_standard_price, current_standard_price):
    query_string = """SELECT si.ItemID,Code,Price,sip.PriceBandId,pb.Name, bi.quantityonhand, si.manufacturer
FROM bh_StockItem si
join bh_StockItemPrice sip on si.ItemID = sip.ItemID
join bh_PriceBand pb on pb.PriceBandID = sip.PriceBandID
join bh_dwh_stock_levels bi on si.code = bi.productid
where pb.Name = 'Offer Price' and split_part(Code, '-', 1) = '{0}'""".format(sku)
    res_df_cl = pd.read_sql_query(query_string, self.engine)
    try:
        price = res_df_cl['price'].loc[0]
    except: 
        price = 0
    clearence_price = price
    week2, week4, week6, week8, week10 = 0, 0, 0, 0, 0
    margin2, margin4, margin6, margin8, margin10 = 0, 0, 0, 0, 0
    tp, tp2, tp4, tp6, tp8, tp10 = 0, 0, 0, 0, 0, 0
    cd2, cd4, cd6, cd8, cd10 = 0, 0, 0, 0, 0
 
    season_record = res_df_cl['manufacturer']
    season = season_record[0]
    
    predicted_throughput = self.sim_items_sold_per_week 
    stock_level = sum(res_df_cl['quantityonhand'])
    try:
        weeks_of_stock = sum(res_df_cl['quantityonhand'])/items_sold_per_week
    except: 
        weeks_of_stock = 0
    current_velocity = items_sold_per_week
    if cost_price:
        current_margin = self.current_price - cost_price
    else: 
        current_margin = 0
    for weeks in [2,4,6,8,10]:
        if sum(res_df_cl['quantityonhand']) < (weeks*items_sold_per_week):
            clearence_price = new_price
            tp = predicted_throughput 
        else:
            try:
                clearence_price = new_price*(items_sold_per_week/(sum(res_df_cl['quantityonhand'])/weeks))
                tp = (predicted_throughput - predicted_throughput * (items_sold_per_week/(sum(res_df_cl['quantityonhand'])/weeks))) + predicted_throughput 
            except: 
                clearence_price = new_price
                tp = predicted_throughput 

        if clearence_price < cost_price:
            clearence_price = cost_price
        if weeks == 2:
            week2= clearence_price
            if cost_price != 0:
                margin2 = week2 - cost_price
            tp2 = tp
            cd2 = (week2 - self.current_price)*stock_level
        if weeks == 4:
            week4= clearence_price
            if cost_price != 0:
                margin4 = week4 - cost_price
            tp4 = tp
            cd4 = (week4 - self.current_price)*stock_level
        if weeks == 6:
            week6= clearence_price
            if cost_price != 0:
                margin6 = week6 - cost_price
            tp6 = tp
            cd6 = (week6 - self.current_price)*stock_level
        if weeks == 8:
            week8= clearence_price
            if act_weeks_on_sale < 5:
                week8 = new_price
            if cost_price != 0:
                margin8 = week8 - cost_price
            tp8 = tp
            cd8 = (week8 - self.current_price)*stock_level
        if weeks == 10:
            week10= clearence_price
            if act_weeks_on_sale < 5:
                week10 = new_price
            if cost_price != 0:
                margin10 = week10 - cost_price
            tp10 = tp
            cd10 = (week10 - self.current_price)*stock_level

    cost_decision = (new_price - self.current_price)*stock_level
    if cost_price != 0:
        predicted_margin = new_price - cost_price
    else: 
        predicted_margin = 0
    suggestion = ""
    
    if new_price == self.current_price:
        suggestion = "HOLD"
    elif new_price >  self.current_price:
        suggestion = "INCREASE"
    elif new_price <  self.current_price:
        suggestion = "DECREASE" 
     
    if cost_price != 0:
        current_percentage_achieved_margin = (self.current_price/cost_price) * 100
    else:
        current_percentage_achieved_margin = 0
    
    if cost_price != 0:
        suggested_price_percentage_margin = (new_price/ cost_price) * 100
    else:
        suggested_price_percentage_margin = 0
    
    #CASE WHEN pricing.week2_price > 0 THEN pricing.week2_margin/pricing.week2_price*100 ELSE NULL END AS Price_2_Weeks_Percentage_Margin, 

    price_2_Weeks_Percentage_Margin = (margin2/week2)*100
    price_4_Weeks_Percentage_Margin = (margin4/week4)*100
    price_6_Weeks_Percentage_Margin = (margin6/week6)*100
    price_8_Weeks_Percentage_Margin = (margin8/week8)*100
    price_10_Weeks_Percentage_Margin = (margin10/week10)*100
    
    demand_Pounds_Last_7_Days = current_velocity * self.current_price 
            
    curr_pric = self.actual_price
    
    priceList=[new_price,week2,week4,week6,week8,week10]
    
    used_price = min(priceList, key=lambda x:abs(x-self.actual_price))
    
    if used_price == new_price:
        next_7_days_weekly_velocity = current_velocity 
        demand_pounds_next_7_days = current_velocity * new_price 
        next_7_days_margin = suggested_price_percentage_margin
        stock_level_next_7_days = stock_level - current_velocity
    elif used_price == week2:
        next_7_days_weekly_velocity = tp2 
        demand_pounds_next_7_days = tp2 * week2 
        next_7_days_margin = price_2_Weeks_Percentage_Margin
        stock_level_next_7_days = stock_level - tp2
    elif used_price == week4:
        next_7_days_weekly_velocity = tp4 
        demand_pounds_next_7_days = tp4 * week4 
        next_7_days_margin = price_4_Weeks_Percentage_Margin
        stock_level_next_7_days = stock_level - tp4            
    elif used_price == week6:
        next_7_days_weekly_velocity = tp6 
        demand_pounds_next_7_days = tp6 * week6 
        next_7_days_margin = price_6_Weeks_Percentage_Margin
        stock_level_next_7_days = stock_level - tp6            
    elif used_price == week8:
        next_7_days_weekly_velocity = tp8 
        demand_pounds_next_7_days = tp8 * week8 
        next_7_days_margin = price_8_Weeks_Percentage_Margin
        stock_level_next_7_days = stock_level - tp8
    elif used_price == week10:
        next_7_days_weekly_velocity = tp10 
        demand_pounds_next_7_days = tp10 * week10 
        next_7_days_margin = price_10_Weeks_Percentage_Margin
        stock_level_next_7_days = stock_level - tp10          
                              
    self.spamwriter.writerow([sku, new_price, week2, week4, week6, week8, week10, self.current_price, weeks_of_stock , current_velocity, current_margin,predicted_throughput,predicted_margin, stock_level, self.name, self.department, margin2, margin4, margin6, margin8, margin10, tp2, tp4, tp6, tp8, tp10, cd2, cd4, cd6, cd8, cd10, cost_decision,self.department, season, cost_price, published, in_clearance, act_weeks_on_sale, original_standard_price, current_standard_price, current_percentage_achieved_margin, suggested_price_percentage_margin, suggestion,price_2_Weeks_Percentage_Margin, price_4_Weeks_Percentage_Margin, price_6_Weeks_Percentage_Margin,price_8_Weeks_Percentage_Margin,price_10_Weeks_Percentage_Margin,demand_Pounds_Last_7_Days,next_7_days_weekly_velocity,demand_pounds_next_7_days,next_7_days_margin,stock_level_next_7_days])


# In[ ]:

def price_point(self, group, current_price):
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
       try:
           self.cur.execute("drop table temp_table;")
           self.conn.commit()
       except:
           self.conn.commit()
       try:
           self.cur.execute("drop table temp_table2;")
           self.conn.commit()
       except:
           self.conn.commit()
       try:
           self.cur.execute("drop table temp_table3;")
           self.conn.commit()
       except:
           self.conn.commit()

       s3 = boto.connect_s3(AWSKEY, AWSSECRETKEY)
       bucket = s3.get_bucket(BUCKET)
       key_name =  '%s%s' % (PROJECT,int(time.time()))
       print(key_name)
       key = bucket.new_key(key_name)
       key.set_contents_from_filename('%s.csv' %(PROJECT))
       try:
           self.cur.execute("drop table pricing_latest;")
           self.conn.commit()
       except:
           self.conn.commit()
           pass

       self.cur.execute("create table pricing_latest (sku varchar,optimal_price float,week2_price float , week4_price float, week6_price float, suggested_price float, week10_price float, current_price float, weeks_of_stock float, Current_weekly_velocity float, current_margin float, predicted_throughput float, predicted_margin float, stock_level float, name varchar, department varchar, week2_margin float, week4_margin float, week6_margin float, week8_margin float, week10_margin float, predicted_throughput_was_relevant_to_2_weeks float, predicted_throughput_was_relevant_to_4_weeks float, predicted_throughput_was_relevant_to_6_weeks float, predicted_throughput_was_relevant_to_8_weeks float, predicted_throughput_was_relevant_to_10_weeks float, cost_of_the_decision_was_relevant_to_2_weeks float, cost_of_the_decision_was_relevant_to_4_weeks float,cost_of_the_decision_was_relevant_to_6_weeks float, cost_of_the_decision_was_relevant_to_8_weeks float, cost_of_the_decision_was_relevant_to_10_weeks float, cost_of_the_decision float, parent_group varchar, season varchar, cost_price float, published varchar, in_clearance varchar, act_weeks_on_sale float, original_standard_price float, current_standard_price float, current_percentage_achieved_margin float, suggested_price_percentage_margin float, suggestion varchar,price_2_Weeks_Percentage_Margin float, price_4_Weeks_Percentage_Margin float, price_6_Weeks_Percentage_Margin float, price_8_Weeks_Percentage_Margin float, price_10_Weeks_Percentage_Margin float, demand_Pounds_Last_7_Days float, next_7_days_weekly_velocity float,demand_pounds_next_7_days float,next_7_days_margin float,stock_level_next_7_days float, currentdate timestamp default '2016-10-01 00:00:00.0');")

                                                                                                                                                                                                                                                                                           
       self.conn.commit()
       self.cur.execute("copy pricing_latest from 's3://%s/%s' CSV credentials 'aws_access_key_id=%s;aws_secret_access_key=%s';" % (BUCKET,key_name, AWSKEY, AWSSECRETKEY))
       self.conn.commit()

pm = PricingModel()
pm.cluster()
pm.calculate_prices()
pm.write_to_db()

