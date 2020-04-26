#!/usr/bin/env python
# coding: utf-8

# In[9]:

import sqlite3
from sqlite3 import Error
import pandas as pd
from sqlalchemy import create_engine
import datetime as dt
import uuid

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base 
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from sqlalchemy import Column, Integer, String, Float
import os


# In[10]:

os.remove("COVID19_vs_H1N1.sqlite")

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
 
    return conn


# In[11]:


def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)


# In[12]:


database = "COVID19_vs_H1N1.sqlite"


# In[13]:


country = """CREATE TABLE "country" (
	"Country_ID"	INTEGER,
	"Country"	TEXT,
	PRIMARY KEY("Country_ID")
);"""
 
covid = """CREATE TABLE "covid" (
	"index"	INTEGER,
	"Country_ID"	INTEGER,
	"Country"	TEXT,
	"Province"	TEXT,
	"Date"	DATETIME, 
	"Confirmed"	FLOAT,
	"Deaths"	FLOAT,
	"Recovered"	FLOAT,
	PRIMARY KEY("index")
);"""

global_covid_data = """CREATE TABLE "global_covid_data" (
	"index"	INTEGER,
	"Country_ID"	INTEGER,
	"Country"	TEXT,
	"Confirmed"	FLOAT,
	"Deaths"	FLOAT,
	"Recovered"	FLOAT,
	PRIMARY KEY("index")
);"""

h1n1 = """CREATE TABLE "h1n1" (
	"index"	INTEGER,
	"Country_ID"	INTEGER,
	"Country"	TEXT,
	"Date"	DATETIME,
	"Confirmed"	INTEGER,
	"Deaths"	FLOAT,
	PRIMARY KEY("index")
);"""

global_h1n1_data = """CREATE TABLE "global_h1n1_data" (
	"index"	INTEGER,
	"Country"	TEXT,
	"Confirmed"	FLOAT,
	"Deaths"	FLOAT,
	PRIMARY KEY("index")
);"""

covid_2 = """CREATE TABLE "covid_2" (
	"index"	INTEGER,
    "Date"	DATETIME, 
	"Country"	TEXT,
	"Confirmed"	FLOAT,
	"Deaths"	FLOAT,
	"Recovered"	FLOAT,
	PRIMARY KEY("index")
);"""


# In[14]:


# create a database connection
conn = create_connection(database)


# In[15]:


# create tables
if conn is not None:
       # create projects table
   create_table(conn, country)

       # create tasks table
   create_table(conn, covid)
   
   create_table(conn, global_covid_data)
   
   create_table(conn, h1n1)
   
   create_table(conn, global_h1n1_data)
   
else:
   print("Error! cannot create the database connection.")


csv_path = "Resources/covid_19_data.csv"
csv_path2 = "Resources/H1N1_2009.csv"
csv_path3 = "Resources/global_h1n1.csv"

covid = pd.read_csv(csv_path, parse_dates=["ObservationDate"])
h1n1 = pd.read_csv(csv_path2, parse_dates=["Update Time"],encoding = 'unicode_escape')
global_h1n1_data = pd.read_csv(csv_path3)
covid_2 = pd.read_csv(csv_path, parse_dates=["ObservationDate"])

covid = covid.loc[:,['ObservationDate', 'Province/State', 'Country/Region', 'Confirmed', 'Deaths', 'Recovered']]

#Rename Columns
covid = covid.rename(columns={"ObservationDate": "Date", "Province/State" : "Province", "Country/Region" : "Country"})

#Replace Values for country naming consistency
replace_values = {"(St. Martin)" : "St. Martin", "('St. Martin',)": "St. Martin", 
                  'Republic of Ireland' : "Ireland", 'Cabo Verde' : "Cape Verde" } 

covid = covid.replace({"Country": replace_values})

covid = covid[['Country', 'Province', 'Date', 'Confirmed', 'Deaths', 'Recovered']]

mask = (covid['Country'] == 'Taiwan')
filler = 'Taiwan'

covid.loc[covid['Province'].isnull() & mask, 'Province'] = filler

#Group Provinces and take largest cumulative confirmed and death number
province_df = covid.groupby(by='Province').agg('max').reset_index(drop=False)

#Group all provinces into their countries and add confirmed and death numbers
province_df = province_df.groupby(by='Country').agg('sum').reset_index(drop=False)

#Remove countries that are in province_df dataset
remove_list = province_df['Country']
global_covid_data = covid[~covid['Country'].isin(remove_list)]

#province_df
global_covid_data = global_covid_data.loc[:,['Country', 'Date', 'Confirmed', 'Deaths', 'Recovered']]
global_covid_data = global_covid_data.groupby(by='Country').agg('sum').reset_index(drop=False)

#Merge province and country data
global_covid_data = pd.concat([global_covid_data, province_df], ignore_index=True)

h1n1 = h1n1.rename(columns={"Cases": "Confirmed", "Update Time": "Date"})

#Create lists of all countries
country_covid = global_covid_data['Country']
country_h1n1 = global_h1n1_data['Country']

#Combine country lists together
country_df = pd.concat([country_covid, country_h1n1], ignore_index=True)

#Put countries into a DataFrame
country_df = pd.DataFrame(country_df)

#Drop Duplicate Countries
country_df = country_df.drop_duplicates("Country")

#Reset Index and make new index as a column
country_df = country_df.reset_index(drop=True)
country_df = country_df.reset_index(level=0)

#Rename index column to Country ID
country_df = country_df.rename(columns={"index": "Country_ID"})

country_df = country_df[['Country', 'Country_ID']]

#Merge on global_covid_data
global_covid_data = pd.merge(global_covid_data, country_df, how='inner', on='Country')
global_covid_data = global_covid_data[['Country_ID', 'Country', 'Confirmed', 'Deaths', 'Recovered']]

#Merge on covid
covid = pd.merge(covid, country_df, how='inner', on='Country')
covid = covid[['Country_ID', 'Country', 'Province', 'Date', 'Confirmed', 'Deaths', 'Recovered']]

#Merge on h1n1
h1n1 = pd.merge(h1n1, country_df, how='inner', on='Country')
h1n1 = h1n1[['Country_ID', 'Country', 'Date', 'Confirmed', 'Deaths']]

country_df[['Country_ID', 'Country']]

#Take out Province Column in time series for countries
covid_2 = covid_2.loc[:,['ObservationDate', 'Province/State', 'Country/Region', 'Confirmed', 'Deaths', 'Recovered']]

#Rename Columns
covid_2 = covid_2.rename(columns={"ObservationDate": "Date", "Province/State" : "Province", "Country/Region" : "Country"})

covid_2 = covid_2.groupby(['Date', 'Country'])[["Confirmed", "Deaths", "Recovered"]].sum()

covid_2 = covid_2.reset_index()

engine = create_engine("sqlite:///COVID19_vs_H1N1.sqlite")
conn = engine.connect()

country_df.to_sql('country', con=engine, index=False, if_exists='append')

global_covid_data.to_sql('global_covid_data', con=engine, index=True, if_exists='append')
#engine.execute("SELECT * FROM global_covid_data").fetchall()

covid.to_sql('covid', con=engine, index=True, if_exists='append')

h1n1.to_sql(name='h1n1', con=engine, index=True, if_exists='append')

global_h1n1_data.to_sql('global_h1n1_data', con=engine, index=True, if_exists='append')

covid_2.to_sql(name='covid_2', con=engine, index=True, if_exists='append')

print("*******************************************************************************************")
print("*******************************************************************************************")
print(" ")
print("Complete!! all COVID19 data is now up to date in its sqlite database and ready to run the API")
print(" ")
print("*******************************************************************************************")
print("*******************************************************************************************")


