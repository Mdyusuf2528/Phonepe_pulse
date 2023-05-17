import streamlit as st
from PIL import Image
import os
import json
from streamlit_option_menu import option_menu
import subprocess
import plotly.express as px
import pandas as pd
import requests
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import mysql.connector



class PhonePeData:
    def __init__(self, state_path="E:/VSCODE/Phonepe/pulse/data/"):
        self.state_path = state_path
    
    def agg_transaction_data(self):
        path = os.path.join(self.state_path, "aggregated/transaction/country/india/state/")
        state_list = os.listdir(path)

        transactions = []

        for state in state_list:
            state_path = os.path.join(path, state)
            years = os.listdir(state_path)

            for year in years:
                year_path = os.path.join(state_path, year)
                quarters = os.listdir(year_path)

                for quarter in quarters:
                    quarter_path = os.path.join(year_path, quarter)
                    with open(quarter_path) as f:
                        data = json.load(f)
                        for t in data["data"]["transactionData"]:
                            transaction = [
                                state,
                                year,
                                os.path.splitext(quarter)[0],
                                t["name"],
                                t["paymentInstruments"][0]["count"],
                                t["paymentInstruments"][0]["amount"]
                            ]
                            transactions.append(transaction)

        return pd.DataFrame(transactions, columns=["State", "Year", "Quarter", "Transaction_Type", "Transaction_Count", "Transaction_Amount"])

    def agg_user_data(self):
        path = os.path.join(self.state_path, "aggregated/user/country/india/state/")
        state_list = os.listdir(path)

        users = []

        for state in state_list:
            state_path = os.path.join(path, state)
            years = os.listdir(state_path)

            for year in years:
                year_path = os.path.join(state_path, year)
                quarters = os.listdir(year_path)

                for quarter in quarters:
                    quarter_path = os.path.join(year_path, quarter)
                    with open(quarter_path) as f:
                        data = json.load(f)

                        try:
                            for brand in data["data"]["usersByDevice"]:
                                brand_name = brand["brand"]
                                count = brand["count"]
                                percentage = brand["percentage"]
                                user = [state, year, int(quarter.strip(".json")), brand_name, count, percentage]
                                users.append(user)
                        except:
                            pass

        return pd.DataFrame(users, columns=["State", "Year", "Quarter", "Brand", "Count", "Percentage"])

    def map_transaction_data(self):
        path = os.path.join(self.state_path, "map/transaction/hover/country/india/state/")
        state_list = os.listdir(path)

        transactions = []
        for state in state_list:
            state_path = os.path.join(path, state)
            years = os.listdir(state_path)

            for year in years:
                year_path = os.path.join(state_path, year)
                quarters = os.listdir(year_path)

                for quarter in quarters:
                    quarter_path = os.path.join(year_path, quarter)
                    with open(quarter_path) as f:
                        data = json.load(f)
                        for x in data["data"]["hoverDataList"]:
                            district = x["name"]
                            count = x["metric"][0]["count"]
                            amount = x["metric"][0]["amount"]
                            transactions.append([state, year, int(quarter.strip('.json')), district, count, amount])

        return pd.DataFrame(transactions, columns=["State", "Year", "Quarter", "District", "Transaction_Count", "Transaction_Amount"])

    def map_user_data(self):
        path = os.path.join(self.state_path, "map/user/hover/country/india/state/")
        state_list = os.listdir(path)

        map_user = []
        for state in state_list:
            state_path = os.path.join(path, state)
            years = os.listdir(state_path)

            for year in years:
                year_path = os.path.join(state_path, year)
                quarters = os.listdir(year_path)

                for quarter in quarters:
                    quarter_path = os.path.join(year_path, quarter)
                    with open(quarter_path) as f:
                        data = json.load(f)
                        for district, district_data in data["data"]["hoverData"].items():
                            registered_user = district_data["registeredUsers"]
                            map_user.append([state, year, int(quarter.strip('.json')), district, registered_user])

        return pd.DataFrame(map_user, columns=["State", "Year", "Quarter", "District", "Registered_User"])
    
    def top_transaction_data(self):
        # TO GET THE DATA-FRAME OF TOP <--> TRANSACTION
        path = os.path.join(self.state_path, "top/transaction/country/india/state/")
        state_list = os.listdir(path)

        top_transaction = []
        for state in state_list:
            state_path = os.path.join(path, state)
            years = os.listdir(state_path)

            for year in years:
                year_path = os.path.join(state_path, year)
                quarters = os.listdir(year_path)

                for quarter in quarters:
                    quarter_path = os.path.join(year_path, quarter)
                    with open(quarter_path) as f:
                        data = json.load(f)
                        for entity in data['data']['pincodes']:
                            district = entity['entityName']
                            count = entity['metric']['count']
                            amount = entity['metric']['amount']
                            top_transaction.append([state, year, int(quarter.strip('.json')), district, count, amount])

        return pd.DataFrame(top_transaction, columns=["State", "Year", "Quarter", "District", "Transaction_Count", "Transaction_Amount"])
    
    def top_user_data(self):
        # TO GET THE DATA-FRAME OF TOP <--> USER
        path6 = os.path.join(self.state_path, "top/user/country/india/state/")
        user_list = os.listdir(path6)

        top_user = []
        for state in user_list:
            state_path = os.path.join(path6, state)
            years = os.listdir(state_path)

            for year in years:
                year_path = os.path.join(state_path, year)
                quarters = os.listdir(year_path)

                for quarter in quarters:
                    quarter_path = os.path.join(year_path, quarter)
                    with open(quarter_path) as f:
                        data = json.load(f)
                        for entity in data['data']['pincodes']:
                            district = entity['name']
                            registered_user = entity['registeredUsers']
                            top_user.append([state, year, int(quarter.strip('.json')), district, registered_user])

        return pd.DataFrame(top_user, columns=["State", "Year", "Quarter", "District", "Registered_User"])




conn = mysql.connector.connect(
        user="root",
        password="root",
        host="localhost",
        database="PhonePe",
        port="3306"
    )
cursor = conn.cursor()

#creating functions for query search in mysql to get the data

class Search:
    def __init__(self, cursor):
        self.cursor = cursor
        
    def type_(self, transaction_type):
        self.cursor.execute(f"SELECT DISTINCT State, Quarter, Year, Transaction_type, Transaction_amount FROM aggregated_transaction WHERE Transaction_type = '{transaction_type}' ORDER BY State, Quarter, Year")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Quarter', 'Year', 'Transaction_type', 'Transaction_amount'])
        return df
    
    def type_year(self, transaction_type, year):
        self.cursor.execute(f"SELECT DISTINCT State, Year, Quarter, Transaction_type, Transaction_amount FROM aggregated_transaction WHERE Year = '{year}' AND Transaction_type = '{transaction_type}' ORDER BY State, Quarter, Year")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year', 'Quarter', 'Transaction_type', 'Transaction_amount'])
        return df
    
    def type_state(self, transaction_type, year, state):
        self.cursor.execute(f"SELECT DISTINCT State, Year, Quarter, Transaction_type, Transaction_amount FROM aggregated_transaction WHERE State = '{state}' AND Transaction_type = '{transaction_type}' AND Year = '{year}' ORDER BY State, Quarter, Year")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year', 'Quarter', 'Transaction_type', 'Transaction_amount'])
        return df
    
    def district_choice_state(self, state):
        self.cursor.execute(f"SELECT DISTINCT State, Year, Quarter, District, amount FROM map_transaction WHERE State = '{state}' ORDER BY State, Year, Quarter, District")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year', 'Quarter', 'District', 'amount'])
        return df
    
    def dist_year_state(self, year, state, district):
        self.cursor.execute(f"SELECT DISTINCT State, Year, Quarter, District, amount FROM map_transaction WHERE Year = '{year}' AND State = '{state}' AND District = '{district}' ORDER BY State, Year, Quarter, District")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year', 'Quarter', 'District', 'amount'])
        return df
    
    def district_year_state(self, district, year, state):
        self.cursor.execute(f"SELECT DISTINCT State, Year, Quarter, District, amount FROM map_transaction WHERE District = '{district}' AND State = '{state}' AND Year = '{year}' ORDER BY State, Year, Quarter, District")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year', 'Quarter', 'District', 'amount'])
        return df
    
    def brand_(self, brand_type):
        self.cursor.execute(f"SELECT State, Year, Quarter, brands, Percentage FROM aggregated_user WHERE brands='{brand_type}' ORDER BY State, Year, Quarter, brands, Percentage DESC")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year', 'Quarter', 'brands', 'Percentage'])
        return df
    
    def brand_year(self, brand_type, year):
        self.cursor.execute(f"SELECT State, Year, Quarter, brands, Percentage FROM aggregated_user WHERE Year = '{year}' AND brands='{brand_type}' ORDER BY State, Year, Quarter, brands, Percentage DESC")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year', 'Quarter', 'brands', 'Percentage'])
        return df
    
    def brand_state(self,state,brand_type,year):
        self.cursor.execute(f"SELECT State,Year,Quarter,brands,Percentage FROM aggregated_user WHERE State = '{state}' AND brands='{brand_type}' AND Year = '{year}' ORDER BY State,Year,Quarter,brands,Percentage DESC");
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year',"Quarter", 'brands', 'Percentage'])
        return df

    def transaction_state(self,_state):
        self.cursor.execute(f"SELECT State,Year,Quarter,District,Transaction_count,Transaction_amount FROM top_transaction WHERE State = '{_state}' GROUP BY State,Year,Quarter")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year',"Quarter", 'District', 'Transaction_count', 'Transaction_amount'])
        return df

    def transaction_year(self,_state,_year):
        self.cursor.execute(f"SELECT State,Year,Quarter,District,Transaction_count,Transaction_amount FROM top_transaction WHERE Year = '{_year}' AND State = '{_state}' GROUP BY State,Year,Quarter")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year',"Quarter", 'District', 'Transaction_count', 'Transaction_amount'])
        return df

    def transaction_quater(self,_state,_year,_quater):
        self.cursor.execute(f"SELECT State,Year,Quarter,District,Transaction_count,Transaction_amount FROM top_transaction WHERE Year = '{_year}' AND Quarter = '{_quater}' AND State = '{_state}' GROUP BY State,Year,Quarter")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year',"Quarter", 'District', 'Transaction_count', 'Transaction_amount'])
        return df

    def registered_user_state(self,_state):
        self.cursor.execute(f"SELECT State,Year,Quarter,District,RegisteredUser FROM map_user WHERE State = '{_state}' ORDER BY State,Year,Quarter,District")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year',"Quarter", 'District', 'RegisteredUser'])
        return df

    def registered_user_year(self,_state,_year):
        self.cursor.execute(f"SELECT State,Year,Quarter,District,RegisteredUser FROM map_user WHERE Year = '{_year}' AND State = '{_state}' ORDER BY State,Year,Quarter,District")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year',"Quarter", 'District', 'RegisteredUser'])
        return df

    def registered_user_district(self,_state,_year,_dist):
        self.cursor.execute(f"SELECT State,Year,Quarter,District,RegisteredUser FROM map_user WHERE Year = '{_year}' AND State = '{_state}' AND District = '{_dist}' ORDER BY State,Year,Quarter,District")
        df = pd.DataFrame(self.cursor.fetchall(), columns=['State', 'Year',"Quarter", 'District', 'RegisteredUser'])
        return df

