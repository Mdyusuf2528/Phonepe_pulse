import streamlit as st
from PIL import Image
import os
from streamlit_option_menu import option_menu
import plotly.express as px
import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
import requests
import subprocess
from fn import *
from df import *



#CODE FLOW:
                # -> CLONE THE GITHUB
                # -> TRANSFORM INTO THE DATAFRAME
                # -> STORE INTO THE MYSQL RDBMS
                # -> USING PY TO EXTRACT THE DATA FROM SQL & DISPLAY THAT DATA AS VISUAL USING PLOTLY



#CODE FLOW:
                # -> GIT TASK (HERE)
                # -> SQL & DATAFRAME CLASS CREATED ON "df" MODULE
                # -> STORE THAT DATAFRAME INTO SQL USING PHONEPE CLASS
                # -> JUST CREATE FNS IN "fn" MODULE
                # -> USING THAT FUNCTIONS FOR TASK


# -------------------------------- Page Title --------------------------------------------
st.set_page_config(page_title = 'Phonepe_Pulse')

try:
    #CLONING TH PHONEPE--PULSE GITHUB REPOSITORY

    response = requests.get('https://api.github.com/repos/PhonePe/pulse') 

    #This above API endpoint returns information about the repository in JSON format.
    repo_clone = response.json() #it'll give the clone details in json format
    clone_url = repo_clone['clone_url'] # extract that link from this clone detailed json file

    #DIRECTING THE REPOSITORY TO THE LOCAL DIRECTORY

    repo_local= "pulse"
    clone_dir = os.path.join(os.getcwd(), repo_local) # It helps to create just clone directory

    #Clones the PhonePe Pulse GitHub repository to the local directory

    subprocess.run(["git", "clone", clone_url, clone_dir], check=True)

except:
    pass

# DataFrames

phonepe_data = PhonePeData()  # create an instance of the PhonePeData class

# call the functions to get the dataframes
agg_transaction_data = phonepe_data.agg_transaction_data()
agg_user_data = phonepe_data.agg_user_data()

map_transaction_data = phonepe_data.map_transaction_data()
map_user_data = phonepe_data.map_user_data()

top_transaction_data = phonepe_data.top_transaction_data()
top_user_data = phonepe_data.top_user_data()

# ------------------------------------------------------------------------------


st.title(":violet[PhonePe Pulse]")
st.markdown(":violet[Welcome to the PhonePe Pulse Dashboard ,This PhonePe Pulse Data Visualization and Exploration dashboard is a user-friendly tool designed to provide insights and information about the data in the PhonePe Pulse GitHub repository. This dashboard offers a visually appealing and interactive interface for users to explore various metrics and statistics.]")

        
# Creating the side bars
with st.sidebar:
    selected = st.selectbox("Select a page", ["Top Performers", "Explore Data"])

if selected == "Top Performers":
    top_performers()
elif selected == "Explore Data":
    explore_data()
