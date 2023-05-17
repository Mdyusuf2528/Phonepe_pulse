import streamlit as st
import pandas as pd
import plotly.express as px
import mysql.connector as sql


mydb = sql.connect(host="localhost",
                   user="root",
                   password= "root",
                   database= "phonepe"
                  )
mycursor = mydb.cursor(buffered=True)


def top_performers():
    st.markdown("## :violet[Top 10 performers]")

    Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))

    if Type == "Transactions":
        Data_segmentation = st.sidebar.selectbox("**Data segmentation**", ("State", "District"), key="Data segmentation_selectbox")
        colum1,colum2= st.columns([1,1.5],gap="large")
        with colum1:
            Year = st.slider("**Select the Year**", min_value=2018, max_value=2022)
            Quarter = st.selectbox('**Select the Quarter**',('1','2','3','4'),key='qgwe2')

        if Data_segmentation == "State":
            mycursor.execute(f"select state, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total_amount from agg_transaction where year = {Year} and quarter = {Quarter} group by state order by Total_amount desc limit 10")
            df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Transactions_Count','Total_Amount'])
            title = "Top 10 Phonepe Transaction according to States"

            fig = px.pie(df, values='Transactions_Count',
                    names='State',
                    title=title,
                    color_discrete_sequence=px.colors.sequential.Magenta,
                    hover_data=['Total_Amount'],
                    labels={'Total_Amount':'Total Amount'})

            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig,use_container_width=True)
        if Data_segmentation == "District":
                mycursor.execute(f"SELECT district, state, sum(Transaction_count) as Total_Count, sum(Transaction_amount) as Total_amount FROM map_transaction WHERE year = {Year} AND quarter = {Quarter} GROUP BY district, state ORDER BY Total_amount DESC LIMIT 10")
                df = pd.DataFrame(mycursor.fetchall(), columns=['District', 'State', 'Transactions_Count', 'Total_Amount'])

                fig = px.pie(df, values='Total_Amount',
                        names='District',
                        title='Top 10 Phonepe Transactions according to Districts',
                        color_discrete_sequence=px.colors.sequential.Magenta,
                        hover_data=['Transactions_Count'],
                        labels={'Transactions_Count': 'Transactions_Count'})

                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
    if Type == "Users":
        Data_segmentation = st.sidebar.selectbox("**Data segmentation**", ["Registered_User"], key="Data segmentation_selectbox")
        colum1,colum2= st.columns([1,1.5],gap="large")
        with colum1:
            Year = st.slider("**Select the Year**", min_value=2018, max_value=2022)
            Quarter = st.selectbox('**Select the Quarter**',('1','2','3','4'),key='quat')
        
        if Data_segmentation == "Registered_User":
                mycursor.execute(f"select district, sum(Registered_User) as Total_Registered_Users from map_user where Year = {Year} and quarter = {Quarter} group by District order by Total_Registered_Users desc limit 10")
                df = pd.DataFrame(mycursor.fetchall(), columns=['District', 'Total_Registered_Users'])
                st.write(df)
                fig = px.pie(df,
                        title='Top 10 Phonepe users according to Districts',
                        values="Total_Registered_Users",
                        names="District",
                        hole = 0.5,
                        color='Total_Registered_Users')
                st.plotly_chart(fig, use_container_width=True)
        if Data_segmentation == "Appopeners":
                mycursor.execute(f"SELECT state, SUM(App_opening) AS Total_Appopeners FROM map_user WHERE year = {Year} AND quarter = {Quarter} AND App_opening > 0 GROUP BY state ORDER BY Total_Appopeners DESC limit 10")
                df = pd.DataFrame(mycursor.fetchall(), columns=['State','Total_Appopeners'])
                st.write(df)
                fig = px.pie(df, 
                        values='Total_Appopeners',
                        names='State',
                        title='Top 10 Phonepe users according to Appopeners',
                        hole=0.5,
                        color='Total_Appopeners')
                st.plotly_chart(fig, use_container_width=True)


def explore_data():
    # Add code to explore data here
    st.markdown("## :violet[Exploring the data]")
    Type = st.sidebar.selectbox("**Type**", ("Analysis of Transactions", "Users"))
    if Type == "Analysis of Transactions":
        Type = st.sidebar.selectbox("**Type**", ("Transaction Count", "Payment Type"))
        if Type == "Transaction Count":
            colum1,colum2= st.columns([1,1.5],gap="large")
            with colum1:
                            Year = st.slider("**Select the Year**", min_value=2018, max_value=2022)
                            Quarter = st.selectbox('**Select the Quarter**',('1','2','3','4'),key='qgwe2')
            st.markdown("## :violet[**Transaction Count According To District**]")
            selected_state = st.selectbox("**please select any State to visualize**",
                                ('andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar',
                                'chandigarh','chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu','delhi','goa','gujarat','haryana',
                                'himachal-pradesh','jammu-&-kashmir','jharkhand','karnataka','kerala','ladakh','lakshadweep',
                                'madhya-pradesh','maharashtra','manipur','meghalaya','mizoram',
                                'nagaland','odisha','puducherry','punjab','rajasthan','sikkim',
                                'tamil-nadu','telangana','tripura','uttar-pradesh','uttarakhand','west-bengal'),index=22,key="state_to_selectbox")
            
            mycursor.execute(f"select State, District,year,quarter, sum(Transaction_count) as Total_Transactions_count from map_transaction where year = {Year} and quarter = {Quarter} and State = '{selected_state}' group by State, District,year,quarter order by state,district")
            
            df1 = pd.DataFrame(mycursor.fetchall(), columns=['State','District','Year','Quarter',
                                                            'Total_Transactions_count'])
            fig = px.bar(df1,
                        title='Transaction Count According To District' ,
                        x="District",
                        y="Total_Transactions_count",
                        orientation='v',
                        color='Total_Transactions_count',
                        color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig,use_container_width=True)
        
        if Type == "Payment Type":
            colum1,colum2= st.columns([1,1.5],gap="large")
            with colum1:
                            Year = st.slider("**Select the Year**", min_value=2018, max_value=2022)
                            Quarter = st.selectbox('**Select the Quarter**',('1','2','3','4'),key='qgwe2')
            st.markdown("## :violet[Payment Type]")
            selected_state = st.selectbox("**please select any State to visualize**",
                                ('andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar',
                                'chandigarh','chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu','delhi','goa','gujarat','haryana',
                                'himachal-pradesh','jammu-&-kashmir','jharkhand','karnataka','kerala','ladakh','lakshadweep',
                                'madhya-pradesh','maharashtra','manipur','meghalaya','mizoram',
                                'nagaland','odisha','puducherry','punjab','rajasthan','sikkim',
                                'tamil-nadu','telangana','tripura','uttar-pradesh','uttarakhand','west-bengal'),key="state_selectbox")
            Type = st.selectbox('**Please select the values to visualize**', ('Transaction_count', 'Transaction_amount'))
            if Type == "Transaction_count":
                            mycursor.execute(f"select Transaction_type, sum(Transaction_count) as Total_Transactions_count from agg_transaction where year= {Year} and quarter = {Quarter} group by transaction_type order by Transaction_type")
                            df = pd.DataFrame(mycursor.fetchall(), columns=['Transaction_type', 'Total_Transactions_count'])
                            fig = px.bar(df,
                                    title='Transaction Types vs Total_Transactions_count',
                                    x="Transaction_type",
                                    y="Total_Transactions_count",
                                    orientation='v',
                                    color='Transaction_type',
                                    color_continuous_scale=px.colors.sequential.Magenta)
                            st.plotly_chart(fig,use_container_width=False)
            if Type == "Transaction_amount":
                            mycursor.execute(f"select Transaction_type, sum(Transaction_amount) as Total_Transaction_amount from agg_transaction where year= {Year} and quarter = {Quarter} group by transaction_type order by Transaction_type")
                            df = pd.DataFrame(mycursor.fetchall(), columns=['Transaction_type', 'Total_Transactions_amount'])
                            fig = px.bar(df,
                                    title='Transaction Types vs Total_Transactions_amount',
                                    x="Transaction_type",
                                    y="Total_Transactions_amount",
                                    orientation='v',
                                    color='Transaction_type',
                                    color_continuous_scale=px.colors.sequential.Magenta)
                            st.plotly_chart(fig,use_container_width=False)
             
    if Type == "Users":
        data_segmentation = st.sidebar.selectbox("Data segmentation", ["Registered Users"])
        column1, column2 = st.columns([1, 1.5])
        with column1:
            year = st.slider("Select the Year", min_value=2018, max_value=2022)
            quarter = st.selectbox("Select the Quarter", ['1', '2', '3', '4'], key='quart')
        if data_segmentation == "Registered Users":
            st.markdown("## :violet[Total Numbers Of Registered Users According to Districts]")
            selected_state = st.selectbox("Select a state to fetch the data", [
                'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh', 'assam', 'bihar',
                'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat', 'haryana',
                'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand', 'karnataka', 'kerala', 'ladakh', 'lakshadweep',
                'madhya-pradesh', 'maharashtra', 'manipur', 'meghalaya', 'mizoram',
                'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal'
            ], index=1)

            mycursor.execute(f"SELECT State, Year, Quarter, District, SUM(Registered_user) AS Total_Registered_Users "
                            f"FROM map_user "
                            f"WHERE Year = {year} AND Quarter = {quarter} AND State = '{selected_state}' "
                            f"GROUP BY State, District, Year, Quarter "
                            f"ORDER BY State, District")

            df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Year', 'Quarter', 'District', 'Total_Registered_Users'])

            fig = px.bar(df,
                        x="District",
                        y="Total_Registered_Users",
                        orientation='v',
                        color="Total_Registered_Users",
                        color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig, use_container_width=True)
