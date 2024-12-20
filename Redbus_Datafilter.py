# Import Libraries
import streamlit as st
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from pyodbc import ProgrammingError
import csv
import os

#Create Database Connection
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="4274",
    )
conn = get_db_connection()
mycursor = conn.cursor()
# Creating Sidebar Options
if  "view" not in st.session_state:
    st.session_state.view = None
# Creating SQL engine
if 'engine' not in st.session_state:
    st.session_state.engine = create_engine("mysql+mysqlconnector://root:4274@localhost/Redbus")
if 'dframe' not in st.session_state:
    st.session_state.dframe = None
st.sidebar.title('Red Bus Project')
st.sidebar.header('Navigation')
if st.sidebar.button('Home'):
    st.session_state.view = 'Option 1'
if st.sidebar.button('Data_Filtering'):
    st.session_state.view = 'Option 2'
#Adding Information in Home button
if st.session_state.view == 'Option 1':
    
    st.title('REDBUS PROJECT')
    st.write('''
    The Project has been created using Python and the following Libraries:
    - **Selenium** : for Web Scraping,
    - **mysql.connector** & **sqlalchemy** : for storing the data to a database,
    - **Streamlit** : to create a user interface for data filtering.
    ''')
# Data Filtering from SQL Database
elif st.session_state.view == 'Option 2':

    @st.cache_data
    def fetch_filtered_data(query, params):
        conn = get_db_connection()  # Function to get your DB connection
        mycursor = conn.cursor()
        mycursor.execute("USE redbus")
        mycursor.execute(query, params)
        myresult = mycursor.fetchall()
        columns = [desc[0] for desc in mycursor.description]
        df = pd.DataFrame(myresult, columns=columns)
        conn.close()  # Ensure to close the connection
        st.session_state.dframe = df
        return df

    if 'dframe' in st.session_state:
        conn = get_db_connection()
        mycursor = conn.cursor()
        mycursor.execute("USE redbus")
        df = pd.read_sql(f"SELECT * FROM bus_routes", con=st.session_state.engine)
        # Column Split
        col1,col2 = st.columns(2)
        #Column 1
        with col1:
        # SQL Qyery Filter Creation based on Tranport Name 
            transport_name = df['govt_bus_name'].unique()
            transport_name = np.insert(transport_name, 0, 'All')
            sbox1 = st.selectbox('Filter the Data by Government Transport Name:', transport_name)
            # SQL Qyery Filter Creation based on Route Name 
            bus_route_name = df[df['govt_bus_name'] == sbox1]['route_name'].unique() if sbox1 != 'All' else df['route_name'].unique()
            bus_route_name = np.insert(bus_route_name, 0, 'All')
            sbox2 = st.selectbox('Filter the Data by Bus Route Name:', bus_route_name)
            # SQL Qyery Filter Creation based on Bus Type 
            bus_type_option = {
                'All': None,
                'Sleeper': "bustype LIKE '%Sleeper%'",
                'Semi-Sleeper': "bustype LIKE '%Semi Sleeper %'",
                'A/C': "bustype LIKE '% A/C %'",
                'NON A/C': "bustype LIKE '% NON A/C%'",
                'Seater': "bustype LIKE '% Seater %'",
                'Other': "bustype NOT LIKE '%Sleeper' AND bustype NOT LIKE '%Semi-Sleeper %' AND bustype NOT LIKE '% Seater %' AND bustype NOT LIKE '% A/C%' AND bustype NOT LIKE '%NON A/C %'"
            }
            sbox3 = st.selectbox('Filter the Data by Bus Type:', bus_type_option.keys())
        #Column 2
        with col2:
            # SQL Qyery Filter Creation based on Price
            price = df['price'].max()
            sbox4_min_val, sbox4_max_val = st.slider('Filter the Data by Bus Price Range:', 0, int(price), value=(2000,3000))
            # SQL Qyery Filter Creation based on Rating
            sbox5_min_val, sbox5_max_val = st.slider('Filter the Data by Bus Rating:',0,5,value=(2,4))
            # SQL Qyery Filter Creation based on Seat Availablity
            seats = df['seats_available'].max()
            sbox6_min_val, sbox6_max_val = st.slider('Filter the Data by Seat Availbility:',0,int(seats),value=(10,30))
 
        query_filters = []
        params = []

        # Query Creation based on Transport Name Selection
        if sbox1 != 'All':
            query_filters.append("govt_bus_name = %s")
            params.append(sbox1)
        # Query Creation based on Bus Route Selection
        if sbox2 != 'All':
            query_filters.append("route_name = %s")
            params.append(sbox2)
        # Query Creation based on Bus Type Selection
        if sbox3 != 'All':
            if sbox3 != 'Other':  # For other bus types, we use a complex condition
                query_filters.append(f"bustype LIKE %s")
                params.append(f"%{sbox3}%")
            else:
                query_filters.append("bustype NOT LIKE '%Sleeper%' AND bustype NOT LIKE '%Semi-Sleeper%' AND bustype NOT LIKE '%Seater%' AND bustype NOT LIKE '%A/C%' AND bustype NOT LIKE '%NON A/C%'")

        # Query Creation based on Price Selection
        if sbox4_min_val or sbox4_max_val:
            query_filters.append(f"price BETWEEN %s AND %s")
            params.extend([sbox4_min_val, sbox4_max_val])
        
        # Query Creation based on Star Rating Selection
        if sbox5_min_val or sbox5_max_val:
            query_filters.append(f"star_rating BETWEEN %s AND %s")
            params.extend([sbox5_min_val, sbox5_max_val])
        # Query Creation based on Seat Availablity Selection
        if sbox6_min_val or sbox6_max_val:
            query_filters.append(f"seats_available BETWEEN %s AND %s")
            params.extend([sbox6_min_val, sbox6_max_val])        

        # Construct final query with appropriate parameters
        if query_filters:
            final_query = f"SELECT * FROM bus_routes WHERE {' AND '.join(query_filters)}"
        else:
            final_query = "SELECT * FROM bus_routes"  # No filters, fetch all data
        # Fetch filtered data using the cached function
        df4 = fetch_filtered_data(final_query, tuple(params))  # Ensure params are a tuple
        st.dataframe(df4)