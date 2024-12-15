# Import Libraries
import streamlit as st
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from pyodbc import ProgrammingError
import csv
import os


def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="4274",
    )
#Create the database and the bus_routes table if they don't exist.
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
st.sidebar.write('Step 1')
if st.sidebar.button('Home'):
    st.session_state.view = 'Option 1'
st.sidebar.write('Step 2') 
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
    st.header('Step:2 - Press Data_scraping Button from Navigation Bar')
# Data Scraping from Red Bus Website
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
        col1,col2 = st.columns(2)
        with col1:
        # SQL Qyery Filter Creation based on Tranport Name 
            transport_name = df['govt_bus_name'].unique()
            transport_name = np.insert(transport_name, 0, 'All')
            sbox1 = st.selectbox('Filter the Data by Government Transport Name:', transport_name)
            bus_route_name = df[df['govt_bus_name'] == sbox1]['route_name'].unique() if sbox1 != 'All' else df['route_name'].unique()
            bus_route_name = np.insert(bus_route_name, 0, 'All')
            sbox2 = st.selectbox('Filter the Data by Bus Route Name:', bus_route_name)
            
            # SQL Qyery Filter Creation based on Price Range
            # price_range_options = {
            #     'All': None,
            #     'Below 1000': "price <= 1000",
            #     '1000 - 2000': "price BETWEEN 1000 AND 2000",
            #     '2000 - 3000': "price BETWEEN 2000 AND 3000",
            #     'Above 3000': "price >= 3000"
            # }
            price = df['price'].max()
            # sbox3 = st.selectbox('Filter the Data by Bus Price Range:', price_range_options.keys())
            sbox3 = str(st.slider('Filter the Data by Bus Price Range:',0,int(price),1000))
        with col2:
            # SQL Qyery Filter Creation based on Rating
            sbox4 = str(st.slider('Filter the Data by Bus Rating:',0,5,3))
            # SQL Qyery Filter Creation based on Seat Availablity
            seats = df['seats_available'].max()
            sbox5 = str(st.slider('Filter the Data by Seat Availbility:',0,int(seats),10))
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
            #bus_type = np.insert(bus_type, 0, 'All')
            sbox6 = st.selectbox('Filter the Data by Bus Type:', bus_type_option.keys())

        query_filters = []
        params = []

        # Add filters based on user selections
        if sbox1 != 'All':
            query_filters.append("govt_bus_name = %s")
            params.append(sbox1)
        
        if sbox2 != 'All':
            query_filters.append("route_name = %s")
            params.append(sbox2)

        # Only add price filter if selected
        if sbox3 != 'All':
            query_filters.append(f"price <= {sbox3}")
        
        # Only add rating filter if selected
        if sbox4 != 'All':
            query_filters.append(f"star_rating <= {sbox4}")

        if sbox5 != 'All':
            query_filters.append(f"seats_available <= {sbox5}")

        if sbox6 != 'All':
            if sbox6 != 'Other':  # For other bus types, we use a complex condition
                query_filters.append(f"bustype LIKE %s")
                params.append(f"%{sbox6}%")
            else:
                query_filters.append("bustype NOT LIKE '%Sleeper%' AND bustype NOT LIKE '%Semi-Sleeper%' AND bustype NOT LIKE '%Seater%' AND bustype NOT LIKE '%A/C%' AND bustype NOT LIKE '%NON A/C%'")

        # Construct final query with appropriate parameters
        if query_filters:
            final_query = f"SELECT * FROM bus_routes WHERE {' AND '.join(query_filters)}"
        else:
            final_query = "SELECT * FROM bus_routes"  # No filters, fetch all data
        # Fetch filtered data using the cached function
        df4 = fetch_filtered_data(final_query, tuple(params))  # Ensure params are a tuple
        st.dataframe(df4)