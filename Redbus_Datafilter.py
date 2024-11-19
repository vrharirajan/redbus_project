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
        # SQL Qyery Filter Creation based on Tranport Name 
        transport_name = df['govt_bus_name'].unique()
        transport_name = np.insert(transport_name, 0, 'All')
        sbox1 = st.selectbox('Filter the Data by Government Transport Name:', transport_name)
        # SQL Qyery Filter Creation based on Price Range
        price_range_options = {
            'All': None,
            'Below 1000': "price <= 1000",
            '1000 - 2000': "price BETWEEN 1000 AND 2000",
            '2000 - 3000': "price BETWEEN 2000 AND 3000",
            'Above 3000': "price >= 3000"
        }
        sbox2 = st.selectbox('Filter the Data by Bus Price Range:', price_range_options.keys())
        # SQL Qyery Filter Creation based on Rating
        rating_options = {
            'All': None,
            '2 stars and below': "star_rating <= 2",
            '3 to 4 stars': "star_rating BETWEEN 3 AND 4",
            'Above 4 stars': "star_rating > 4"
        }

        sbox3 = st.selectbox('Filter the Data by Bus Rating:', rating_options.keys())
        # SQL Qyery Filter Creation based on Seat Availablity
        Seats = {
            'All': None,
            '10 Seats and Below': "seats_available <= 10",
            '11 to 20 Seats': "seats_available BETWEEN 11 AND 20",
            '20 to 30 Seats': "seats_available BETWEEN 20 AND 30",
            '30 to 40 Seats': "seats_available BETWEEN 30 AND 40",
            '40 Seats and Above': "seats_available >= 40"
        }
        
        sbox4 = st.selectbox('Filter the Data by Seat Availbility:',Seats.keys() )
        # SQL Qyery Filter Creation based on Bus Type
        bus_type = df['bustype'].unique()
        bus_type = np.insert(bus_type, 0, 'All')
        sbox5 = st.selectbox('Filter the Data by Bus Type:', bus_type)
        # SQL Qyery Filter Creation based on Route Name
        bus_route_name = df['route_name'].unique()
        bus_route_name = np.insert(bus_route_name, 0, 'All')
        sbox6 = st.selectbox('Filter the Data by Bus Route Name:', bus_route_name)

        query_filters = []
        params = []

        # Add filters based on user selections
        if sbox1 != 'All':
            query_filters.append("govt_bus_name = %s")
            params.append(sbox1)

        # Only add price filter if selected
        if sbox2 != 'All':
            query_filters.append(price_range_options[sbox2])
        
        # Only add rating filter if selected
        if sbox3 != 'All':
            query_filters.append(rating_options[sbox3])

        if sbox4 != 'All':
            query_filters.append(Seats[sbox4])

        if sbox5 != 'All':
            query_filters.append("bustype = %s")
            params.append(sbox5)

        if sbox6 != 'All':
            query_filters.append("route_name = %s")
            params.append(sbox6)

        # Construct final query with appropriate parameters
        if query_filters:
            final_query = f"SELECT * FROM bus_routes WHERE {' AND '.join(query_filters)}"
        else:
            final_query = "SELECT * FROM bus_routes"  # No filters, fetch all data
        # Fetch filtered data using the cached function
        df4 = fetch_filtered_data(final_query, tuple(params))  # Ensure params are a tuple
        st.dataframe(df4)
# Deleting the final CSV file from the path
current_dir = os.getcwd()
file = "Redbus_Data.csv"
file_path = os.path.join(current_dir, file)
if os.path.exists(file_path):
    del file_path