# Import Libraries
import streamlit as st
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import StaleElementReferenceException
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import time
import numpy as np
from datetime import datetime
from pyodbc import ProgrammingError
from concurrent.futures import ThreadPoolExecutor
import csv
import os

# Global lists for storing scraped data
route = [] 
route1 = []
route_names = []
route_link = []
govt_bus_set = []
govt_bus = list(govt_bus_set)
govt_bus_name = []

#Establish a connection to the MySQL database.
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="4274",
    )
#Create the database and the bus_routes table if they don't exist.
conn = get_db_connection()
mycursor = conn.cursor()
mycursor.execute("DROP DATABASE IF EXISTS redbus")
mycursor.execute("CREATE DATABASE redbus")
mycursor.execute("USE redbus")
mycursor.execute("CREATE TABLE bus_routes (id INT PRIMARY KEY AUTO_INCREMENT,govt_bus_name VARCHAR(100), route_name VARCHAR(100), route_link VARCHAR(255), bus_start_point VARCHAR(100),bus_end_point VARCHAR(100), busname VARCHAR(100), bustype VARCHAR(100), departing_time VARCHAR(10), duration VARCHAR(20), reaching_time VARCHAR(10), star_rating DECIMAL(3,2), price DECIMAL(10,2), seats_available INT)")
engine = create_engine("mysql+mysqlconnector://root:4274@localhost/Redbus")

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
if st.sidebar.button('Data_Scraping'):
    st.session_state.view = 'Option 2'
st.sidebar.write('Step 3') 
if st.sidebar.button('Data_Warehouse'):
    st.session_state.view = 'Option 3'
st.sidebar.write('Step 4') 
if st.sidebar.button('Data_Filtering'):
    st.session_state.view = 'Option 4'
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
    driver = webdriver.Chrome()
    driver.maximize_window()
    driver.get('https://www.redbus.in/online-booking/rtc-directory')
    st.title('DATA SCRAPING PROCESS STRATED')    
    wait=WebDriverWait(driver,20)
    # Scraping Government Bus Name
    bus_service=driver.find_elements( By.CLASS_NAME,"D113_item_rtc")
    for w in bus_service:
        govt_bus_set.append(w.text)
    seen = set()
    govt_bus = [x for x in govt_bus_set if not (x in seen or seen.add(x))]
    
    # Check and delete if the CSV file is exists
    current_dir = os.getcwd()
    file = "Redbus_Data.csv"
    file_path = os.path.join(current_dir, file)
    if os.path.exists(file_path):
        del file_path
    # Creating a emptpy CSV file with Column Headings
    header = ['govt_bus_name','route_name','route_link','bus_start_point','bus_end_point','busname','bustype','departing_time','duration','reaching_time','star_rating','price','seats_available']
    with open('Redbus_Data.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)
    file.close()
    # Scraping the data from redbus website
    for a in range(0,11):
        actions = ActionChains(driver)
        elm = driver.find_element(By.XPATH,f'//a[text()="{govt_bus[a]}"]')
        actions.move_to_element(elm).click(elm).perform()
        page_number=[]
        pg_c = driver.find_elements(By.XPATH, '//div[@class="DC_117_pageTabs "]')
        page_number.append('1')
        for t in pg_c:
            page_number.append(t.text)
        max_page_num = int(max(page_number))
        for x in range (0, len(page_number)):
            page_number[x] = int(page_number[x])
        
        st.header(f'No. {a+1}: Data Scraping for the Bus : {govt_bus[a]}')

        for j in page_number: # Page Navigation for Scraping the data

            try:
                actions = ActionChains(driver)
                nxt_pg = driver.find_element(By.XPATH,f'//div[contains(@class,"DC_117_pageTabs") and text()="{j}"]')
                actions.move_to_element(nxt_pg).click(nxt_pg).perform()
                route_details=driver.find_elements(By.XPATH,'//a [@class="route"]')
                route_ext = driver.find_elements(By.XPATH, '//*[@id="root"]/div/div[4]/div[2]/div[1]')
                for m in route_details:
                    route_link.append(m.get_attribute('href'))
                for k in route_details:
                    route.append(k.text)
                    route1.append(k.text)
                    route_names.append(k.text)
            except NoSuchElementException:
                pass
            finally:
                
                for route,route_link in zip(route,route_link): # Iterating through each bus routes
                    route_name,busname,bus_start_point,bus_end_point,bustype,price,departing_time,reaching_time,duration,star_rating,route_link_df,seats_available = [],[],[],[],[],[],[],[],[],[],[],[]
                    try:
                        actions = ActionChains(driver)
                        pg1 = driver.find_element(By.XPATH,f'//div[contains(@class,"DC_117_pageTabs") and text()="{j}"]')
                        actions.move_to_element(pg1).click(pg1).perform()
                        page = driver.execute_script("return Math.max(document.body.scrollHeight,document.body.offsetHeight,document.documentElement.clientHeight,document.documentElement.scrollHeight)")
                        actions = ActionChains(driver)
                        element = driver.find_element(By.XPATH, '//a[text()="{0}"]'.format(route,route_link))
                        actions.move_to_element(element).click(element).perform()
                        
                        element1 = wait.until(EC.presence_of_element_located((By.XPATH,'//i [@class="p-left-10 icon icon-down"]')))
                        actions.move_to_element(element1).click(element1).perform()
                        time.sleep(1)
                        element2 = wait.until(EC.presence_of_element_located((By.XPATH,'//*[@id="result-section"]/div[2]/div[1]/div[2]/div/div[4]/div[2]')))
                        driver.execute_script("arguments[0].scrollIntoView()", element2)
                        actions.move_to_element(element2).click(element2).perform()
                        WebDriverWait(driver, 90).until(EC.presence_of_all_elements_located((By.XPATH,'//u1[contains(@class,"bus-items")]')))
                    except (NoSuchElementException, TimeoutException) as e:
                        pass
                    while True: # Page Scrolling
                        driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
                        page1 = driver.execute_script("return document.body.scrollHeight")
                        if driver.execute_script("return document.body.scrollHeight") == page:
                            break
                        page=page1
                        
                    try: # Scraping data
                        WebDriverWait(driver, 60).until(lambda driver: driver.execute_script('return document.readyState') == 'complete')
                        bus1=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class,"travels lh-24")]'))) # Busname
                        bus2=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class,"dp-loc l-color w-wrap")]'))) # bus_start_point
                        bus3=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class,"bp-loc l-color w-wrap")]'))) # bus_end_point
                        bus4=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class,"bus-type")]'))) # bustype
                        bus5=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//span[contains(@class,"f-19 f-bold")]|//span[contains(@class,"f-bold f-19")]'))) # price
                        bus6=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[contains(@class,"seat-left")]'))) # seats_available
                        bus7=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[@class="dp-time f-19 d-color f-bold"]'))) # departing_time
                        bus8=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[@class="bp-time f-19 d-color disp-Inline"]'))) # reaching_time
                        bus9=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[@class="dur l-color lh-24"]'))) # duration
                        bus10=wait.until(EC.presence_of_all_elements_located((By.XPATH, '//div[@class="rating-sec lh-24"]'))) # star_rating
                        # Storing the scraped date to  Global list                

                        busname = [element.text for element in bus1]
                        bus_start_point = [element.text for element in bus2]
                        bus_end_point = [element.text for element in bus3]
                        bustype = [element.text for element in bus4]
                        price = [element.text for element in bus5]
                        departing_time = [element.text for element in bus7]
                        reaching_time = [element.text for element in bus8]
                        duration = [element.text for element in bus9]
                        star_rating = [element.text for element in bus10]
                                            
                        for g in bus6:
                            seat=g.text[:2]
                            seats_available.append(seat)
                        
                        for c in bus1:
                            route_name.append(route)
                            govt_bus_name.append(govt_bus[a])
                            route_link_df.append(route_link)
                        
                    except TimeoutException:
                        
                        pass
                    st.write(f'Page No. {j } out of {max(page_number)}: Data Scraping completed for the Route :',route)
                     
                    #Creating the df and storing values from local and global list
                    df=pd.DataFrame(list(zip(govt_bus_name,route_name,route_link_df,bus_start_point,bus_end_point,busname,bustype,departing_time,duration,reaching_time,star_rating,price,seats_available)),columns=['govt_bus_name','route_name','route_link','bus_start_point','bus_end_point','busname','bustype','departing_time','duration','reaching_time','star_rating','price','seats_available'])
                    #Relacing the null values
                    replacement_values = {
                    'govt_bus_name': 'Unknown',
                    'route_name': 'Unknown',
                    'route_link': 'Unknown',
                    'bus_start_point': 'Unknown',
                    'bus_end_point': 'Unknown',
                    'busname': 'Unknown',
                    'bustype': 'Unknown',
                    'departing_time': 'Not Specified',
                    'duration': 'Unknown',  # Replace with mean for numeric column
                    'reaching_time': 'Unknown',
                    'star_rating': 'Unknown',  # Replace with mean for numeric column
                    'price': 0,  # Replace with mean for numeric column
                    'seats_available': 0  # Assuming seats available is numeric, replace with 0
                    }
                    df = df.fillna(value=replacement_values)
                    #Appending the Scraped date to CSV File
                    file_exists = os.path.isfile('Redbus_Data.csv')
                    df.to_csv('Redbus_Data.csv', mode='a', header=not file_exists, index=False)
                    #Making the local Variable list empty for storing next route details
                    govt_bus_name,route_name,busname,bus_start_point,bus_end_point,bustype,price,departing_time,reaching_time,duration,star_rating,route_link_df,seats_available = [],[],[],[],[],[],[],[],[],[],[],[],[]
                    driver.execute_script("window.history.go(-2)")
            # making Global Variable list empty to store next pages values
            route = []
            route_link = []
        driver.execute_script("window.history.go(-1)")
    driver.close()
    st.write('Date Scraping Completed')
    current_dir = os.getcwd()
    file_name = "Redbus_Data.csv"
    file_path = os.path.join(current_dir, file_name)
    if os.path.exists(file_path):
        st.session_state.dframe = pd.read_csv(file_path)
    
    st.dataframe(st.session_state.dframe)
    st.header('Step:3 - Press DataWarehouse Button from Navigation Bar')
# Storing dataframe to sql db
elif st.session_state.view == 'Option 3':
    st.write('Connecting to SQL Server to Store the Database')
    if 'dframe' in st.session_state:
        # Creating SQL Connection
        conn = get_db_connection()
        mycursor = conn.cursor()
        mycursor.execute("USE redbus")
        # Transfering the data to SQL database from the CSV file
        current_dir = os.getcwd()
        file_name = "Redbus_Data.csv"
        file_path = os.path.join(current_dir, file_name)
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
        df.to_sql('bus_routes', con=engine, if_exists='append', index=False)
        conn.commit()
        # Running the select query to check the data in SQL table
        mycursor.execute("SELECT * FROM bus_routes")
        myresult = mycursor.fetchall()
        columns = [desc[0] for desc in mycursor.description]
        df2 = pd.DataFrame(myresult, columns=columns)
        st.header("Data Stored in SQL Database")
        st.dataframe(df2)
        mycursor.close()
        conn.close()
        st.header('Step:4 - Press Data_Filtering Button from Navigation Bar')
elif st.session_state.view == 'Option 4':

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
        return df

    if 'dframe' in st.session_state:
        conn = get_db_connection()
        mycursor = conn.cursor()
        mycursor.execute("USE redbus")

        st.session_state.dframe.to_sql('bus_routes', con=engine, if_exists='append', index=False)
        conn.commit()

       # SQL Qyery Filter Creation based on Tranport Name 
        transport_name = st.session_state.dframe['govt_bus_name'].unique()
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
        bus_type = st.session_state.dframe['bustype'].unique()
        bus_type = np.insert(bus_type, 0, 'All')
        sbox5 = st.selectbox('Filter the Data by Bus Type:', bus_type)
        # SQL Qyery Filter Creation based on Route Name
        bus_route_name = st.session_state.dframe['route_name'].unique()
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