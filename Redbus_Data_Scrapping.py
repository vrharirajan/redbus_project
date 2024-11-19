# Import Libraries
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
route_names = []
route_link = []
govt_bus_set = []
govt_bus_set1 = []
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
#mycursor.execute("DROP DATABASE IF EXISTS redbus")
#mycursor.execute("CREATE DATABASE redbus")
#mycursor.execute("USE redbus")
#mycursor.execute("CREATE TABLE bus_routes (id INT PRIMARY KEY AUTO_INCREMENT,govt_bus_name VARCHAR(100), route_name VARCHAR(100), route_link VARCHAR(255), bus_start_point VARCHAR(100),bus_end_point VARCHAR(100), busname VARCHAR(100), bustype VARCHAR(100), departing_time VARCHAR(10), duration VARCHAR(20), reaching_time VARCHAR(10), star_rating DECIMAL(3,2), price DECIMAL(10,2), seats_available INT)")
engine = create_engine("mysql+mysqlconnector://root:4274@localhost/Redbus")

driver = webdriver.Chrome()
driver.maximize_window()
driver.get('https://www.redbus.in/online-booking/rtc-directory')
wait=WebDriverWait(driver,20)
# Scraping Government Bus Name
bus_service=driver.find_elements(By.XPATH,'//a [@class="D113_link"]')
for w in bus_service:
    govt_bus_set.append(w.get_attribute('href'))
    govt_bus_set.append(w.text)
seen = set()
seen1 = set()
govt_bus_link = [x for x in govt_bus_set if not (x in seen or seen.add(x))]
govt_bus = [x for x in govt_bus_set if not (x in seen1 or seen1.add(x))]
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
for a in range(21,22):
    try:
        actions = ActionChains(driver)
        driver.get(govt_bus_link[a])
        page_number=[]
        pg_c = driver.find_elements(By.XPATH, '//div[@class="DC_117_pageTabs "]')
        page_number.append('1')
        for t in pg_c:
            page_number.append(t.text)
        max_page_num = int(max(page_number))
        for x in range (0, len(page_number)):
            page_number[x] = int(page_number[x])
    except NoSuchElementException:
        pass
    try:
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
                        WebDriverWait(driver, 30).until(lambda driver: driver.execute_script('return document.readyState') == 'complete')
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
    except NoSuchElementException:
        pass
driver.close()
# Storing dataframe to sql db
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
mycursor.close()
conn.close()
