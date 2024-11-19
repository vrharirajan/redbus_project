# redbus_project
Redbus Data Scraping with Selenium & Dynamic Filtering using Streamlit

Problem Statement:
The "Redbus Data Scraping and Filtering with Streamlit Application" aims to revolutionize the transportation industry by providing a comprehensive solution for collecting, analyzing, and visualizing bus travel data. By utilizing Selenium for web scraping, this project automates the extraction of detailed information from Redbus, including bus routes, schedules, prices, and seat availability. By streamlining data collection and providing powerful tools for data-driven decision-making, this project can significantly improve operational efficiency and strategic planning in the transportation industry.

Solution:
This project is created by 2 stages;
  1. Data Scraping & Data Storage in SQL Database:
      - Selenium is used to scrap the data from the Redbus Website.
      - Government Transport will be iterated one by one
      - Bus route data for the Government Transport will be iterated page wise and the necessary data are scraped and stored in a global variable
      - All variables are stored in a CSV file for each route and appended by one route.
      - a database (Redbus) created in SQL and stored from the final CSV file as Table (bus_routes)
  2. Data Analysis/Filtering using Streamlit:
       - Developed a Streamlit application to display and filter the scraped data.
       - Implemented filters such as Government Transport Name, bustype, route, price range, star rating, and seat availability.
       - SQL queries used to retrieve and filter data based on user inputs.
     
