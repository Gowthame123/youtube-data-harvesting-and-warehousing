# youtube-data-harvesting-and-warehousing 

Explores YouTube trends using Python, MySQL, MongoDB, and Streamlit. Gather data efficiently, store it smartly, and visualize insights easily Youtube channel details are retrieved using Youtube data API application after getting the key from it . We are processing the channel details using PYTHON programming language,with various libraries for the given Project such as,

from googleapiclient.discovery import build 
     This library is part of the Google API Client library and is used for interacting with various Google services. It allows you to create a service object for making API requests and fetching data from Google services.

2.from pymongo import MongoClient 
     PyMongo is a Python driver for MongoDB, a NoSQL database. It provides a convenient way to interact with MongoDB databases.

3.import mysql.connector 
     This library provides a Python interface for interacting with MySQL databases. It allows you to connect to a MySQL server, execute SQL queries, and manage database transactions.

4.import pandas as pd 
      Pandas is a powerful data manipulation library for Python. It provides data structures like DataFrames, which are efficient for handling and analyzing structured data.

5.from datetime import datetime,timedelta 
      The datetime module in Python provides classes for working with dates and times.

6.import streamlit as st 
      Streamlit is a Python library used for creating web applications with minimal effort.

7.import re 
      The re module is the regular expression module in Python. It provides functions for working with regular expressions, which are powerful tools for pattern matching in strings.

After extracting it the details of youtube channel, it is saved in MongoDB (unstructured database) and then transefered to SQL(structured database).
