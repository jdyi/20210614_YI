####################################################################
# Data Model Creation
####################################################################

# ONE TIME RUN

import sqlite3

# Connect to db
con = sqlite3.connect('psDatabase.sqlite3')
cur = con.cursor()

# create user dimension table 
sql = """
    CREATE TABLE users_dim (
        userId varchar(256) primary key, 
        smoker varchar(256),
        drink_level varchar(256)
        -- etc...
        )  
"""

cur.execute(sql)

# create place dimension table
sql = """
    CREATE TABLE places_dim (
        placeId primary key varchar(256)
        )  
"""

cur.execute(sql)

# create user place fact table 
sql = """
    CREATE TABLE userplace_fact (
        userplaceId primary key varchar(256),
        userId varchar(256),
        placeId varchar(256),
        restRating int,
        foodRating int,
        serviceRating int,
        salesAmount float,
        visitDate date,
        FOREIGN KEY(userId) REFERENCES users_dim(userId)
        FOREIGN KEY(placeId) REFERENCES places_dim(placeId)
        )  
"""

cur.execute(sql)

# create cuisine fact table 
sql = """
    CREATE TABLE placecuisine_fact (
        placecuisineId primary key varchar(256),
        placeId varchar(256),
        servedCuisines varchar(256),
        FOREIGN KEY(placeId) REFERENCES places_dim(placeId)
        )  
"""

cur.execute(sql)
