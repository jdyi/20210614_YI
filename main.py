import json
import pandas as pd

# These particular json files have one object per line so in order to accomodate we go through line by line and store each object into a list
userData = [json.loads(line) for line in open('./jsonData/userDetails.json', 'r')]
placeData = [json.loads(line) for line in open('./jsonData/placeDetails.json', 'r')]

# Grab attributes
print(userData[0].keys())
print(placeData[0].keys())


import sqlite3
con = sqlite3.connect('psDatabase.sqlite3')
cur = con.cursor()

####################################################################
# Data Model Creation
####################################################################

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
        placeId varchar(256) primary key
        )  
"""

cur.execute(sql)

# create user place fact table 
sql = """
    CREATE TABLE userplace_fact (
        userplaceId varchar(256) primary key,
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
        placecuisineId varchar(256) primary key,
        placeId varchar(256),
        servedCuisines varchar(256),
        FOREIGN KEY(placeId) REFERENCES places_dim(placeId)
        )  
"""

cur.execute(sql)

####################################################################
# Data Load
####################################################################

# insert data into users_dim table
for u in userData:
    cur.execute("insert into users_dim values (?,?,?)",
        [u['userID'], u['smoker'], u['drink_level']])
    con.commit()

# insert data into places_dim table
for p in placeData:
    cur.execute("insert into places_dim values (?)",
        [p['placeId']])
    con.commit()

# insert data into userplace_fact table
for up in userData:
    for pp in up['placeInteractionDetails']:
        cur.execute("insert into userplace_fact values (?,?,?,?,?,?,?,?)",
            [up['userID'] + '_' + pp['placeID'], up['userID'], pp['placeID'], pp['restRating'], pp['foodRating'], pp['serviceRating'], pp['salesAmount'], pp['visitDate']])
        con.commit()   

# insert data into placecuisine_fact table
for p in placeData:
    if ('servedCuisines' in p):
        for sc in p['servedCuisines']:
            cur.execute("insert into placecuisine_fact values (?,?,?)",
                [p['placeId'] + '_' + sc, p['placeId'], sc])
            con.commit()   

placeData[27].get('servedCuisines')

cur.execute("select * from placecuisine_fact")

asd = cur.fetchall()

####################################################################
# Question 1
####################################################################
# Output file was modified to fit csv format for ease-of-access. 
# The actual output of this function is a list of lists.

# Edge cases to note:
# 1. If the restaurant has not made any sales in the selected period, they are not included in this query. 
# I chose to do this because there are many cuisine types that do not have any sales from their restaurants and if we include them, it would not make sense with the data.
# For example, the "Asian" cuisine has no sales from their restaurants and if we chose to include these results, we would need to include three Asian cuisine restaurants
# which do not have any sales. 
 
# 2. If a restaurant has the same summed sale, they will be ordered by placeId in ascending order.

def topThreePerCuisine(startDate, endDate):

    con = sqlite3.connect('psDatabase.sqlite3')
    cur = con.cursor()

    sql = """
        -- These ctes grab each restaurant-cuisine and ranks them by their summed sales amount
        WITH summed AS (
            SELECT 
                up.placeId,
                SUM(up.salesAmount) as summedSales -- Summing like this will ensure that the sum is applied to all resturant-cuisinetype combos
            FROM userplace_fact up 
            WHERE up.visitDate >= '""" + startDate + """' and up.visitDate <= '""" + endDate + """'
            GROUP BY up.placeId
        ),
        ranked AS (
            SELECT 
                pc.servedCuisines, 
                pc.placeId,
                c.summedSales,
                ROW_NUMBER() OVER(PARTITION BY pc.servedCuisines ORDER BY c.summedSales desc) as dr
            FROM placecuisine_fact pc 
            JOIN summed c on c.placeId = pc.placeId -- inner join to get rid of any restaurants with 0 sales in the specified period
            ORDER BY pc.servedCuisines, pc.placeId ASC
        )
        select 
            r.servedCuisines,
            r.placeId,
            ROUND(r.summedSales, 2) as sale
        from ranked r
        where r.dr <= 3
        ORDER BY r.servedCuisines, sale DESC
    """
    cur.execute(sql)

    ans = cur.fetchall()
    return ans

# Output to csv for ease-of-access
# topThreePerCuisine('2020-01-01', '2021-01-01')

ans1 = topThreePerCuisine('2020-01-01', '2021-01-01')
df = pd.DataFrame(ans1, columns=["servedCuisines", "placeId", "summedSale"])
df.to_csv('Answer1.csv', index=False, header=True)

#topThreePerCuisine('2020-05-09', '2020-05-10')   


####################################################################
# Question 2
####################################################################


