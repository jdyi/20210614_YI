import pandas as pd
import sqlite3

####################################################################
# Question 1
####################################################################
# Output file was modified to fit csv format for ease-of-access. 
# The actual output of this function is a list of lists.

# Edge cases to note:

# 1. If the restaurant has not made any sales in the selected period, they are not included in this query. 
# I chose to do this because there are many cuisine types that do not have any sales from their restaurants and if we include them, it would not be coherent with the data.
# For example, the "Asian" cuisine has no sales from their restaurants and if we chose to include these results, we would need to include three Asian cuisine restaurants
# which do not have any sales. 
 
# 2. If a restaurant has the same summed sale, they will be ordered by placeId in ascending order and the ordered placeIds
# will be chosen starting from the lowest placeId to fill in the top 3 restaurant criteria. 
# We do not want to output all results if they have the same summed result because we only want 3 restaurants 
# in our output for each cuisine.

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

    df = pd.DataFrame(ans1, columns=["servedCuisines", "placeId", "summedSale"])


    return ans




# Output to csv for queryable file
# topThreePerCuisine('2020-01-01', '2021-01-01')

ans1 = topThreePerCuisine('2020-01-01', '2021-01-01')
df = pd.DataFrame(ans1, columns=["servedCuisines", "placeId", "summedSale"])
df.to_csv('Answer1.csv', index=False, header=True)

#topThreePerCuisine('2020-05-09', '2020-05-10')   


####################################################################
# Question 2
####################################################################

# Edge cases to note:

# 1. This query also excludes any restaurants that have $0 in sales, as it would not be coherent
# with the data (explained in question 1, same concept). 
 
# 2. If a restaurant has the same summed sale, all restaurants with the same summed sale will be 
# shown in the output. This is different than question 1 because we no longer have a restriction of 
# showing only the top 3 restaurants.

# 3. If a cuisine has no Nth restaurant by summed sale, then that cuisine will not be shown


def topNPerCuisine(N: int, startDate: str, endDate: str):

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
                DENSE_RANK() OVER(PARTITION BY pc.servedCuisines ORDER BY c.summedSales desc) as dr
            FROM placecuisine_fact pc 
            JOIN summed c on c.placeId = pc.placeId -- inner join to get rid of any restaurants with 0 sales in the specified period
            ORDER BY pc.servedCuisines, pc.placeId ASC
        )
        select 
            r.servedCuisines,
            r.placeId,
            ROUND(r.summedSales, 2) as sale
        from ranked r
        where r.dr = """ + str(N) + """
        ORDER BY r.servedCuisines, sale DESC
    """
    cur.execute(sql)

    ans = cur.fetchall()
    return ans

# Output to csv for queryable file
# topNPerCuisine(3, '2020-01-01', '2021-01-01')

ans2 = topNPerCuisine(3, '2020-01-01', '2021-01-01')
df = pd.DataFrame(ans2, columns=["servedCuisines", "placeId", "summedSale"])
df.to_csv('Answer2.csv', index=False, header=True)


####################################################################
# Question 3
####################################################################

def avgConsecutiveVisits(visitDate):

    con = sqlite3.connect('psDatabase.sqlite3')
    cur = con.cursor()

    sql = """
        SELECT 
            userId, 
            CASE
                WHEN COUNT(visitDate) > 1 
                    THEN 
                        ROUND(CAST((MAX(strftime('%H', visitDate)) - MIN(strftime('%H', visitDate))) AS FLOAT) / 
                        CAST(COUNT(visitDate) AS FLOAT), 2)
                ELSE -9994
            END AS avg_cons
        FROM userplace_fact
        WHERE visitDate LIKE '""" + visitDate + """%'
        GROUP BY userId

    """
    cur.execute(sql)

    ans = cur.fetchall()
    return ans

# Output to csv for queryable file
# avgConsecutiveVisits('2020-05-09')
# avgConsecutiveVisits('2020-05-10')

ans3 = avgConsecutiveVisits('2020-05-09')
df = pd.DataFrame(ans3, columns=["userId", "avgConsecutiveHours"])
df.to_csv('Answer3.csv', index=False, header=True)
