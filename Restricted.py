import time
import redis
import json
from flask import Flask, request, render_template
import pyodbc
import random

app = Flask(__name__, template_folder='templates')

@app.route('/')
def redisdata():
    server = 'tcp:my-server01.database.windows.net'
    database = 'My-db'
    username = 'sxg6912'
    password = 'PoiuytrewQ@239'
    # driver= '{ODBC Driver 17 for SQL Server}'
    account_name = "assign1"
    account_key = "LRQ7KMFLfj76yCbLIdfF0VeLYyrNvkTWPi35Xt6vumz/XmL74jiUCeTzvSahTMKVTrN/N5AwqXm9+AStsZurlQ=="
    container_name = "assgn-pics"

    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)

    r = redis.Redis()

    cached_data = r.get('redis_earthquake')
    if cached_data is not None:
        ans = []
        cachestart_time = time.time()
        print(json.loads(cached_data))
        cacheend_time = time.time()
        cachetime = cacheend_time - cachestart_time
        print("Time Duration for the query result after caching:", cachetime)
        ans.append("Data is all Ready cached")
        ans.append(cachetime)
        return get_data(ans)

    query = "SELECT * FROM allearthquakedata WHERE latitude > 0 and longitude > 0 and place LIKE '%CA%' and mag > 4.0"

    if cached_data is None:
        ans = []
        start = time.time()
        cursor = cnxn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append(list(row))
        r.set(name = 'redis_earthquake', value=json.dumps(result))



        end = time.time()
        beforeCacheTime = end - start
        cached_data = r.get('redis_earthquake')

        if cached_data is not None:
            cachestart_time = time.time()
            print(json.loads(cached_data))
            cacheend_time = time.time()
            cachetime = cacheend_time - cachestart_time

        ans.append(beforeCacheTime)
        ans.append(cachetime)
        return get_data(ans)

@app.route('/')
def get_data(ans):
    return render_template("cacheResult.html", ans = ans)

app.run()