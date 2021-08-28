"""
Testing adding decorator to SQL query
"""

from flask import Flask, jsonify, Response
import psycopg2
from dotenv import load_dotenv
from functools import wraps
import json
import os

load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

app = Flask(__name__)

def get_query_response(f):
    @wraps(f)
    def wrapper(*args,**kwargs):
        connection = psycopg2.connect(dsn=DATABASE_URL)
        cursor = connection.cursor()
        cursor.execute(f())
        row_headers=[x[0] for x in cursor.description] #this will extract row headers
        row = cursor.fetchall()
        json_data=[]
        for result in row:
                json_data.append(dict(zip(row_headers,result)))
        cursor.close()
        connection.close()
        funded_data = json.dumps(json_data, default=str) # hack to jsonify datetime object
        return Response(funded_data, mimetype='application/json')
    return wrapper 

@app.route('/funded')
@get_query_response
def funded_last_ten(cursor):
    query = """
        SELECT 
            id, 
            company_name, 
            raise_date, 
            raise_amount_mill_dollars::float, 
            company_funding_round, 
            company_website 
        FROM raise
        LIMIT 25;
    """
    
    return query

@app.route('/funded/category/<string:category>')
@get_query_response
def funded_by_category(category):
    connection = psycopg2.connect(dsn=DATABASE_URL)
    cursor = connection.cursor()
    query = """
        SELECT 
            id, 
            company_name, 
            raise_date, 
            raise_amount_mill_dollars::float, 
            company_funding_round, 
            company_website, 
            company_category
        FROM raise
        WHERE company_category=%s
        LIMIT 10;
    """

    cursor.execute(query, (category,))

    row_headers=[x[0] for x in cursor.description]
    row = cursor.fetchall()
    json_data=[]
    for result in row:
            json_data.append(dict(zip(row_headers,result)))
    cursor.close()
    connection.close()

    category_data = json.dumps(json_data, default=str)
    return Response(category_data,  mimetype='application/json') 

@app.route('/funded/company/<string:company_name>')
def funded_company(company_name):
    connection = psycopg2.connect(dsn=DATABASE_URL)
    cursor = connection.cursor()
    query = """
        SELECT 
            id, 
            company_name, 
            raise_date, 
            raise_amount_mill_dollars::float, 
            company_funding_round, 
            company_website 
        FROM raise
        WHERE company_name=%s;
    """

    cursor.execute(query, (company_name,))

    row_headers=[x[0] for x in cursor.description]
    row = cursor.fetchall()
    json_data=[]
    for result in row:
            json_data.append(dict(zip(row_headers,result)))
    cursor.close()
    connection.close()

    company_data = json.dumps(json_data, default=str)
    return Response(company_data,  mimetype='application/json') 

if __name__ == '__main__':
    app.run()