from flask import Flask, jsonify, Response, render_template
import psycopg2
import json
import os

# from dotenv import load_dotenv
# load_dotenv()
# DATABASE_URL = os.environ.get('DATABASE_URI')

app = Flask(__name__)
DATABASE_URL = os.environ.get('DATABASE_URI')

@app.route('/')
def home_view():
    """
    Provides some information on endpoints
    """
    """
    <h1> Welcome </h1>
    <h3> This is a simple REST API with information on VC funded companies </h3>
    <br> Data is updated roughly every month, and there are currently +30K entries in the full dataset. 
    <br>
    <br> There are currently 3 test endpoints - more comprehensive documentation to follow...
    <br>
    <h5> 1) Return 25 recently funded start-ups; </h5>
    https://vc-funded-api.herokuapp.com/funded
    <br>
    <h5> 2) Returns companies funded by category: </h5>
    https://vc-funded-api.herokuapp.com/funded/category/<string:category>
    <br> Example endpoint: https://vc-funded-api.herokuapp.com/funded/category/Cybersecurity
    <br>
    <h5> 3) Returns companies funded by name: </h5>
    https://vc-funded-api.herokuapp.com/funded/company/<string:company_name>
    <br> Example endpoint: https://vc-funded-api.herokuapp.com/funded/company/Tessian
    """
    return render_template("main.html")

@app.route('/funded')
def funded_recently():
    """
    This endpoint returns all the recently funded companies 
    Limit to last 25 for demo - this is roughly the number funded in the last week
    """
    try:
        connection = psycopg2.connect(dsn=DATABASE_URL)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT 
                id, 
                company_name, 
                raise_date, 
                raise_amount_mill_dollars::float, 
                company_funding_round, 
                company_website
                company_category 
            FROM raise
            LIMIT 25;
        """)
        row_headers=[x[0] for x in cursor.description] #this will extract row headers
        row = cursor.fetchall()
        json_data=[]
        for result in row:
                json_data.append(dict(zip(row_headers,result)))
        cursor.close()
        connection.close()
        funded_data = json.dumps(json_data, default=str) # hack to jsonify datetime object
        return Response(funded_data, mimetype='application/json') 
        
    except Exception as e:
        return {'error': str(e)} 

@app.route('/funded/category/<string:category>')
def funded_by_category(category):
    """
    This endpoint returns all the recently funded companies 
    Limit to last 25 for demo - this is roughly the number funded in the last week
    """
    try:
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
        # row = jsonify(cursor.fetchall())

        row_headers=[x[0] for x in cursor.description]
        row = cursor.fetchall()
        json_data=[]
        for result in row:
                json_data.append(dict(zip(row_headers,result)))
        cursor.close()
        connection.close()

        if len(row) < 1:
            return jsonify({'errorCode' : 404, 'message' : 'Category not found'})

        category_data = json.dumps(json_data, default=str) # hack to jsonify datetime object
        return Response(category_data,  mimetype='application/json')
    
    except Exception as e:
        return {'error': str(e)} 

@app.route('/funded/company/<string:company_name>')
def funded_company(company_name):
    try:
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

        if len(row) < 1:
            return jsonify({'errorCode' : 404, 'message' : 'Company not found'})

        company_data = json.dumps(json_data, default=str) # hack to jsonify datetime object
        return Response(company_data,  mimetype='application/json') 
    
    except Exception as e:
        return {'error': str(e)}

if __name__ == '__main__':
    app.run()