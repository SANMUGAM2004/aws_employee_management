from flask import Flask, render_template, request
from pymysql import connections
import os
import boto3
from config import *

app = Flask(__name__)

bucket_new = bucket
region_new = region

db_conn = connections.Connection(
    host=host,
    port=3306,
    user=user,
    password=password,
    db=db
)
output = {}
table = 'empdet'

@app.route("/", methods=['GET', 'POST'])
def home():
    return render_template('AddEmp.html')

@app.route("/addemp", methods=['POST'])
def AddEmp():
    emp_id = request.form['emp_id']
    first_name = request.form['first_name']
    last_name = request.form['last_name']
    pri_skill = request.form['pri_skill']
    location = request.form['location']
    emp_image_file = request.files['emp_image_file']
    
    insert_sql = "INSERT INTO empdet VALUES (%s, %s, %s, %s, %s)"
    cursor = db_conn.cursor()
    if emp_image_file.filename == "":
        return "Please select a file"
    try:
        cursor.execute(insert_sql, (emp_id, first_name, last_name, pri_skill, location))
        db_conn.commit()
        emp_name = "" + first_name + " " + last_name
        # Uplaod image file in S3 #
        emp_image_file_name_in_s3 = "emp-id-" + str(emp_id) + "_image_file"
        s3 = boto3.resource('s3')
        try:
            print("Data inserted in MySQL RDS..")
            print("Image is Uploaded in S3")
            s3.Bucket(bucket_new).put_object(Key=emp_image_file_name_in_s3, Body=emp_image_file)
            bucket_location = boto3.client('s3').get_bucket_location(Bucket=bucket_new)
            s3_location = (bucket_location['LocationConstraint'])

            if s3_location is None:
                s3_location = ''
            else:
                s3_location = '-' + s3_location
            object_url = "https://s3{0}.amazonaws.com/{1}/{2}".format(s3_location,bucket_new,emp_image_file_name_in_s3)
        except Exception as e:
            return str(e)

    finally:
        cursor.close()

    print("all modification done...")
    return render_template('AddEmpOutput.html', name=emp_name)

@app.route("/getemp", methods=['GET', 'POST'])
def getemp():
    return render_template('GetEmp.html')


@app.route("/fetchdata", methods=['POST'])
def GetEmp():
    search_type = request.form.get('search_type')
    search_value = request.form.get('search_value')
    emp_id = request.form.get('emp_id')

    if search_type == 'emp_id':
        query = f"SELECT * FROM empdet WHERE emp_id = {search_value}"
    elif search_type == 'emp_name':
        query = f"SELECT * FROM empdet WHERE first_name LIKE '%{search_value}%' OR last_name LIKE '%{search_value}%'"
    elif search_type == 'primary_skills':
        query = f"SELECT * FROM empdet WHERE pri_skill LIKE '%{search_value}%'"
    else:
        return "Invalid search type"

    cursor = db_conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    
    s3 = boto3.client('s3')
    bucket_location = s3.get_bucket_location(Bucket=bucket_new)
    s3_location = bucket_location['LocationConstraint']

    if s3_location is None:
        s3_location = ''
    else:
        s3_location = '-' + s3_location

    object_key = f"emp-id-{emp_id}_image_file"
    image_url = f"https://s3{s3_location}.amazonaws.com/{bucket_new}/{object_key}"

    return render_template('GetEmpOutput.html', output=result, image_url=image_url)

    


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
