from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
import xml.etree.ElementTree as ET
from airflow.models import TaskInstance
import oracledb
import csv
import os
from dotenv import load_dotenv
import requests
import urllib3
import smtplib
from email.message import EmailMessage
import ssl

with DAG(dag_id="etl_sql_dag", start_date=datetime(2022, 1, 1), schedule="@once", default_args= {"retries": 1}) as dag:

    # --------------------------------------------------------------------------------------------------------------------------
    # -------------------------------------------SHOULD-BE-IN-MODULE-FOLDER-----------------------------------------------------
    # --------------------------------------------------------------------------------------------------------------------------
    
    # connect to oracle database tables
    def check_connect_to_db(**kwargs):
        load_dotenv()
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_dns = os.getenv("DB_DNS")

        print(db_user, db_password, db_dns)
        conn = oracledb.connect(user=db_user, password=db_password, dsn=db_dns)

        print(conn.version)
        cursor = conn.cursor()
        try:
            sql = """
            SELECT * FROM PERSONS
            """
            cursor.execute(sql)
            result = cursor.fetchall()
            cursor.close()

            # If sql returns data, return True
            if result != None:
                print("connected to database successful")
                kwargs['ti'].xcom_push(key='check_connect_to_db', value="success")
                return True
            else:
                print("No data returned from sql")
                kwargs['ti'].xcom_push(key='check_connect_to_db', value="failed")
                raise AirflowException("No data returned from sql")
        except oracledb.Error as e:
            print("Oracle Error:", e)
            cursor.close()
            kwargs['ti'].xcom_push(key='check_connect_to_db', value=e)
            raise AirflowException(e)
    
    # create csv file for storing data
    def create_csv_file(**kwargs):
        # create csv file
        
        print("creating csv file")
        csv_file = ".\data\demo_xml.csv"

        if os.path.exists(csv_file):
            try:
                os.remove(csv_file)
                print("Existing CSV file deleted")
            except Exception as e:
                print("Error deleting existing CSV file:", e)
                raise AirflowException(e)
            
        try:
            with open(csv_file, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["ID", "name", "email", "address", "createdate","loaddate"])
                print("csv file created")  
        except Exception as e:
            print(e)
            kwargs['ti'].xcom_push(key='create_csv_file', value=e)
            raise AirflowException(e)
        kwargs['ti'].xcom_push(key='create_csv_file', value="success")
        return True
    
    # get data from api and load to csv file
    def extract_to_csv(**kwargs):

        print("extracting data from api")
        api = "http://restapi.adequateshop.com/api/Traveler" 
        page = 1
        while True:
            request_session = requests.session()
            request_session.keep_alive = False
            urllib3.disable_warnings()
            try:
                result = request_session.get(url=api+f"?Page={page}",verify=False)
                print(result.raise_for_status())
            except Exception as e:
                result = f"Error: {e}"
                kwargs['ti'].xcom_push(key='extract_to_csv', value=e)
                raise AirflowException(result )

            print(f"Status Code: {result.status_code}")
            root = ET.fromstring(result.text)
            current_page = root.find('./page').text
            total_page = root.find('./total_pages').text
            total_traveler = root.find('./totalrecord').text
            print(f"Total traveler: {total_traveler}")
            print(f"Current page: {current_page} out of {total_page}")

            
            for elem in root.findall('.//Travelerinformation'):
                
                print(f"Traveler in {current_page}")
                export_array = []
                # find by field name
                # for attribute in elem:
                #     export_array.append(attribute.text)
                id = elem.find("id").text if elem.find("id") is not None else None
                name = elem.find("name").text if elem.find("name") is not None else "N/A"
                email = elem.find("email").text if elem.find("email") is not None else "N/A"
                addres = elem.find("adderes").text if elem.find("adderes") is not None else "N/A"
                createdat = elem.find("createdat").text if elem.find("createdat") is not None else "N/A"
                
                # Parse data from string
                if createdat == "0001-01-01T00:00:00":
                    createdat = "N/A"
                else:
                    createdat = str(createdat[:10])
                
                export_array.append(id)
                export_array.append(name)
                export_array.append(email)
                export_array.append(addres)
                export_array.append(createdat)

                # Some data is missing in the api
                export_array.append(str(datetime.now().strftime('%Y-%m-%d')))
                print(export_array)
                # save into a csv file created above
                csv_file = ".\data\demo_xml.csv"
                try:
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(export_array)
                        print("csv file updated")
                except Exception as e:
                    print(e) 
                    kwargs['ti'].xcom_push(key='extract_to_csv', value=e)
                    raise AirflowException(e)
                del export_array
            
            page = int(current_page) + 1
            if int(page) > int(total_page):
                break
        kwargs['ti'].xcom_push(key='extract_to_csv', value="success")
        return True

    # Load records in csv file into oracle db using bulk insert
    def load_to_db(**kwargs):
        # connect to oracle database tables
        load_dotenv()
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_dns = os.getenv("DB_DNS")

        conn = oracledb.connect(user=db_user, password=db_password, dsn=db_dns)
        print(conn.version)
        cursor = conn.cursor()
        csv_path = ".\data\demo_xml.csv"
        try:
            sql = f"""
            INSERT INTO TRAVELER (ID, NAME, EMAIL, ADDRESS, CREATEDATE, LOADDATE) VALUES (:1, :2, :3, :4, :5, :6)
            """

            df = pd.read_csv(csv_path)
            df['ID'] = df['ID'].astype(int)
            df["name"] = df["name"].astype(str)
            df["email"] = df["email"].astype(str)
            df["address"] = df["address"].astype(str)
            df["createdate"] = df["createdate"].astype(str)
            df["loaddate"] = df["loaddate"].astype(str)
            print (df.dtypes)
            print (df.head())

            cursor.executemany(sql, df.values.tolist())
    
            conn.commit()
            cursor.close()
            conn.close()
            print ("loaded to database successful")

        except oracledb.Error as e:
            print("Oracle Error:", e)
            cursor.close()
            conn.close()
            kwargs['ti'].xcom_push(key='load_to_db', value=e)
            raise AirflowException(e)
        kwargs['ti'].xcom_push(key='load_to_db', value="success")
        return True
    
    # Delete the csv file after loading to db
    def delete_csv_file(**kwargs):
        
        csv_path = ".\data\demo_xml.csv"
        try:
            os.remove(csv_path)
            print("csv file deleted")
        except Exception as e:
            print(e) 
            kwargs['ti'].xcom_push(key='delete_csv_file', value=e)
        kwargs['ti'].xcom_push(key='delete_csv_file', value="success")
        return True
    
    # Send email after ETL process completed
    def send_email(**kwargs):

        load_dotenv()
        email_sender = os.getenv("EMAIL_SENDER")
        email_password = os.getenv("EMAIL_PASSWORD")
        email_receiver = os.getenv("EMAIL_RECEIVER")

        print("sending email")
        task1 = kwargs['ti'].xcom_pull(key='check_connect_to_db', task_ids='check_connect_to_db')
        task2 = kwargs['ti'].xcom_pull(key='create_csv_file', task_ids='create_csv_file')
        task3 = kwargs['ti'].xcom_pull(key='extract_to_csv', task_ids='extract_to_csv')
        task4 = kwargs['ti'].xcom_pull(key='load_to_db', task_ids='load_to_db')
        task5 = kwargs['ti'].xcom_pull(key='delete_csv_file', task_ids='delete_csv_file')

        print(f"Task 1 status: {task1}")
        print(f"Task 2 status: {task2}")
        print(f"Task 3 status: {task3}")
        print(f"Task 4 status: {task4}")
        print(f"Task 5 status: {task5}")
        
        em = EmailMessage()
        
        if task1 == "success" and task2 == "success" and task3 == "success" and task4 == "success" and task5 == "success":
            em['Subject'] = "ETL process completed at " + str(datetime.now())
            em['From'] = email_sender
            em['To'] = email_receiver
            em.set_content("All tasks completed successfully" + "\n" + "ETL process completed at " + str(datetime.now())
                        + "\n" + "Regards," + "\n" + "PIPELINE" + "\n")
        else:
            em['Subject'] = "ETL process failed at " + str(datetime.now())
            em['From'] = email_sender
            em['To'] = email_receiver
            em.set_content("Some tasks failed" + "\n" + "ETL process failed at " + str(datetime.now())
                        + "\n" + "Task 1: " + task1 + "\n" + "Task 2: " + task2 + "\n" + "Task 3: " + task3 
                        + "\n" + "Task 4: " + task4 + "\n" + "Task 5: " + task5
                        + "\n" + "Please check the log for more details"
                        + "\n" + "Regards," + "\n" + "PIPELINE" + "\n")
        
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
            server.login(email_sender, email_password)
            server.send_message(em)
        return True
    
    # --------------------------------------------------------------------------------------------------------------------------
    # -------------------------------------------SHOULD-BE-IN-MODULE-FOLDER-----------------------------------------------------
    # --------------------------------------------------------------------------------------------------------------------------

    # task operators
    # xcom: cross communication between tasks, can pass a small amount of data between tasks
    # provide_context=True: pass the context to the callable function, must be set for using ti.xcom_pull and push

    check_connect_to_db = PythonOperator(task_id='check_connect_to_db', 
                                        python_callable=check_connect_to_db,
                                        provide_context=True,
                                        trigger_rule="all_done")
    
    create_csv_file = PythonOperator(task_id='create_csv_file',
                                    python_callable=create_csv_file,
                                    provide_context=True,
                                    trigger_rule="all_done")
    
    extract_to_csv = PythonOperator(task_id='extract_to_csv',
                                    python_callable=extract_to_csv,
                                    provide_context=True,
                                    trigger_rule="all_success")
    
    load_to_db = PythonOperator(task_id='load_to_db',
                                python_callable=load_to_db,
                                provide_context=True,
                                trigger_rule="all_success")
    
    delete_csv_file = PythonOperator(task_id='delete_csv_file',
                                    python_callable=delete_csv_file,
                                    provide_context=True,
                                    trigger_rule="all_done")

    send_email = PythonOperator(task_id='send_email_task',
                                python_callable=send_email,
                                provide_context=True,
                                trigger_rule="all_done")
    # task flow
    check_connect_to_db >> create_csv_file >> extract_to_csv >> load_to_db >>  delete_csv_file >> send_email
    
    