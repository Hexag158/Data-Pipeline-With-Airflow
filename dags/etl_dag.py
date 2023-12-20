from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import xml.etree.ElementTree as ET
import csv
import  requests

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="etl_dag", start_date=datetime(2022, 1, 1), schedule="@once", default_args=
         {"retries": 1}) as dag:

    # Could not dirrectly create file in local because airflow is running on a virtual environment
    # Cannot find the file inside the virtual environment

    # @task()
    # def create_csv():
    #     print("creating csv file")
    #     csv_file = ""
    #     try:
    #         with open(csv_file, 'w', newline='') as file:
    #             writer = csv.writer(file)
    #             writer.writerow(["ID", "name", "email", "address", "createdate","importdate"])
    #             print("csv file created")
    #     except Exception as e:
    #         print(e)
    #     return True      


    @task()
    def ETL():
        csv_file = ".\dags\data\demo_xml.csv"
        print("transforming data")
        #date_time = datetime.today().strftime('%d-%b-%y-%H-%M-%S')
        api = "http://restapi.adequateshop.com/api/Traveler"
        date_time = "9-28-2023"
        page =1
        while True:
            # result = call_api(api,page)
            result = requests.get(api, params={'page': page})
            print(f"Status Code: {result.status_code}")
            root = ET.fromstring(result.text)
            current_page = root.find('./page').text
            total_page = root.find('./total_pages').text
            total_traveler = root.find('./totalrecord').text
            print(f"Total traveler: {total_traveler}")
            print(f"Current page: {current_page} out of {total_page}")

            for elem in root.findall('.//Travelerinformation'):
                print("new traveler")
                export_array = []
                for attribute in elem:
                    export_array.append(attribute.text)

                print (export_array)
                export_array.append(date_time)
                #save to csv
                print ("-----------------------------")
                del export_array
            page = int(current_page) + 1
            if int(page) > int(total_page):
                break

        print("ETL completed")
        return True
    
    ETL()