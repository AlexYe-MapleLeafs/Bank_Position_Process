# Bank_Position_Process

Bank Loan and Investment Status Process in Google Cloud Platform

This is an modified process from my previous real work project, to demonstrate the necessary codes to automate a process in Google Cloud Platform

The purpose of files/folder:

1. scripts folder:
   The python scripts in scripts folder are to use SQL in python to fetch data and populate target tables in Bigquery.
   The real project will have more than 12 python scripts in that folder. However, for the demonstration purpose, we omit most of the python scripts for simplicity.

2. accp.yaml:
   accp.yaml file is to tell accp pipeline the instruciton about the necessary information to build the image/container.
   we mention accp.yaml file on accp pipeline initiation process.

3. Dockerfile:
   Dockerfile has detailed info about how to set up the image, include the packges needed to run the python scripts.
   Dockerfile is referenced in accp.yaml file

4. requirement.txt
   listing all packages needed to install in the image.
   requirement.txt is referenced in Dockerfile.

5. positions_dag.py
   dag file is uploaded to airflow to set up and schedule process in airflow.
   Once the image is created based on files mentioned above, the image is referenced in this dag file.
