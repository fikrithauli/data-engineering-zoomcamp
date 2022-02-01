## Week 2 Homework

In this homework, we'll prepare data for the next week. We'll need
to put the NY Taxi data from 2019 and 2020 to our data lake.

For the lessons, we'll need the Yellow taxi dataset. For the homework 
of week 3, we'll need FHV Data (for-hire vehicles, for 2019 only).

You can find all the URLs on [the dataset page](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

In this homework, we will:

* Modify the DAG we created during the lessons for transferring the yellow taxi data
* Create a new dag for transferring the FHV data
* Create another dag for the Zones data


If you don't have access to GCP, you can do that locally and ingest data to Postgres 
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want.



## Question 1: Start date for the Yellow taxi data (1 point)

You'll need to parametrize the DAG for processing the yellow taxi data that
we created in the videos. 

What should be the start date for this dag?
Answer :
* **2019-01-01**


## Question 2: Frequency for the Yellow taxi data (1 point)

How often do we need to run this DAG?
Answer:
* **Monthly**


## Question 3: DAG for FHV Data (2 points)

Now create another DAG - for uploading the FHV data. 

We will need three steps: 

* Download the data
* Parquetize it 
* Upload to GCS

If you don't have a GCP account, for local ingestion you'll need two steps:

* Download the data
* Ingest to Postgres

Use the same frequency and the start date as for the yellow taxi dataset

Question: how many DAG runs are green for data in 2019 after finishing everything? 
Answer: **12 greens**


## Question 4: DAG for Zones (2 points)


Create the final DAG - for Zones:

* Download it
* Parquetize 
* Upload to GCS

(Or two steps for local ingestion: download -> ingest to postgres)

How often does it need to run?
Answer:
* **Once**
 