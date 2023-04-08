# Upwork Job Tracker + Mail Alarm + Skill Monitor Dashboard
## Project Description

As a freelancer, it can be challenging to keep up with the constantly changing demands of the market. That's why I chose to create a personal Upwork job tracking and notification system to help me stay ahead of the competition. The data prosessed in this project can be seen in this Looker Studio Dashboard.

By tracking Upwork job postings in real-time and analyzing the required skills and qualifications, this project provides me personalized push notifications by email for the job listings that match my skills and preferences. This saves me time and effort by highlighting the best opportunities and filtering out unwanted job postings.

On the other hand, by analyzing the job listings and identifying the most commonly requested skills, I can understand the current market trends and adjust my skills accordingly with the market. 

## Skills used in this project
1) Data Analysis/Interpretation
2) Data Visualization & Dashboard
3) Python
4) SQL (Bigquery)
5) Data Mining

## Tools used

1) Python
2) Google Platform (Cloud Functions, Cloud Storage, Google Bigquery, Cloud Scheduer, Looker Studio)

## Highlights
#### A) The main ETL is created in Google Cloud Function and Scheduer
1) Cloud Storage is used to save all the historic data
2) All the information is scraped with an RSS created with my Upwork Account.
3) Most of the transformation is made with Pandas
4) The Entities are analyzed with Spacy
5) Each job post is scored with a value from -100 to 100, based on the skills required in it
6) There is an alarm send into my personal Gmail account when there is a job I can apply to.
#### B) Bigquery is used in this project to pivot and organize array columns to create the views needed for Looker Studoi  
#### C) The data is displayed with Looker Studio in this Dashboard
