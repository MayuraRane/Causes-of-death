# Causes of Death

## Table of contents

- [Overview](#overview)
  - [Architecture](#architecture)
  - [Tools](#tools-used)
- [Process](#process)
  - [Dataset](#dataset)
  - [Data Cleaning](#data-cleaning)
  - [Data Transformation](#data-transformation)
  - [Data Visualization](#data-visualization)
  - [Tableau Dashboard Link](#tableau-dashboard-link)
- [Summary](#summary)

## Overview

The agenda of this project was to practice an end-to-end ETL project with data visualization.

### Architecture:
<img width="1600" alt="image" src="https://github.com/MayuraRane/Causes-of-death/assets/42894788/aa71f16f-f96f-480a-82ed-069c9f5fa0da">

### Tools used:
- Amazon S3: Used to **extract** data
- Databricks: Generally used to process big data. In this case, we use it to run Python code
- Python: Used for data **transformation**
- Snowflake: Used to **load** data in a tabular form
- Tableau: Used for **data visualization**

## Process
### Dataset

Used the dataset "Causes of Death - Our World Data" from Kaggle: [Link](https://www.kaggle.com/datasets/ivanchvez/causes-of-death-our-world-in-data)

### Data Cleaning

Data cleaning performed:
- Changed column names
- Changed datatypes
- Filled nulls with 0
- Removed some records with nulls

### Data Transformation

Transformations performed:
- Reshaped the data (Performed un-pivoting)
- Removed unwanted columns
- Only considering countries which have all year's data from 1990 to 2019 

### Data Visualization

<img width="700" alt="image" src="https://github.com/MayuraRane/Causes-of-death/assets/42894788/825e0c2c-5d68-4656-a33f-5a5b4bb31a09">
<br>
<img width="480" alt="image" src="https://github.com/MayuraRane/Causes-of-death/assets/42894788/19bd80f0-e6c7-450d-8796-6946c69951d8">
<br>
<img width="686" alt="image" src="https://github.com/MayuraRane/Causes-of-death/assets/42894788/c42f8670-056e-4488-8f70-87026cfb22d9">


### Tableau Dashboard Link

Tableau Dahsboard: [Link](https://public.tableau.com/app/profile/mayura.rane/viz/CausesOfDeath_17084140203460/CauesofDeathWorldWide)

## Summary

Overall, I learned how to integrate different technologies for the ETL process.

