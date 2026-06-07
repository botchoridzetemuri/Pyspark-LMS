## Overview
This project demonstrates data engineering and analysis skills using **PySpark** and **PostgreSQL**. It connects to a local PostgreSQL instance hosting the [Pagila](https://github.com/devrimgunduz/pagila) database (a PostgreSQL port of the Sakila sample database) via JDBC, and performs various complex data aggregations, joins, and window functions to extract business insights.

## Technologies Used
* **Python 3**
* **Apache Spark (PySpark)**
* **PostgreSQL**
* **Java 17** (for Spark JVM)
* **PostgreSQL JDBC Driver**

## Tasks Solved
The `main.py` script executes the following 7 data analysis tasks:
1. **Category Counts:** Outputs the number of movies in each category, sorted in descending order.
2. **Top Actors:** Outputs the 10 actors whose movies rented the most, sorted in descending order.
3. **Highest Revenue Category:** Outputs the category of movies on which the most money was spent.
4. **Missing Inventory:** Outputs the names of movies that exist in the catalog but are not in the physical inventory (using a Left Anti Join).
5. **Top Children's Movie Actors:** Outputs the top 3 actors who have appeared most in "Children" category movies, handling ties using Window functions and `dense_rank()`.
6. **Customer Activity by City:** Outputs cities with the count of active and inactive customers, sorted by inactive customers.
7. **Rental Hours by City Name:** Calculates total rental hours and outputs the top movie category for:
   * Cities starting with the letter "a".
   * Cities containing a "-" symbol.

## Setup and Installation

### 1. Database Setup
1. Install PostgreSQL.
2. Download the `pagila-schema.sql` and `pagila-data.sql` files from the official repository.
3. Create a database named `pagila` and run the SQL scripts to populate the tables.

### 2. Environment Setup
1. Install Java 17.
2. Install PySpark: `pip3 install pyspark`
3. Download the PostgreSQL JDBC Driver (`postgresql-42.x.x.jar`).

### 3. Running the Code
Update the `main.py` file to point to your local JDBC `.jar` file path and your PostgreSQL database credentials. Then run:
