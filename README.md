# Coffee Shop Data Analysis

## Prompt
Sample-HIVE-Project:
This project is mainly for learning and practicing simple HIVE commands in real time scenarios. Here we have taken some sample coffee shop data and processed some essential queries to demonstrate HDFS & HIVE commands. Using Spark and Scala With Coffee shop data.

## Project Description
We were given CSV files with sample data about a Coffee Shop including beverages associated with different branches of the shop and files with consumer count numbers for the different beverages. We were tasked with running HIVE queries against the data to make useful analysis of it. There were some initial queries we were asked to create and implement to find basic information such as the total number of consumers for a specific branch or the common beverages between different branches. After that we had to come up with our own query that would display data that we could use to predict trends in the Coffee Shop. After completing the queries, we utilized a visualization tool such as Excel of Zeppelin to create graphical displays of our tables and see the trends.

## Technologies Used
- IntelliJ IDE with SBT(1.6.2)
- Scala Programming Language (scala 2.13.8)
- Spark lib dependencies (spark v.3.2.0)
- Spark Session which enable HIVE support (hive v.2.3.9)
- Microsoft Excel for visualization

## Features
List of features and TODOs for future development:
- Program reads in data from multiple CSV files and stores them in Spark SQL tables
- Uses appropriate queries to gather information on the data from the CSV files
- Creates a menu for the user to easily choose which queries to run and which tables to see.
To-do list:
- Implement better exception handling for the menu
- Optimize the code
- Get more creative with visualization

## Getting Started
Use the git clone command in your terminal to clone this repo and be able to use it in an IntelliJ IDE:
```
git clone https://github.com/PatrickGABrown/P1.git
```

## Usage
Once you clone the repo, the main object is located in the Project1 directory in the src/main/scala path.
All of the CSV files used are located in the input directory.
Once you run the project it will boot up the menu which you can follow to see different query results.

