# Good Day User!!
## Sections
- [Setup To Run this Data Engineering System (Lexicon Construction)](#setup-to-run-this-data-engineering-system-lexicon-construction)
- [Run the script](#run-the-script)
- [View the analysis output](#view-the-analysis-output)
- [Access the lexicon](#access-the-lexicon-without-directly-go-to-neo4j-database-website)
- [Folder Strucutre](#folder-strucutre)

## Setup To Run this Data Engineering System (Lexicon Construction):
1. Get your distro ready!

   i. Make sure you imported the latest distro provided by Choo Jun (release: 1.0.0.20241114)

   ii. Perform task from **Practical 0 to 4.1** provided at Choo Jun GitHub, This is to ensure all your de-venv is properly setup accordingly as well as dfs, yarn, kafka and environment variables

3. Start **dfs, yarn, zookeeper and kafka server services** (basically the first step in Practical 5)
4. su - student (switch to student)
5. Place DE_malay_news folder in your student home directory
6. Activate your de-venv, (python virtual environment)
7. **Run the DE_malay_news/setup.sh** file at python virtual environment setup duirng practical class
8. Navigate to config.json file under DE_malay_news
9. Upate the value with your neo4j credentials and when you wish to start scrap the data from.
10. (Optional) set ideal number of peribahasa to register in neo4j
11. Before running the script
    i. make sure that the neo4j database provided is empty to ensure data relevancy.
   ii. make sure your hdfs doesn't have DE-prj folder _if this is your first run_.
  
## Run the script:
   Before proceeding to Option 1, and 2, create a crontab for linux user first.
   #### **Option 1**: run the bash file `schedule-run.sh` with argument format **(most ideal way)**
   - `./schedule-run.sh <max_number_of_links_to_scrap - optional> <cron schedule ("* * * * *")>`
   - e.g. - `./schedule-run.sh 1 "30 * * * *"`
   - Then the pipeline will be run at background.
   - To view the real-time output, navigate to DE_malay_news/logs, find the latest created log files
   - Then, fire this command `tail -f <the latest log file file path>`
   
   For the first time schedule, if you found out there is output "no crontab for user" after you ran the .sh file with valid input, just ignore it, it stil successfully registed to crontab, you crontab -e to verify it.

   #### **Option 2**: schedule the run-lexicraft.sh file yourself on crontab using `crontab -e` **(most ideal way)**
   - **Note that, you must follow this format to schedule the run.
   - Format: `<crontab schedule> /home/student/DE_malay_news/run-lexicraft.sh <max_number_of_links_to_scrap - optional>`

   #### **Option 3**: run the bash file `run-lexicraft.sh` with argument format
   - `./schedule-run.sh <max_number_of_links_to_scrap - optional>`
   - This only will run one cycle of lexicon construction


   ##### Note that the cycle might crash due to hardware dependencies. If the log files show that pyspark error, it's mostly due to hardware dependecies.
   By changing the number of partitions, in the code, you might be able to run one cycle.

   
   ##### Note when you run on the first time on fresh distro or machine, you may encounter hadoop map reduce function stucking. 
   If you encounter this issue, you have to rerun the cycle again. The issue that process stuck on Hadoop MapReduce should be solved.

## View the analysis output
Note, there only will have analysis output when analysis is ran during the cycle or manually run on notebook "4. LexiconAnalysis.ipynb"
1. Open the "5. AnalysisVisualization.ipynb" under DE_malay_news directory
2. Run all cell to view the output

## Access the lexicon without directly go to Neo4j database website
1. Open the AccessLexicon.ipynb under DE_malay_news directory
2. Run the first cell, a menu should be shown on the output cell.
   
## Folder Strucutre
| Folders    | Description |
| --------------------------- | ------- |
| DE-prj  | Sample of folder output to hdfs when the system is ran with one fully cycle    | 
| analysis_csv_result | store some of the analysis result in csv format    | 
| analyisis_spark_buffer    | buffer as temporary store for analysis result when structure stearming is used to retrieve analysis result from kafka broker    | 
| extrinsic_sample_data    | consist sample data for extrinsic evaluation   | 
| **lexicraft** | The package and library for this Data Engineering Projects, consist all the python files to run the script | 
| logs | consists of log files for each cycle. e.g. log_2024-12-21_14-51-48.log | 

| Files    | Description |
| ------------------------------------- | ------- |
| 1. DataCollection.ipynb | Jupyter Notebook showing data collection process |
| 2. Transformation.ipynb | Jupyter Notebook showing data transformation process |
| 3. LexiconCreation.ipynb | Jupyter Notebook showing lexicon creation into neo4j process |
| 4. LexiconAnalysis.ipynb | Jupyter Notebook showing lexicon analysis process |
| 5. AnalysisVisualization.ipynb    | Jupyter Notebook to show lexicon analysis result in graphs |
| Accesslexicon.ipynb | Jupyter Notebook showing how to access neo4j lexicon wihout login to neo4j database |
| README.md | ReadMe file for users |
| config.json | configuration on neo4j database location, scrap berita harian articles from which date, number of peribahasa register into neo4j lexicon |
| data_output.json | buffer file for retrieve data collected from kafka |
| run-lexicraft.py | python script file to run the lexicon creation process (one cycle) |
| run-lexicraft.sh | bash file to simplify command needed to run the  lexicon creation process (one cycle) |
| schedule-run.sh | schedule the run-lexicrafh.sh on crontab |
| unschedule.sh | unschedule the run-lexicrafh.sh from crontab |
   
