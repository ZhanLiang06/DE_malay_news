# Good Day User!!
## Setup To Run this Data Engineering System (Lexicon Construction):
1. Get your distro ready!

   i. Make sure you imported the latest distro provided by Choo Jun (release: 1.0.0.20241114)

   ii. Perform task from **Practical 0 to 5** provided at Choo Jun GitHub

3. Start **dfs, yarn, zookeeper and kafka server services** (basically the first step in Practical 5)
4. su - student (switch to student)
5. **Run the setup.sh** file at python virtual environment setup duirng practical class
6. Navigate to config.json file under DE_malay_news
7. Upate the value with your neo4j credentials and when you wish to start scrap the data from.
8. (Optional) set ideal number of peribahasa to register in neo4j
9. Before running the script
    i. make sure that the neo4j database provided is empty to ensure data relevancy.
   ii. make sure your hdfs doesn't have DE-prj folder _if this is your first run_.
  
## Run the script:

   #### **Option 1**: run the bash file `schedule-run.sh` with argument format **(most ideal way)**
   - `./schedule-run.sh <max_number_of_links_to_scrap - optional> <cron schedule ("* * * * *")>`
   - e.g. - `./schedule-run.sh 1 "30 * * * *"`
   - Then the pipeline will be run at background.
   - To view the real-time output, navigate to DE_malay_news/logs, find the latest created log files
   - Then, fire this command `tail -f <the latest log file file path>`

   #### **Option 2**: schedule the run-lexicraft.sh file yourself on crontab using `crontab -e` **(most ideal way)**
   - **Note that, you must follow this format to schedule the run.
   - Format: `<crontab schedule> /home/student/DE_malay_news/run-lexicraft.sh <max_number_of_links_to_scrap - optional>`

   #### **Option 3**: run the bash file `run-lexicraft.sh` with argument format
   - `./schedule-run.sh <max_number_of_links_to_scrap - optional>`
   - This only will run one cycle of lexicon construction

## To view the analysis output
Note, there only will have analysis output when analysis is ran during the cycle or manually run on notebook "4. LexiconAnalysis.ipynb"
1. Open the "5. AnalysisVisualization.ipynb" under DE_malay_news directory
2. Run all cell to view the output

## To access the lexicon without directly go to Neo4j database websites
1. Open the AccessLexicon.ipynb under DE_malay_news directory
2. Run the first cell, a menu should be shown on the output cell.
   
   
   
