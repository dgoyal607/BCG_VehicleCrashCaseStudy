# Vehicle Crash Case Study (BCG)

## Dataset:
The dataset for this case study consists of six CSV files located in the Data Set folder. To understand the dataset, please refer to the data dictionary provided via email. The dataset includes the following files:
1. Charges_use
2. Damages_use
3. Primary_Person_use
4. Unit_use
5. Endorse_use
6. Restrict_use


## Analytics:
The application is designed to perform the following analyses and store the results for each analysis:
1. Find the number of crashes (accidents) where the number of males killed is greater than 2.
2. Determine the count of two-wheelers involved in crashes.
3. Identify the top 5 vehicle makes of cars present in crashes where the driver died and airbags did not deploy.
4. Determine the number of vehicles involved in hit and run incidents where the driver has a valid license.
5. Identify the state with the highest number of accidents in which females are not involved.
6. Determine the 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries, including death.
7. For each unique body style involved in crashes, mention the top ethnic user group.
8. Identify the top 5 zip codes with the highest number of crashes involving alcohol as a contributing factor, using driver zip codes.
9. Count the distinct crash IDs where no damaged property was observed, and the damage level (VEH_DMAG_SCL~) is above 4, and the car is insured.
10. Determine the top 5 vehicle makes where drivers are charged with speeding-related offenses, have valid licenses, used the top 10 vehicle colors, and have cars licensed in the top 25 states with the highest number of offenses.

## Expected Output:
1. Develop an application following software engineering best practices, such as modular design, classes, docstrings, and config-driven approaches. 
2. Organize the code properly into folders as a project.
3. Input data sources and output should be configured through a config file.
4. Strictly use Data Frame APIs for coding (avoid Spark SQL).
5. Share the entire project as a zip file or provide a link to the GitHub repository.

## Execution Steps:
To execute the project, follow these steps:
1. Open an Anaconda prompt and run `build`. This command will build the artifact to run via spark-submit.
3. In the Anaconda prompt, run `spark-submit --master local[*] --py-files VehicleCrash.zip --class VehicleCrash VehicleCrash/main.py "dev" "configs/env.yaml"` to execute the application.
