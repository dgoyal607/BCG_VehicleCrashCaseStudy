#!/usr/bin/env python
# coding: utf-8

# In[10]:


"""
Module Name         : Vehicle Crash Case Study
Purpose             : Analyze vehicle crash data to extract insights
Pre_requisites      : PySpark environment, input data files in CSV format
Inputs              : CSV files containing vehicle crash data
Execution Steps     : 
                      1. Open an Anaconda prompt and run `build`. This command will build the artifact to run via spark-submit.
                      2. In the Anaconda prompt, run `spark-submit --master local[*] --py-files VehicleCrash.zip --class VehicleCrash VehicleCrash/main.py "dev" "configs/env.yaml"` to execute the application.
   
Last changed on     : 08-Feb-2024
Last changed by     : Deepak Goyal
"""

__author__ = "Deepak Goyal"




import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from utils import futil 
import yaml


class VehicleCrash:
    
    def __init__(self, config_path, spark, env):
        
        """
        Initializes an instance of the data processor.

        Parameters:
        - config_path (str): The path to the configuration file.
        - spark (SparkSession): The SparkSession object.
        - env (str): The environment name.

        This method reads configuration data from the specified YAML file and sets up input/output file paths and 
        formats based on the specified environment. It also reads CSV data files into Spark DataFrames for different 
        entities (e.g., Person, Charges, Damages).
        """
        
        self.config_path = config_path
        self.spark = spark
        self.env = env

        with open(config_path, "r") as f:
            config_data = yaml.safe_load(f)

        self.input_file_paths = config_data.get(env).get("input")
        self.output_file_paths = config_data.get(env).get("output")
        self.output_file_format = config_data.get(env).get("file_format").get("format")

        self.df_person = futil.read_csv_data(spark, self.input_file_paths.get("Person"))
        self.df_charge = futil.read_csv_data(spark, self.input_file_paths.get("Charges"))
        self.df_damage = futil.read_csv_data(spark, self.input_file_paths.get("Damages"))
        self.df_endorse = futil.read_csv_data(spark, self.input_file_paths.get("Endorse"))
        self.df_units = futil.read_csv_data(spark, self.input_file_paths.get("Units"))
        self.df_restrict = futil.read_csv_data(spark, self.input_file_paths.get("Restrict"))
        
        
        
    def CollectList(self, df, column_name):
        """
        Collects values from a DataFrame column as a list.

        Args:
        - self: The instance of the class.
        - df: The DataFrame from which to collect values.
        - column_name: The name of the column from which to collect values.

        Returns:
        - A list containing the values from the specified column.
        """
        
        return [row[column_name] for row in df.select(column_name).collect()]
    
    
    def CollectTupleList(self, df, column_name1, column_name2):
        """
        Collects values from two DataFrame columns as a list of tuples.

        Args:
        - self: The instance of the class.
        - df: The DataFrame from which to collect values.
        - column_name1: The name of the first column from which to collect values.
        - column_name2: The name of the second column from which to collect values.

        Returns:
        - A list containing tuples, each tuple containing values from the specified columns.
        """

        return [(row[column_name1], row[column_name2]) for row in df.select(column_name1, column_name2).collect()]



    
    def CountMaleAccidents(self):
        
        """
        Find the number of crashes (accidents) in which number of males killed are greater than 2?

        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is the count of accidents where male individuals were killed and more than two such individuals were involved.
          - The second element is a DataFrame containing the details of accidents meeting the criteria.
        """
        
        df_person_male_killed = self.df_person\
            .select('crash_id', 'unit_nbr', 'prsn_nbr', 'PRSN_GNDR_ID', 'PRSN_INJRY_SEV_ID').distinct()\
            .where((upper(col('PRSN_GNDR_ID')) == 'MALE') & (upper(col('PRSN_INJRY_SEV_ID')) == 'KILLED')).orderBy('crash_id')

        df_male_killed_more_than_2 = df_person_male_killed.groupBy('crash_id').agg(count('*').alias('cnt')).filter(col('cnt') > 2)

        return [df_male_killed_more_than_2.count(), df_male_killed_more_than_2]
    

    
    
    def CountTwoWheeler(self):
        
        """
        How many two wheelers are booked for crashes?

        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is the count of two-wheeler vehicles involved in accidents.
          - The second element is a DataFrame containing the details of accidents involving two-wheeler vehicles.
        """
        
        df_units_2_wheeler = self.df_units.select('crash_id', 'unit_nbr', 'VEH_BODY_STYL_ID').distinct()\
        .where(upper(col('VEH_BODY_STYL_ID')).like('%MOTORCYCLE%'))
        
        return [df_units_2_wheeler.count(), df_units_2_wheeler]
    
    

    
    def TopFiveCarBrand(self):
        
        """
        Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy?

        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is a list of the top five car brands involved in such accidents.
          - The second element is a DataFrame containing the details of car brands involved in accidents meeting the criteria.
        """
        
        df_units_filter = self.df_units.select('crash_id', 'unit_nbr', 'VEH_BODY_STYL_ID', 'VEH_MAKE_ID').distinct()\
        .where(upper(col('VEH_BODY_STYL_ID')).like('%CAR%'))

        df_person_filter = self.df_person\
        .select('crash_id', 'unit_nbr', 'prsn_nbr', 'PRSN_TYPE_ID', 'PRSN_AIRBAG_ID', 'DEATH_CNT', 'PRSN_INJRY_SEV_ID').distinct()\
        .where( (upper(col('PRSN_TYPE_ID'))=='DRIVER') & (upper(col('PRSN_AIRBAG_ID'))=='NOT DEPLOYED') & (upper(col('PRSN_INJRY_SEV_ID'))=='KILLED') )

        df_person_unit_merge = df_units_filter.join(df_person_filter, ['crash_id', 'unit_nbr'], 'inner')\
        .groupBy('VEH_MAKE_ID').agg(count('*').alias('brand_cnt')).orderBy(col('brand_cnt').desc()).limit(5).select('VEH_MAKE_ID')

        return [self.CollectList(df_person_unit_merge, 'VEH_MAKE_ID'), df_person_unit_merge]
    
    
    
    
    def HitAndRunVehicle(self):
        
        """
        Determine number of Vehicles with driver having valid licences involved in hit and run? 

        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is the count of vehicles involved in hit-and-run incidents with drivers having valid licenses.
          - The second element is a DataFrame containing the details of vehicles involved in such incidents.
        """
        
        df_units_filter = self.df_units.select('crash_id', 'unit_nbr', 'VEH_HNR_FL').distinct()\
        .where(col('VEH_HNR_FL')=='Y')

        df_person_filter = self.df_person\
        .select('crash_id', 'unit_nbr', 'prsn_nbr', 'PRSN_TYPE_ID', 'DRVR_LIC_TYPE_ID').distinct()\
        .where( upper(col('PRSN_TYPE_ID')).isin('DRIVER', 'DRIVER OF MOTORCYCLE TYPE VEHICLE') & upper(col('DRVR_LIC_TYPE_ID')).isin('COMMERCIAL DRIVER LIC.', 'OCCUPATIONAL', 'DRIVER LICENSE') )

        df_person_unit_merge = df_units_filter.join(df_person_filter, ['crash_id', 'unit_nbr'], 'inner')

        return [df_person_unit_merge.count(), df_person_unit_merge]
    
    
    
    def HighestAccidentState(self):
        
        """
        Which state has highest number of accidents in which females are not involved? 

        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is the state with the highest number of accidents where females are not involved.
          - The second element is a DataFrame containing the details of the state with the highest number of accidents.
        """
        
        df_person_filter = self.df_person.select('crash_id', 'unit_nbr', 'prsn_nbr', 'PRSN_TYPE_ID', 'DRVR_LIC_STATE_ID', 'PRSN_GNDR_ID').distinct()

        df_person_filter.persist()

        df_person_female_involved = df_person_filter.where(upper(col('PRSN_GNDR_ID'))=='FEMALE').select('crash_id').distinct()

        crash_ids_female_involved = [row.crash_id for row in df_person_female_involved.collect()]

        df_person_states = df_person_filter.filter(~col('crash_id').isin(crash_ids_female_involved))\
        .groupBy('DRVR_LIC_STATE_ID').agg(countDistinct('crash_id').alias('no_of_accidents')).orderBy(col('no_of_accidents').desc())\
        .select('DRVR_LIC_STATE_ID').limit(1)

        return [self.CollectList(df_person_states, 'DRVR_LIC_STATE_ID'), df_person_states]
    
    
    
    
    def TopInjuriesCarBrand(self):
        
        """
        Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death?

        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is a list of VEH_MAKE_IDs ranking 3rd to 5th in terms of contributing to the largest number of injuries, including death.
          - The second element is a DataFrame containing the details of the VEH_MAKE_IDs ranking 3rd to 5th.
        """
        
        df_units_filter = self.df_units.select('crash_id', 'unit_nbr', 'VEH_MAKE_ID', 'TOT_INJRY_CNT', 'DEATH_CNT').distinct()

        df_injury_counts = df_units_filter.groupBy("VEH_MAKE_ID")\
        .agg(sum(col('TOT_INJRY_CNT')).alias('INJURY_COUNT'), sum(col('DEATH_CNT')).alias('DEATH_COUNT'))

        df_total_injury_counts = df_injury_counts.withColumn("TOTAL_INJURIES_COUNT", col("INJURY_COUNT") + col("DEATH_COUNT"))\
        .select('VEH_MAKE_ID', 'TOTAL_INJURIES_COUNT')

        window_spec = Window.orderBy(col('TOTAL_INJURIES_COUNT').desc())

        df_top_3_to_5_brands = df_total_injury_counts.withColumn('rn', row_number().over(window_spec)).filter(col('rn').between(3,5))\
        .drop('rn')

        return [self.CollectList(df_top_3_to_5_brands, 'VEH_MAKE_ID'), df_top_3_to_5_brands]
    
    
      
    
    def TopEthnicVehicle(self):
        
        """
        For all the body styles involved in crashes, mention the top ethnic user group of each unique body style?

        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is a list of tuples, where each tuple contains the unique body style and its corresponding top ethnic user group.
          - The second element is a DataFrame containing the details of the top ethnic user group for each unique body style.
        """

        df_units_filter = self.df_units.select('crash_id', 'unit_nbr', 'VEH_BODY_STYL_ID').distinct()\
        .where(~col('VEH_BODY_STYL_ID').isin('NA', 'UNKNOWN', 'OTHER  (EXPLAIN IN NARRATIVE)', 'NOT REPORTED'))

        df_person_filter = self.df_person.select('crash_id', 'unit_nbr', 'prsn_nbr', 'PRSN_ETHNICITY_ID').distinct()\
        .where(~col('PRSN_ETHNICITY_ID').isin('NA', 'UNKNOWN', 'OTHER'))

        df_person_unit_merge = df_units_filter.join(df_person_filter, ['crash_id', 'unit_nbr'], 'inner')\
        .groupBy(['VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID']).agg(count('*').alias('person_count'))\
        .orderBy('VEH_BODY_STYL_ID', col('person_count').desc())

        window_spec = Window.partitionBy('VEH_BODY_STYL_ID').orderBy(col('VEH_BODY_STYL_ID').desc())

        df_top_ethnic = df_person_unit_merge.withColumn('rn', row_number().over(window_spec))\
        .where(col('rn')==1).select('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID')

        return [self.CollectTupleList(df_top_ethnic, 'VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID'), df_top_ethnic]
    
    
    
    
    def TopFiveZipCodes(self):
        
        """
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)?

        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is a list of the top 5 zip codes with the highest number of crashes involving alcohol as a contributing factor.
          - The second element is a DataFrame containing the details of the top 5 zip codes.
        """
        
        df_units_filter = self.df_units.select('crash_id', 'unit_nbr', 'CONTRIB_FACTR_1_ID', 'CONTRIB_FACTR_2_ID').distinct()\
        .where(upper(col('CONTRIB_FACTR_1_ID')).like('%ALCOHOL%') | upper(col('CONTRIB_FACTR_2_ID')).like('%ALCOHOL%'))

        df_person_filter = self.df_person.select('crash_id', 'unit_nbr', 'prsn_nbr', 'DRVR_ZIP').distinct()\
        .where(col('DRVR_ZIP').isNotNull())

        df_person_unit_merge = df_units_filter.join(df_person_filter, ['crash_id', 'unit_nbr'], 'inner')\
        .groupBy('DRVR_ZIP').agg(countDistinct('crash_id').alias('crash_count')).orderBy(col('crash_count').desc()).limit(5)

        return [self.CollectList(df_person_unit_merge, 'DRVR_ZIP'), df_person_unit_merge]
    
    
    
    
    def CountCrashNoDmagPrpty(self):
        
        """
        Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance?

        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is the count of distinct crash IDs meeting the specified criteria.
          - The second element is a DataFrame containing the details of crashes meeting the criteria.
        """
        
        df_damage_filter = self.df_damage.select('crash_id', 'DAMAGED_PROPERTY').distinct()\
        .where(col('DAMAGED_PROPERTY').like('%NONE%'))

        df_units_filter = self.df_units.select('crash_id', 'unit_nbr', 'VEH_DMAG_SCL_1_ID', 'VEH_DMAG_SCL_2_ID', 'FIN_RESP_TYPE_ID').distinct()\
        .withColumn("VEH_DMAG_SCL_1_value", regexp_extract("VEH_DMAG_SCL_1_ID", r"DAMAGED\s*(\d+)", 1).cast('int'))\
        .withColumn("VEH_DMAG_SCL_2_value", regexp_extract("VEH_DMAG_SCL_2_ID", r"DAMAGED\s*(\d+)", 1).cast('int'))\
        .where( (col('VEH_DMAG_SCL_1_value').isNotNull() & (col('VEH_DMAG_SCL_1_value')>4)
               | col('VEH_DMAG_SCL_2_value').isNotNull() & (col('VEH_DMAG_SCL_2_value')>4))
               & upper(col('FIN_RESP_TYPE_ID')).like('%INSURANCE%')
              )\
        .select('crash_id', 'unit_nbr', 'VEH_DMAG_SCL_1_value', 'VEH_DMAG_SCL_2_value').distinct()

        df_damage_unit_merge = df_units_filter.join(df_damage_filter, ['crash_id'], 'inner')\
        .select('crash_id').distinct()

        return [df_damage_unit_merge.count(), df_damage_unit_merge]
    
    
    

    def TopFiveVehSpeedOffences(self):
        
        """
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)?
        
        Args:
        - self: The instance of the class.

        Returns:
        - A list containing two elements:
          - The first element is a list of the top 5 vehicle makes meeting the specified criteria.
          - The second element is a DataFrame containing the details of the top 5 vehicle makes.
        """
        
        df_units_filter = self.df_units.select('crash_id', 'unit_nbr', 'VEH_COLOR_ID', 'VEH_LIC_STATE_ID', 'VEH_MAKE_ID').distinct()
        
        df_charge_filter = self.df_charge.select('crash_id', 'unit_nbr', 'prsn_nbr', 'charge').distinct()\
        .where(upper(col('charge')).like('%SPEED%'))

        df_person_filter = self.df_person.select('crash_id', 'unit_nbr', 'prsn_nbr', 'DRVR_LIC_TYPE_ID').distinct()\
        .where(upper(col('DRVR_LIC_TYPE_ID')).isin("DRIVER LICENSE", "COMMERCIAL DRIVER LIC."))

        df_top_10_vehicle_colors = df_units_filter.where(~col('VEH_COLOR_ID').isin('NA','99'))\
        .groupBy('VEH_COLOR_ID').agg(count('*').alias('color_count')).orderBy(col('color_count').desc()).limit(10)

        top_10_vehicle_colors = [row["VEH_COLOR_ID"] for row in df_top_10_vehicle_colors.collect()]

        df_top_25_states = df_units_filter.where(~col('VEH_LIC_STATE_ID').isin('98', 'NA'))\
        .groupBy('VEH_LIC_STATE_ID').agg(count('*').alias('state_count')).orderBy(col('state_count').desc()).limit(25)

        top_25_states = [row["VEH_LIC_STATE_ID"] for row in df_top_25_states.collect()]

        df_charge_person_merge = df_charge_filter.join(df_person_filter, ['crash_id', 'unit_nbr', 'prsn_nbr'], 'inner')\
        .join(df_units_filter, ['crash_id', 'unit_nbr'], 'inner').distinct()\
        .where(col('VEH_COLOR_ID').isin(top_10_vehicle_colors) & col('VEH_LIC_STATE_ID').isin(top_25_states))\
        .orderBy(col('crash_id').desc())\
        # .select('crash_id', 'unit_nbr', 'VEH_MAKE_ID').distinct()

        df_vehicle_agg = df_charge_person_merge.groupBy('VEH_MAKE_ID').agg(count('*').alias('offence_count'))\
        .orderBy(col('offence_count').desc()).limit(5)

        return [self.CollectList(df_vehicle_agg, 'VEH_MAKE_ID'), df_vehicle_agg]
        
        

        
def main():
    
    spark = SparkSession.builder.appName("VehicleCrashCaseStudy").master("local[*]").getOrCreate()
    
    
    user_args = sys.argv[1:]

    env = user_args[0]
    config_path = user_args[1]
    
    vc = VehicleCrash(config_path, spark, env)
    
    CountMaleAccidents_output = vc.CountMaleAccidents()
    print("1. CountMaleAccidents: {}".format(CountMaleAccidents_output[0]))
    futil.write_df(CountMaleAccidents_output[1], vc.output_file_paths.get('Answer1'), vc.output_file_format)

    
    CountTwoWheeler_output = vc.CountTwoWheeler()
    print("2. CountTwoWheeler: {}".format(CountTwoWheeler_output[0]))
    futil.write_df(CountTwoWheeler_output[1], vc.output_file_paths.get('Answer2'), vc.output_file_format)


    TopFiveCarBrand_output = vc.TopFiveCarBrand()
    print("3. TopFiveCarBrand: {}".format(TopFiveCarBrand_output[0]))
    futil.write_df(TopFiveCarBrand_output[1], vc.output_file_paths.get('Answer3'), vc.output_file_format)
    
    
    HitAndRunVehicle_output = vc.HitAndRunVehicle()
    print("4. HitAndRunVehicle: {}".format(HitAndRunVehicle_output[0]))
    futil.write_df(HitAndRunVehicle_output[1], vc.output_file_paths.get('Answer4'), vc.output_file_format)
    
    
    HighestAccidentState_output = vc.HighestAccidentState()
    print("5. HighestAccidentState: {}".format(HighestAccidentState_output[0]))
    futil.write_df(HighestAccidentState_output[1], vc.output_file_paths.get('Answer5'), vc.output_file_format)
    
    
    TopInjuriesCarBrand_output = vc.TopInjuriesCarBrand()
    print("6. TopInjuriesCarBrand: {}".format(TopInjuriesCarBrand_output[0]))
    futil.write_df(TopInjuriesCarBrand_output[1], vc.output_file_paths.get('Answer6'), vc.output_file_format)
    
    TopEthnicVehicle_output = vc.TopEthnicVehicle()
    print("7. TopEthnicVehicle: {}".format(TopEthnicVehicle_output[0]))
    futil.write_df(TopEthnicVehicle_output[1], vc.output_file_paths.get('Answer7'), vc.output_file_format)
    
    
    TopFiveZipCodes_output = vc.TopFiveZipCodes()
    print("8. TopFiveZipCodes: {}".format(TopFiveZipCodes_output[0]))
    futil.write_df(TopFiveZipCodes_output[1], vc.output_file_paths.get('Answer8'), vc.output_file_format)
    
    CountCrashNoDmagPrpty_output = vc.CountCrashNoDmagPrpty()
    print("9. CountCrashNoDmagPrpty: {}".format(CountCrashNoDmagPrpty_output[0]))
    futil.write_df(CountCrashNoDmagPrpty_output[1], vc.output_file_paths.get('Answer9'), vc.output_file_format)
    
    TopFiveVehSpeedOffences_output = vc.TopFiveVehSpeedOffences()
    print("10. TopFiveVehSpeedOffences: {}".format(TopFiveVehSpeedOffences_output[0]))
    futil.write_df(TopFiveVehSpeedOffences_output[1], vc.output_file_paths.get('Answer10'), vc.output_file_format)

    
    
if __name__ == '__main__':
    main()
    


# In[ ]:




