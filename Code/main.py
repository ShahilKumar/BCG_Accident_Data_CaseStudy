from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

import os
import sys

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './Code/src')

from utilities import utils


class USVehicleAccidentAnalysis:
    def __init__(self, path_to_config_file):
        input_file_paths = utils.read_yaml(path_to_config_file).get("INPUT_FILENAME")
        self.df_charges = utils.load_csv_data_to_df(spark, input_file_paths.get("Charges"))
        self.df_damages = utils.load_csv_data_to_df(spark, input_file_paths.get("Damages"))
        self.df_endorse = utils.load_csv_data_to_df(spark, input_file_paths.get("Endorse"))
        self.df_primary_person = utils.load_csv_data_to_df(spark, input_file_paths.get("Primary_Person"))
        self.df_units = utils.load_csv_data_to_df(spark, input_file_paths.get("Units"))
        self.df_restrict = utils.load_csv_data_to_df(spark, input_file_paths.get("Restrict"))

    def count_crashes_with_male_killed_gt_2(self, output_path, output_format):
        """
        Find the number of crashes (accidents) in which number of males killed are greater than 2.
        :param output_path: output file path
        :param output_format: Write file format
        :return: Count of number of crashes (accidents) in which number of males killed are greater than 2.
        """
        data = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == "MALE")
		df = (
            data.groupBy("CRASH_ID")
            .agg(col("DEATH_CNT").cast("int").alias("males_killed"))
            .filter(col("males_killed") > 2)
        )
        utils.write_output(df, output_path, output_format)
        return df.count()
		


    def count_2_wheeler_accidents(self, output_path, output_format):
            """
            Find how many two wheelers are booked for crashes.
            :param output_format: Write file format
            :param output_path: output file path
            :return: Count of how many two wheelers are booked for crashes.
            """
            df = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
            utils.write_output(df, output_path, output_format)

            return df.count()
            
    def get_top5_vehicle_maker_accidents_where_driver_died_airbag_not_deployed(self, output_path, output_format):
            """
            Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
            :param output_format: Write file format
            :param output_path: output file path
            :return: Dataframe of top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
            """
            unit_data = self.df_units
            primary_person_data = self.df_primary_person		
            crash_data = (
                primary_person_data.filter(
                    (col("PRSN_TYPE_ID") == "DRIVER") &
                    (col("DEATH_CNT") > 0) &
                    (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")
                )
                .join(unit_data, "CRASH_ID", "inner")
            )
            
            result = (
                crash_data.groupBy("VEH_MAKE_ID")
                .count()
                .orderBy(col("count").desc())
                .limit(5)
            )
            
            utils.write_output(result, output_path, output_format)

            return result
            

    def count_vehicle_with_licensed_driver_involved_in_hit_and_run(self, output_path, output_format):
            """
            Determine number of Vehicles with driver having valid licences involved in hit and run.
            :param output_format: Write file format
            :param output_path: output file path
            :return: Count of vehicles with driver having valid licences involved in hit and run
            """
            valid_licenses_data = self.df_endorsements.filter(self.df_endorsements.DRVR_LIC_ENDORS_ID != "UNLICENSED")
            unit_data = self.df_units
            primary_person_data = self.df_primary_person		
            result = (
                primary_person_data
                .filter((col("PRSN_TYPE_ID").contains("DRIVER")))
                .join(unit_data, "CRASH_ID", "inner")
                .join(valid_licenses_data, "CRASH_ID", "inner")
                .filter(col("VEH_HNR_FL") == "Y")
                .groupBy("CRASH_ID")
            )		
                
            utils.write_output(result, output_path, output_format)

            return result.count()
            

            
    def get_state_with_highest_nonfemale_accident(self, output_path, output_format):
            """
            Which state has highest number of accidents in which females are not involved.
            :param output_format: Write file format
            :param output_path: output file path
            :return: State name of highest non female accidents.
            """
            df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID != "FEMALE"). \
                groupby("DRVR_LIC_STATE_ID").count(). \
                orderBy(col("count").desc())
                
            utils.write_output(df.first().DRVR_LIC_STATE_ID, output_path, output_format)

            return df.first().DRVR_LIC_STATE_ID
            

    def get_top_vehiclemaker_contributing_to_injuries(self, output_path, output_format):
            """
            Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
            :param output_format: Write file format
            :param output_path: output file path
            :return: Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
            """
            df = self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA"). \
                withColumn('TOT_CASUALTIES_CNT', self.df_units.TOT_INJRY_CNT + self.df_units.DEATH_CNT). \
                groupby("VEH_MAKE_ID").sum("TOT_CASUALTIES_CNT"). \
                withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG"). \
                orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())

            df_top_3_to_5 = df.limit(5).subtract(df.limit(2))
            veh_make_ids = (veh[0] for veh in df_top_3_to_5.select("VEH_MAKE_ID"))
            result = spark.createDataFrame(veh_make_ids, ["VEH_MAKE_ID"])
            utils.write_output(result, output_path, output_format)
            
            return result
            

    def get_top_ethnic_usergroup_crash_for_each_body_style(self, output_path, output_format):
            """
            Find all the body styles involved in crashes, mention the top ethnic user group of each unique body style  
            :param output_format: Write file format
            :param output_path: output file path
            :return: None
            """
            df = self.df_units.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
                filter(~self.df_units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED"])). \
                filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])). \
                groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count())        

            window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("count"))
            ranked_data = df.withColumn("rank", row_number().over(window_spec)).filter(col("rank") == 1).drop("rank", "count")
            utils.write_output(ranked_data, output_path, output_format)
            return ranked_data
            
            
    def get_top_5_zip_codes_with_alcohols_as_cf_for_crash(self, output_path, output_format):
            """
            Finds top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
            :param output_format: Write file format
            :param output_path: output file path
            :return: List of Zip Codes
            """
            df = self.df_units.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
                dropna(subset=["DRVR_ZIP"]). \
                filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
                groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
            utils.write_output(df, output_path, output_format)

            return [row[0] for row in df.collect()]
            
            
    def count_crash_ids_with_no_damaged_property(self, output_path, output_format):
            """
            Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.
            :param output_format: Write file format
            :param output_path: output file path
            :return: Count of Distinct Crash IDs
            """
            df = self.df_damages.join(self.df_units, on=["CRASH_ID"], how='inner'). \
                filter(
                        (self.df_units.VEH_DMAG_SCL_1_ID.isin(["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"]))| 
                        (self.df_units.VEH_DMAG_SCL_2_ID.isin(["DAMAGED 5", "DAMAGED 6", "DAMAGED 7 HIGHEST"])) 
            ). \
                filter(self.df_damages.DAMAGED_PROPERTY.contains("NONE")). \
                filter(self.df_units.FIN_RESP_TYPE_ID.contains("PROOF OF LIABILITY INSURANCE|LIABILITY INSURANCE POLICY|CERTIFICATE OF SELF-INSURANCE|INSURANCE BINDER"))
            distinct_crash_ids_count = df.select("CRASH_ID").distinct().count()
            utils.write_output(df, output_path, output_format)

            return distinct_crash_ids_count
            
            
    def get_top_5_vehicle_makers_charged_overspeeding(self, output_path, output_format):
            """
            Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers,
            used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences
            :param output_format: Write file format
            :param output_path: output file path
            :return List of Vehicle brands
            """
            top_25_state_list = [row[0] for row in self.df_units.filter(col("VEH_LIC_STATE_ID").isNotNull()).
                groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
            top_10_used_vehicle_colors = [row[0] for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA").
                groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

            df = self.df_charges.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
                join(self.df_units, on=['CRASH_ID'], how='inner'). \
                filter(self.df_charges.CHARGE.contains("SPEED")). \
                filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
                filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)). \
                filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
                groupby("VEH_MAKE_ID").count(). \
                orderBy(col("count").desc()).limit(5)

            utils.write_output(df, output_path, output_format)

            return [row[0] for row in df.collect()]


if __name__ == '__main__':
    # Initialize sparks session
    spark = SparkSession \
        .builder \
        .appName("USVehicleAccidentAnalysis") \
        .getOrCreate()

    config_file_path = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    usvaa = USVehicleAccidentAnalysis(config_file_path)
    output_file_paths = utils.read_yaml(config_file_path).get("OUTPUT_PATH")
    file_format = utils.read_yaml(config_file_path).get("FILE_FORMAT")

    # 1. Find the number of crashes (accidents) in which number of males killed are greater than 2.
    print("1. Result:", usvaa.count_crashes_with_male_killed_gt_2(output_file_paths.get(1), file_format.get("Output")))

    # 2. Find how many two wheelers are booked for crashes.
    print("2. Result:", usvaa.count_2_wheeler_accidents(output_file_paths.get(2), file_format.get("Output")))

    # 3. Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
    print("3. Result:", usvaa.get_top5_vehicle_maker_accidents_where_driver_died_airbag_not_deployed(output_file_paths.get(3),
                                                                     file_format.get("Output")))

    # 4. Determine number of Vehicles with driver having valid licences involved in hit and run.
    print("4. Result:", usvaa.count_vehicle_with_licensed_driver_involved_in_hit_and_run(output_file_paths.get(4),
                                                                       file_format.get("Output")))

    # 5. Find which state has highest number of accidents in which females are not involved.
    print("5. Result:", usvaa.get_state_with_highest_nonfemale_accident(output_file_paths.get(5), file_format.get("Output")))

    # 6. Find the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print("6. Result:", usvaa.get_top_vehiclemaker_contributing_to_injuries(output_file_paths.get(6), file_format.get("Output")))
                                                                                
    # 7. Find all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("7. Result:", usvaa.get_top_ethnic_usergroup_crash_for_each_body_style(output_file_paths.get(7), file_format.get("Output")))

    # 8. Finds top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
    print("8. Result:", usvaa.get_top_5_zip_codes_with_alcohols_as_cf_for_crash(output_file_paths.get(8), file_format.get("Output")))                                                                                 

    # 9. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance.
    print("9. Result:", usvaa.count_crash_ids_with_no_damaged_property(output_file_paths.get(9), file_format.get("Output")))

    # 10.Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers,
    #    used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences
    print("10. Result:", usvaa.get_top_5_vehicle_makers_charged_overspeeding(output_file_paths.get(10), file_format.get("Output")))

    spark.stop()
