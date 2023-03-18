from sensor.constant.training_pipeline import SCHEMA_FILE_PATH
from sensor.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from sensor.entity.config_entity import DataValidationConfig
from sensor.logger import logging
from sensor.exception import SensorException
from sensor.utils.main_utils import read_yaml_file,write_yaml_file
from scipy.stats import ks_2samp
import pandas as pd
import os,sys

class Datavalidataion:

    def __init__(self,data_ingetsion_artifact:DataIngestionArtifact,
    data_validation_config:DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingetsion_artifact
            self.data_validation_config = data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise SensorException(e,sys)

    def validate_no_of_columns(self, dataframe: pd.DataFrame)->bool:
        try:
            number_of_columns = len(self._schema_config["columns"])
            if len(dataframe.columns) == number_of_columns:
                return True
            return False            
        except Exception as e:
            raise SensorException(e,sys)
    
    def is_numerical_column_exist(self, dataframe: pd.DataFrame)->bool:
        try:
            numerical_columns = self._schema_config["numerical_columns"]
            dataframe_columns = list(dataframe.columns)
            missing_numerical_columns=[]
            all_numerical_column_exits =True
            for column in numerical_columns:
                if column not in dataframe_columns:
                    all_numerical_column_exits = False
                    missing_numerical_columns.append(column)
            if len(missing_numerical_columns)>0:
                logging.info(f"missing numerical columns {missing_numerical_columns}")
            else:
                logging.info("all the required columns exist")
            return all_numerical_column_exits
        except Exception as e:
            raise SensorException(e,sys)


    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise SensorException(e,sys)

    def detect_drift_report(self,base_df,current_df,theshold=0.5)->bool:
        try:
            report={}
            for column in base_df:
                d1=base_df[column]
                d2=current_df[column]
                is_same_dist = ks_2samp(d1,d2)
                status = False
                if theshold <= is_same_dist.pvalue:
                    is_found = True
                    status = True
                else:
                    is_found = False
                report.update({column : {
                    "p value":float(is_same_dist.pvalue),
                    "drift_status":is_found 
                }})
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            dir_path = os.path.dirname(drift_report_file_path)
            os.makedirs(drift_report_file_path,exist_ok=True)
            write_yaml_file(drift_report_file_path,report)

            return status
        except Exception as e:
            raise SensorException(e,sys)
    
    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            error_message=""
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path
            train_dataframe = Datavalidataion.read_data(train_file_path)
            test_dataframe = Datavalidataion.read_data(test_file_path)
            
            status = self.validate_no_of_columns(train_dataframe)
            if not status:
                error_message=f"{error_message} no of columns didn't match for train data"
            status = self.validate_no_of_columns(test_dataframe)
            if not status:
                error_message=f"{error_message} no of columns didn't match for test data"
            
            #validate numerical columns
            status = self.is_numerical_column_exist(train_dataframe)
            if not status:
                error_message=f"{error_message} all numerical columns doesn't exist for train data"
            status = self.is_numerical_column_exist(test_dataframe)
            if not status:
                error_message=f"{error_message} all numerical columns doesn't exist for test data"

            if len(error_message)>0:
                raise Exception(error_message)
            else:
                valid_data_file_path = self.data_validation_config.valid_train_file_path
                dir_path = os.path.dirname(valid_data_file_path)
                os.makedirs(dir_path,exist_ok=True)
                train_dataframe.to_csv(self.data_validation_config.valid_train_file_path,index=False,header=True)
                test_dataframe.to_csv(self.data_validation_config.valid_test_file_path,index=False,header=True)
            
            status = self.detect_drift_report(base_df=train_dataframe,current_df=test_dataframe)


            data_validation_artifact = DataValidationArtifact(
                validation_status = status,
                valid_train_file_path = self.data_validation_config.valid_train_file_path,
                valid_test_file_path = self.data_validation_config.valid_test_file_path,
                invalid_train_file_path = self.data_validation_config.invalid_train_file_path,
                invalid_test_file_path = self.data_validation_config.invalid_test_file_path,
                drift_report_file_path = self.data_validation_config.drift_report_file_path
            )
            return data_validation_artifact
        
        except Exception as e:
            raise Exception(e,sys)