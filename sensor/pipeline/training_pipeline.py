from sensor.entity.config_entity import (DataIngestionConfig,
                                        TrainingPipelineConfig,
                                        DataValidationConfig,
                                        DataTransformationConfig,
                                        ModelTrainerConfig,
                                        ModelEvaluationConfig,
                                        ModelPusherConfig)
from sensor.entity.artifact_entity import (DataIngestionArtifact,
                                           DataValidationArtifact,
                                           DataTransformationArtifact,
                                           ModelTrainerArtifact,
                                           ModelEvaluationArtifact,
                                           ModelPusherArtifact)
from sensor.components.data_ingestion import DataIngestion
from sensor.components.data_validation import Datavalidataion
from sensor.components.data_transformation import DataTransformation
from sensor.components.model_trainer import ModelTrainer
from sensor.components.model_evaluation import ModelEvaluation
from sensor.components.model_pusher import ModelPusher
from sensor.exception import SensorException
from sensor.logger import logging
import sys

class TrainPipeline:

    is_pipeline_running = False
    def __init__(self):
        training_pipeline_config = TrainingPipelineConfig()
        self.training_pipeline_config = training_pipeline_config

    def start_data_ingestion(self)->DataIngestionArtifact:
        try:
            self.data_ingestion_config = DataIngestionConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info("starting data ingestion")
            data_ingestion = DataIngestion(data_ingestion_config=self.data_ingestion_config)
            data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
            logging.info("data ingestion completed")
            return data_ingestion_artifact
        except Exception as e:
            raise SensorException(e,sys)
        
    def start_data_validation(self,data_ingestion_artifact: DataIngestionArtifact)->DataValidationArtifact:
        try:
            self.data_validation_config = DataValidationConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info("starting data validation")
            data_validation = Datavalidataion(data_ingetsion_artifact=data_ingestion_artifact,
                                              data_validation_config=self.data_validation_config)
            data_validation_artifact = data_validation.initiate_data_validation()
            logging.info("data validation completed")
            return data_validation_artifact
        except Exception as e:
            raise SensorException(e,sys)    
    
    def start_data_transformation(self,data_validation_artifact: DataValidationArtifact)->DataTransformationArtifact:
        try:
            self.data_transformation_config = DataTransformationConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info("starting data transformation")
            data_transformation = DataTransformation(data_validation_artifact=data_validation_artifact,
                                                     data_tranformation_config=self.data_transformation_config)
            data_transformation_artifact = data_transformation.initiate_data_transformation()
            logging.info("data transformation completed")
            return data_transformation_artifact
        except Exception as e:
            raise SensorException(e,sys)
        
    def start_model_trainer(self,data_transformation_artifact: DataTransformationArtifact)->ModelTrainerArtifact:
        try:
            self.model_trainer_config = ModelTrainerConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info("starting model training")
            model_trainer = ModelTrainer(data_transformation_artifact=data_transformation_artifact,
                                         model_trainer_config=self.model_trainer_config)
            model_trainer_artifact = model_trainer.initiate_model_trainer()
            logging.info("model training completed")
            return model_trainer_artifact
        except Exception as e:
            raise SensorException(e,sys)
        
    def start_model_evaluation(self,model_trainer_artifact: ModelTrainerArtifact,
                                    data_validation_artifact: DataValidationArtifact)->ModelTrainerArtifact:
        try:
            self.model_evaluation_config = ModelEvaluationConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info("starting model evaluation")
            model_evaluation = ModelEvaluation(model_evaluation_config = self.model_evaluation_config,
                                               data_validation_artifact = data_validation_artifact,
                                               model_trainer_artifact = model_trainer_artifact)
            model_evaluation_artifact = model_evaluation.initiate_model_evaluation()
            logging.info("model evaluation completed")
            return model_evaluation_artifact
        except Exception as e:
            raise SensorException(e,sys)
        
    def start_model_pusher(self,model_evaluation_artifact: ModelEvaluationArtifact):
        try:
            self.model_pusher_config = ModelPusherConfig(training_pipeline_config=self.training_pipeline_config)
            logging.info("starting model pusher")
            model_pusher = ModelPusher(model_pusher_config=self.model_pusher_config,
                                       model_evaluation_artifact=model_evaluation_artifact)
            model_pusher_artifact =  model_pusher.initiate_model_pusher()
            logging.info("model pusher completed")
            return model_pusher_artifact
        except Exception as e:
            raise SensorException(e,sys)
        
    def run_pipeline(self):
        try:
            TrainPipeline.is_pipeline_running = True
            data_ingestion_artifact = self.start_data_ingestion()
            data_validation_artifact  = self.start_data_validation(data_ingestion_artifact=data_ingestion_artifact)
            data_transformation_artifact = self.start_data_transformation(data_validation_artifact=data_validation_artifact)
            model_trainer_artifact = self.start_model_trainer(data_transformation_artifact=data_transformation_artifact)
            model_evaluation_artifact = self.start_model_evaluation(data_validation_artifact=data_validation_artifact,model_trainer_artifact=model_trainer_artifact)
            if not model_evaluation_artifact.is_model_accepted:
                raise Exception("model not accepted :")
            model_pusher_artifact = self.start_model_pusher(model_evaluation_artifact=model_evaluation_artifact)
            TrainPipeline.is_pipeline_running = False
                
        except Exception as e:
            TrainPipeline.is_pipeline_running = False
            raise SensorException(e,sys)