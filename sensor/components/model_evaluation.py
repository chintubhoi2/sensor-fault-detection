from sensor.exception import SensorException
from sensor.logger import logging
from sensor.entity.artifact_entity import ModelTrainerArtifact,DataValidationArtifact,ModelEvaluationArtifact
from sensor.entity.config_entity import ModelEvaluationConfig
import os,sys
from sensor.ml.metric.classsification_metric import get_classification_metrics
from sensor.ml.model.estimator import SensorModel
from sensor.utils.main_utils import save_object,load_object,write_yaml_file
from sensor.ml.model.estimator import ModelResolver,TargetValueMapping
from sensor.constant.training_pipeline import TARGET_COLUMN
import pandas as pd

class ModelEvaluation:

    def __init__(self,model_evaluation_config: ModelEvaluationConfig,
        data_validation_artifact: DataValidationArtifact,
        model_trainer_artifact: ModelTrainerArtifact):

        try:
            self.model_evaluation_config = model_evaluation_config
            self.data_validation_artifact = data_validation_artifact
            self.model_trainer_artifact = model_trainer_artifact
        except Exception as e:
            raise SensorException(e,sys)
        
    def initiate_model_evaluation(self)->ModelEvaluationArtifact:
        try:
            valid_train_file_path = self.data_validation_artifact.valid_train_file_path
            valid_test_file_path = self.data_validation_artifact.valid_test_file_path

            #valid train and test file dataframe
            train_df = pd.read_csv(valid_train_file_path)
            test_df = pd.read_csv(valid_test_file_path)
            df = pd.concat([train_df,test_df])
            y_true = df[TARGET_COLUMN]
            y_true.replace(TargetValueMapping().to_dict(),inplace=True)
            df.drop(TARGET_COLUMN,axis=1,inplace=True)

            trained_model_file_path = self.model_trainer_artifact.trained_model_file_path
            is_model_accepted = True
            model_resolver = ModelResolver()
            if not model_resolver.is_model_exist():
                model_eval_artifact = ModelEvaluationArtifact(
                    is_model_accepted = is_model_accepted,
                    improved_accuracy = None,
                    best_model_path = None,
                    trained_model_path= trained_model_file_path,
                    train_model_metric_artifact = self.model_trainer_artifact.test_metric_artifact,
                    best_model_metric_artifact = None
                )
                logging.info(f"Model Evaluation Artifact : {model_eval_artifact}")
                return model_eval_artifact

            latest_model_file_path = model_resolver.get_best_model_path()
            latest_model = load_object(latest_model_file_path)
            trained_model = load_object(file_path=trained_model_file_path)
            
            y_train_pred = trained_model.predict(df)
            y_latest_pred = latest_model.predict(df)

            trained_metric = get_classification_metrics(y_true,y_train_pred)
            latest_metric = get_classification_metrics(y_true,y_latest_pred)

            score_diff = trained_metric.f1_score - latest_metric.f1_score
            if score_diff > self.model_evaluation_config.change_threshold:
                is_model_accepted = True
            else:
                is_model_accepted = False

            model_eval_artifact = ModelEvaluationArtifact(
                is_model_accepted = is_model_accepted,
                improved_accuracy = score_diff,
                best_model_path = latest_model_file_path,
                trained_model_path = trained_model_file_path,
                train_model_metric_artifact = trained_metric,
                best_model_metric_artifact = latest_metric
                )

            model_eval_report = model_eval_artifact.__dict__
            write_yaml_file(self.model_evaluation_config.report_file_path,model_eval_report)
            logging.info(f"model eval artifact : {model_eval_artifact}")
            return model_eval_artifact
        except Exception as e:
            raise SensorException(e,sys)
        