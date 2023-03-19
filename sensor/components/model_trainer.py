from sensor.utils.main_utils import load_numpy_array_data
from sensor.logger import logging
from sensor.exception import SensorException
from sensor.entity.config_entity import ModelTrainerConfig
from sensor.entity.artifact_entity import DataTransformationArtifact,ModelTrainerArtifact
import os,sys
from xgboost import XGBClassifier
from sensor.ml.metric.classsification_metric import get_classification_metrics
from sensor.ml.model.estimator import SensorModel
from sensor.utils.main_utils import save_object,load_object

class ModelTrainer:

    def __init__(self,data_transformation_artifact: DataTransformationArtifact,model_trainer_config: ModelTrainerConfig):
        
        try:
            self.data_transformation_artifact = data_transformation_artifact
            self.model_trainer_config = model_trainer_config
        except Exception as e:
            raise SensorException(e,sys)
        
    def perform_hyper_parameter_tuning(self):
        pass
    
    def train_model(self,x_train,y_train):
        try:
            xgb_cls = XGBClassifier()
            xgb_cls.fit(x_train,y_train)
            return xgb_cls
        except Exception as e:
            raise SensorException(e,sys)    
        
    def initiate_model_trainer(self)->ModelTrainerArtifact:
        try:
            train_file_path = self.data_transformation_artifact.transformed_train_file_path
            test_file_path = self.data_transformation_artifact.transformed_test_file_path

            train_arr = load_numpy_array_data(train_file_path)
            test_arr = load_numpy_array_data(test_file_path)

            x_train,y_train,x_test,y_test = (
                train_arr[:, :-1],
                train_arr[:, -1],
                test_arr[:, :-1],
                test_arr[:, -1]
            )
            model = self.train_model(x_train,y_train)
            y_train_pred = model.predict(x_train)
            classification_train_metric = get_classification_metrics(y_train,y_train_pred)
            
            if classification_train_metric.f1_score<self.model_trainer_config.expected_accuracy:
                raise Exception("low f1_score try experimenting more")

            y_test_pred = model.predict(x_test)
            classification_test_metric = get_classification_metrics(y_test,y_test_pred)

            #overfitting and underfitting
            diff = abs(classification_train_metric.f1_score-classification_test_metric.f1_score)
            if diff>self.model_trainer_config.overfit_underfit_threshold:
                raise Exception("model ios not good try with different experimentation ")

            preprocessor = load_object(self.data_transformation_artifact.transformed_object_file_path)
            sensor_model = SensorModel(preprocessor=preprocessor,model=model)
            #model_dir_path = os.path.dirname(self.model_trainer_config.trained_model_file_path)
            #os.makedirs(model_dir_path, exist_ok=True)
            save_object(self.model_trainer_config.trained_model_file_path,sensor_model)
            
            #model trainer artifact
            model_trainer_artifact = ModelTrainerArtifact(trained_model_file_path=self.model_trainer_config.trained_model_file_path,
                train_metric_artifact = classification_train_metric,
                test_metric_artifact =classification_test_metric)
            
            logging.info(f"model trainer artifact: {model_trainer_artifact}")
            return model_trainer_artifact            
        except Exception as e:
            raise SensorException(e,sys)
        

  