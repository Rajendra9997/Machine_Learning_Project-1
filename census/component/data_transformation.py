from census.logger import logging
from census.exception import CensusException
from census.entity.config_entity import DataTransformationConfig
from census.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact, \
    DataTransformationArtifact
from census.util.util import read_yaml_file,save_object,save_numpy_array_data,load_data
from census.constant import *
import sys, os
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer


class DataTransformation:

    def __init__(self, data_transformation_config : DataTransformationConfig,
                data_ingestion_artifact : DataIngestionArtifact,
                data_validation_artifact : DataValidationArtifact    
                ):
        try:
            logging.info(f"{'='*20} Data transformation log started.{'='*20}")
            self.data_transformation_config = data_transformation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_artifact = data_validation_artifact
     
        except Exception as e:
            raise CensusException(e,sys) from e

    
    def get_data_transformer_object(self) -> ColumnTransformer:
        try:
            schema_file_path = r"E:\Machine_Learning_Project\config\schema.yaml"
            
            dataset_schema = read_yaml_file(file_path=schema_file_path)

            numerical_columns = dataset_schema[NUMERICAL_COLUMN_KEY]
            categorical_columns = dataset_schema[CATEGORICAL_COLUMN_KEY]
            
            num_pipeline = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy="median")),
                ('scaler', StandardScaler())
            ])

            cat_pipeline = Pipeline(steps=[
                ('imputer', SimpleImputer(strategy="most_frequent")),
                ('onehotencoder', OneHotEncoder()),
                ('scaler', StandardScaler(with_mean=False))
            ])

            logging.info(f"Categorical columns : {categorical_columns}")
            logging.info(f"Numerical_columns : {numerical_columns}")

            preprocessing = ColumnTransformer([
                ('num_pipeline', num_pipeline, numerical_columns),
                ('cat_pipeline', cat_pipeline, categorical_columns),
            ])

            return preprocessing
        except Exception as e:
            raise CensusException(e,sys) from e

    def initiate_data_transformation(self) -> DataTransformationArtifact:
        try:
            logging.info("Obtaining preprocessing object.")
            preprocessing_obj = self.get_data_transformer_object()

            logging.info("Obtaining train and test file path")
            train_file_path = self.data_ingestion_artifact.train_file_path
            test_file_path = self.data_ingestion_artifact.train_file_path

            schema_file_path = r"E:\Machine_Learning_Project\config\schema.yaml"

            logging.info("Loading train and test data as pandas dataframe.")
            train_df = load_data(data_file_path = train_file_path, schema_file_path=schema_file_path)
            
            test_df = load_data(data_file_path = test_file_path, schema_file_path=schema_file_path)

            schema = read_yaml_file(file_path = schema_file_path)

            target_column_name = schema[TARGET_COLUMN_KEY]

            logging.info("Splitting input and target features from training and testing dataframes.")   
            input_feature_train_df = train_df.drop(columns = [target_column_name], axis = 1).replace("?", np.nan)
            target_feature_train_df = train_df[target_column_name]

            input_feature_test_df = test_df.drop(columns = [target_column_name], axis = 1).replace("?", np.nan)
            target_feature_test_df = test_df[target_column_name]
            
            logging.info(f"Applying preprocessing object on training and testing dataframe.")
            input_feature_train_arr = preprocessing_obj.fit_transform(input_feature_train_df)
            input_feature_test_arr = preprocessing_obj.transform(input_feature_test_df)

            
            train_arr = np.c_[ input_feature_train_arr, [np.array(target_feature_train_df)]]

            test_arr = np.c_[input_feature_test_arr, [np.array(target_feature_test_df)]]

            transformed_train_dir = self.data_transformation_config.transformed_train_dir
            transformed_test_dir = self.data_transformation_config.transformed_test_dir

            train_file_name = os.path.basename(train_file_path).replace(".csv",".npz")
            test_file_name = os.path.basename(test_file_path).replace(".csv",".npz")

            transformed_train_file_path = os.path.join(transformed_train_dir, train_file_name)
            transformed_test_file_path = os.path.join(transformed_test_dir, test_file_name)

            logging.info("Saving transformed training and testing array.")
            save_numpy_array_data(file_path=transformed_train_file_path,array=train_arr)    
            save_numpy_array_data(file_path=transformed_test_file_path, array=test_arr)
  
            preprocessing_obj_file_path = self.data_transformation_config.preprocessed_object_file_path


            logging.info("Saving preprocessing object.")
            save_object(file_path=preprocessing_obj_file_path, obj=preprocessing_obj)

            data_transformation_artifact = DataTransformationArtifact(
                is_transformed=True,
                message="Data_transformation successful.",
                transformed_train_file_path=transformed_train_file_path,
                transformed_test_file_path=transformed_test_file_path,
                preprocessed_object_file_path=preprocessing_obj_file_path
            )
            logging.info(f"Data transformation artifact: {data_transformation_artifact}")
            return data_transformation_artifact
        except Exception as e:
            raise CensusException(e, sys) from e

    def __del__(self):
        logging.info(f"{'='*20} transformation log completed {'='*20} \n\n")
        