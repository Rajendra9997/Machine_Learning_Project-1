from census.config.configuration import Configuration
from census.exception import CensusException
from census.logger import logging

from census.component.data_ingestion import DataIngestion
from census.entity.artifact_entity import DataIngestionArtifact
from census.entity.config_entity import DataIngestionConfig
import os,sys

class Pipeline:

    def __init__(self, config: Configuration = Configuration()) -> None:
        try:
            self.config = config
        except Exception as e:
            raise CensusException(e,sys) from e
        
    def start_data_ingestion(self) -> DataIngestionArtifact:
        try:
            data_ingestion = DataIngestion(data_ingestion_config=self.config.get_data_ingestion_config())
            return data_ingestion.initiate_data_ingestion()
        except Exception as e:
            raise CensusException(e,sys) from e

    def run_pipeline(self):
        try:
            #Data ingestion
            data_ingestion_artifact = self.start_data_ingestion()
        except Exception as e :
            raise CensusException(e,sys) from e