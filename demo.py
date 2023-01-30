from census.pipeline.pipeline import Pipeline
from census.logger import logging
from census.exception import CensusException
from census.config.configuration import Configuration

def main():
    try:
        #pipeline = Pipeline()
        #pipeline.run_pipeline()
        data_validation_config = Configuration().get_data_validation_config()
        print(data_validation_config)
    except Exception as e:
        logging.error(f"{e}")
        print(e)

        
if __name__=="__main__":
    main()