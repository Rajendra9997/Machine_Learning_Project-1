from census.pipeline.pipeline import Pipeline
from census.logger import logging
from census.exception import CensusException
def main():
    try:
        pipeline = Pipeline
        pipeline.run_pipeline()
    except Exception as e:
        logging.error(f"{e}")
        print(e)

        
if __name__=="__main__":
    main()