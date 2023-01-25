import yaml
from census.exception import CensusException

def read_yaml_file(file_path:str)->str:
    """
    Read a YAML file and returns the content as dictionary 
    file path: str
    """
    try:
        with open(file_path,"rb") as yaml_file:
            return yaml_file.safe_load(yaml_file)
    except Exception as e:
        raise CensusException(e,sys) from e