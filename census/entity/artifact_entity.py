from collections import namedtuple

DataIngestionArtifact = namedtuple("DataIngestionartifact", 
["train_file_path","test_file_path","is_ingested","message"] )