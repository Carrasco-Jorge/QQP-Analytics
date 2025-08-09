import csv
from qqp.config import paths


def save_processed_file(files_dict: dict, file_name):
    with open(paths.processed_file_path, "w") as file:
        writer = csv.writer(file, delimiter=",", quotechar='"')
        writer.writerows([["id","name"],[files_dict["max_id"]+1, file_name]])
    
    files_dict["files"].append(file_name)
    files_dict["max_id"] += 1
    
    return None