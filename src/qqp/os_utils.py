import os


def join_paths(path_list):
    final_path = ""
    for path in path_list:
        final_path = os.path.join(final_path, path)

    return final_path


def remove_csv_file(file_path):
    if os.path.isfile(file_path) and file_path.endswith(".csv"):
        os.remove(file_path)
    else:
        raise ValueError(f"Expected csv file path. 'file_path': {file_path}")


def remove_csvs_from_dir(dir_path: str):
    if not os.path.isdir(dir_path):
        raise ValueError(f"Provided path is not a valid directory: {dir_path}")
    
    for file_name in os.listdir(dir_path):
        file_path = join_paths([dir_path, file_name])
        if os.path.isfile(file_path) and file_name.endswith(".csv"):
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Error deleting file {file_path}: {e}")
    return None