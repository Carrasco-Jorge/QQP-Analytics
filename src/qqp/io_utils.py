import os


def join_paths(path_list):
    final_path = ""
    for path in path_list:
        final_path = os.path.join(final_path, path)

    return final_path
