import rarfile
import os


def get_rar_file_names(rar_path: str):
    with rarfile.RarFile(rar_path) as rf:
        name_list = rf.namelist()
    return name_list


def extract_file_from_rar(rar_path: str, name: str, output_dir: str) -> None:
    with rarfile.RarFile(rar_path) as rf:
        if not name.endswith(os.sep):
            rf.extract(name, path=output_dir)
    return None
