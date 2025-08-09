import csv
from tqdm import tqdm
from io import StringIO
from qqp.config.settings import default_encoding, alternative_encoding, RAW_DATA_DIR
from qqp.os_utils import join_paths


def init_clean_csv(puebla_csv_path):
    with open(puebla_csv_path, "w", encoding=default_encoding) as new_file:
        pass
    return None


def decode_line(binary_line):
    line = ""
    try:
        line:str = binary_line.decode(default_encoding)
        if "TORTILLER" in line:
            print(binary_line)
            with open(join_paths([RAW_DATA_DIR, "TORTILLERIAS.txt"]), "ab") as file:
                file.write(binary_line)
    except ValueError:
        line = binary_line.decode(alternative_encoding)
    return line


def get_row(line: str):
    with StringIO(line) as csv_line:
        reader = csv.reader(csv_line, quotechar='"')
        reader = list(reader)
        if len(reader) != 1:
            raise Exception("More than 1 line in a line.")
        row = list(reader)[0]
    return row


def validate_row_length(row: list):
    row_not_valid = len(row) != 15
    if row_not_valid:
        raise Exception(f"line has different length as excpected length '15'. {len(row)}  |  {row}")
    return None


def save_row(puebla_csv_path: str, row: list):
    with open(puebla_csv_path, "a", encoding=default_encoding) as new_file:
        writer = csv.writer(new_file, delimiter=',', quotechar='"')
        writer.writerow(row)
    return None


def decode_puebla_csv(raw_csv_path: str, puebla_csv_path: str):
    init_clean_csv(puebla_csv_path)

    with open(join_paths([RAW_DATA_DIR, "TORTILLERIAS.txt"]), "wb") as file:
        pass

    with open(raw_csv_path, "rb") as file:
        for binary_line in tqdm(file.readlines()):
            line = decode_line(binary_line)
            row = get_row(line)
            validate_row_length(row)
            
            # Only save rows from 'PUEBLA'
            if row[11] == "PUEBLA" and row[12] == "PUEBLA":
                save_row(puebla_csv_path, row)

    return None
