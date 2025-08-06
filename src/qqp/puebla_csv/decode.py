import csv
from tqdm import tqdm
from io import StringIO


def init_clean_csv(puebla_csv_path):
    with open(puebla_csv_path, "w", encoding="utf-8") as new_file:
        pass
    return None


def decode_line(binary_line):
    line = ""
    try:
        line = binary_line.decode("utf-8")
    except ValueError:
        line = binary_line.decode("cp850")
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
    with open(puebla_csv_path, "a", encoding="utf-8") as new_file:
        writer = csv.writer(new_file, delimiter=',', quotechar='"')
        writer.writerow(row)
    return None


def decode_puebla_csv(raw_csv_path: str, puebla_csv_path: str):
    init_clean_csv(puebla_csv_path)

    with open(raw_csv_path, "rb") as file:
        for binary_line in tqdm(file.readlines()):
            line = decode_line(binary_line)
            row = get_row(line)
            validate_row_length(row)
            
            # Only save rows from 'PUEBLA'
            if row[11] == "PUEBLA" and row[12] == "PUEBLA":
                save_row(puebla_csv_path, row)

    return None
