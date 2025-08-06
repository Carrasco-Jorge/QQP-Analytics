import pandas as pd
import unicodedata
import re

def clean_text(text: str):
    # Decouple letters and accents
    text = unicodedata.normalize("NFD", text)

    # Remove accents and kepp "Ñ"
    text = text.replace("Ñ", "<<ENYE_UPPER>>")
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.replace(">>ENYE_UPPER>>", "Ñ")
    
    # Keep letters, numbers, punctiation " " and 'ñ'
    allowed_pattern = r"[^a-zA-Z0-9ñÑ.,;:!?()'\-\" ]"
    text = re.sub(allowed_pattern, "", text)

    # Remove extra spaces
    text = re.sub(r"\s+", " ", text)

    return text


def clean_puebla_csv(puebla_csv_path: str) -> pd.DataFrame:
    columns=[
        "product", "presentation", "brand", "type", "category",
        "price", "date", "chain", "line_of_business", "branch",
        "address", "state", "municipality", "lat", "long"
    ]

    text_columns = [
        "product", "presentation", "brand", "type", "category",
        "chain", "line_of_business", "branch", "address",
        "state", "municipality"
    ]

    df = pd.read_csv(puebla_csv_path, names=columns)

    for text_col in text_columns:
        df[text_col] = df[text_col].str.upper()
        df[text_col] = df[text_col].apply(clean_text)

    print(df.shape)
    print(df.head())

    return df