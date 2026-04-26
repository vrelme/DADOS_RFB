import pandas as pd
import numpy as np
from datetime import datetime


class Sanitizer:

    # ==========================================
    # TEXTO
    # ==========================================

    @staticmethod
    def text(value):

        if value is None:
            return None

        value = str(value).strip()

        if value in ("", "NULL", "null", "None", "nan", "NaN"):
            return None

        return value

    # ==========================================
    # INTEIRO
    # ==========================================

    @staticmethod
    def integer(value):

        value = Sanitizer.text(value)

        if value is None:
            return None

        try:
            return int(value)
        except:
            return None

    # ==========================================
    # DECIMAL
    # ==========================================

    @staticmethod
    def decimal(value):

        value = Sanitizer.text(value)

        if value is None:
            return None

        value = value.replace(".", "")
        value = value.replace(",", ".")

        try:
            return float(value)
        except:
            return None

    # ==========================================
    # DATA YYYYMMDD
    # ==========================================

    @staticmethod
    def date(value):

        value = Sanitizer.text(value)

        if value is None:
            return None

        try:
            return datetime.strptime(value, "%Y%m%d").date()
        except:
            return None

    # ==========================================
    # DATAFRAME
    # ==========================================

    @staticmethod
    def dataframe(df, schema):

        """
        schema exemplo:

        {
            "codigo": "int",
            "nome": "text",
            "capital_social": "decimal",
            "data_inicio": "date"
        }
        """

        for coluna, tipo in schema.items():

            if coluna not in df.columns:
                continue

            if tipo == "text":
                df[coluna] = df[coluna].apply(Sanitizer.text)

            elif tipo == "int":
                df[coluna] = df[coluna].apply(Sanitizer.integer)

            elif tipo == "decimal":
                df[coluna] = df[coluna].apply(Sanitizer.decimal)

            elif tipo == "date":
                df[coluna] = df[coluna].apply(Sanitizer.date)

        df = df.replace({np.nan: None})

        return df