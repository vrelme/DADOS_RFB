import pandas as pd
import numpy as np


class DataTransformer:

    def sanitize(self, df: pd.DataFrame) -> pd.DataFrame:

        # =========================
        # CAPITAL SOCIAL
        # =========================
        if "capital_social" in df.columns:
            df["capital_social"] = (
                df["capital_social"]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )

            df["capital_social"] = pd.to_numeric(
                df["capital_social"], errors="coerce"
            )

        # =========================
        # CAMPOS NUMÉRICOS
        # =========================
        int_cols = [
            "natureza_juridica",
            "qualificacao_responsavel",
            "porte_empresa"
        ]

        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # =========================
        #  CORREÇÃO CRÍTICA
        # =========================
        # Converter NaN → None (compatível com MySQL)
        df = df.replace({np.nan: None})

        return df