import pandas as pd


class DataTransformer:

    def sanitize(self, df: pd.DataFrame) -> pd.DataFrame:

        # capital_social: "1.234,56" → 1234.56
        if "capital_social" in df.columns:
            df["capital_social"] = (
                df["capital_social"]
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
                .astype(float)
            )

        # inteiros
        int_cols = [
            "natureza_juridica",
            "qualificacao_responsavel",
            "porte_empresa"
        ]

        for col in int_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        return df