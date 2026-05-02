import pandas as pd


class DataTransformer:

    def sanitize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitização leve e segura:
        - limpa strings
        - remove espaços
        - normaliza valores vazios
        - NÃO faz conversão de tipos (isso é responsabilidade do Validator)
        """

        if df.empty:
            return df

        # =========================================
        # 1. LIMPEZA DE STRINGS
        # =========================================
        str_cols = df.select_dtypes(include=["object"]).columns

        if len(str_cols) > 0:
            df[str_cols] = df[str_cols].apply(
                lambda col: (
                    col.astype(str)
                    .str.strip()
                    .replace({
                        "": None,
                        "nan": None,
                        "None": None,
                        "NaN": None
                    })
                )
            )

        # =========================================
        # 2. NORMALIZAÇÃO DE TEXTO (OPCIONAL)
        # (evita inconsistência tipo "SP ", " sp", etc)
        # =========================================
        if "uf" in df.columns:
            df["uf"] = df["uf"].str.upper()

        # =========================================
        # 3. GARANTIR QUE NÃO EXISTE STRING "NULL"
        # =========================================
        df = df.replace({"NULL": None, "null": None})

        return df