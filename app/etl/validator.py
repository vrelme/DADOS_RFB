import pandas as pd
import logging

logger = logging.getLogger(__name__)


class Validator:

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:

        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .replace({
                    "": None,
                    "nan": None,
                    "None": None,
                    "NaN": None
                })
            )

        return df

    # ===============================
    # CONVERSÕES SEGURAS
    # ===============================
    def to_int(self, series):
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    def to_float(self, series):
        return series.apply(
            lambda x: (
                float(str(x).replace(".", "").replace(",", "."))
                if pd.notna(x)
                else None
            )
        )

    # ===============================
    # EMPRESA
    # ===============================
    def validate_empresa(self, df: pd.DataFrame) -> pd.DataFrame:

        if "capital_social" in df.columns:
            df["capital_social"] = self.to_float(df["capital_social"])

        if "natureza_juridica" in df.columns:
            df["natureza_juridica"] = self.to_int(df["natureza_juridica"])

        if "qualificacao_responsavel" in df.columns:
            df["qualificacao_responsavel"] = self.to_int(df["qualificacao_responsavel"])

        if "porte_empresa" in df.columns:
            df["porte_empresa"] = self.to_int(df["porte_empresa"])

        before = len(df)

        df = df[
            df["cnpj_basico"].notna() &
            (df["cnpj_basico"].str.len() == 8)
        ]

        after = len(df)
        self._log_drop("EMPRESA", before, after)

        return df

    # ===============================
    # ESTABELECIMENTO
    # ===============================
    def validate_estabelecimento(self, df: pd.DataFrame) -> pd.DataFrame:

        before = len(df)

        # evita erro com NaN antes do .str
        df["cnpj_basico"] = df["cnpj_basico"].astype(str)
        df["cnpj_ordem"] = df["cnpj_ordem"].astype(str)
        df["cnpj_dv"] = df["cnpj_dv"].astype(str)

        df = df[
            df["cnpj_basico"].notna() &
            df["cnpj_ordem"].notna() &
            df["cnpj_dv"].notna() &
            (df["cnpj_basico"].str.len() == 8) &
            (df["cnpj_ordem"].str.len() == 4) &
            (df["cnpj_dv"].str.len() == 2)
        ]

        after = len(df)
        self._log_drop("ESTABELECIMENTO", before, after)

        return df

    # ===============================
    # SOCIO
    # ===============================
    def validate_socio(self, df: pd.DataFrame) -> pd.DataFrame:

        before = len(df)

        df = df[
            df["cnpj_basico"].notna() &
            (df["cnpj_basico"].str.len() == 8)
        ]

        after = len(df)
        self._log_drop("SOCIO", before, after)

        return df

    # ===============================
    # UTIL
    # ===============================
    def _log_drop(self, tipo, before, after):
        dropped = before - after
        if dropped > 0:
            logger.warning(f"{tipo}: {dropped} registros inválidos removidos")