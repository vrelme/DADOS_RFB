import pandas as pd
import logging

logger = logging.getLogger(__name__)


class Validator:

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove espaços, trata strings e valores vazios
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f"Esperado DataFrame, recebido: {type(df)}")

        # Apenas colunas string
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .replace({"": None, "nan": None})
            )

        return df

    # =====================================================
    # CONVERSÃO SEGURA DE TIPOS
    # =====================================================
    def to_int(self, series):
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    def to_float(self, series):
        return (
            series.astype(str)
            .str.replace(",", ".", regex=False)
            .pipe(pd.to_numeric, errors="coerce")
        )

    # =====================================================
    # VALIDAÇÕES POR TIPO DE ARQUIVO
    # =====================================================
    def validate_empresa(self, df: pd.DataFrame) -> pd.DataFrame:

        df["capital_social"] = self.to_float(df["capital_social"])
        df["natureza_juridica"] = self.to_int(df["natureza_juridica"])
        df["qualificacao_responsavel"] = self.to_int(df["qualificacao_responsavel"])
        df["porte_empresa"] = self.to_int(df["porte_empresa"])

        # remover inválidos
        before = len(df)

        df = df[df["cnpj_basico"].notna()]

        after = len(df)
        self._log_drop("EMPRESA", before, after)

        return df

    def validate_estabelecimento(self, df: pd.DataFrame) -> pd.DataFrame:

        before = len(df)

        df = df[
            df["cnpj_basico"].notna() &
            df["cnpj_ordem"].notna() &
            df["cnpj_dv"].notna()
        ]

        after = len(df)
        self._log_drop("ESTABELECIMENTO", before, after)

        return df

    def validate_socio(self, df: pd.DataFrame) -> pd.DataFrame:

        before = len(df)

        df = df[df["cnpj_basico"].notna()]

        after = len(df)
        self._log_drop("SOCIO", before, after)

        return df

    # =====================================================
    # UTIL
    # =====================================================
    def _log_drop(self, tipo, before, after):
        dropped = before - after
        if dropped > 0:
            logger.warning(f"{tipo}: {dropped} registros inválidos removidos")