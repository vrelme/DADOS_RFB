class SchemaMapper:

    def __init__(self, model):
        self.model = model
        self.valid_columns = set(model.__table__.columns.keys())

        # Mapeamentos conhecidos (RFB → DB)
        self.column_mapping = {
            "ente_federativo_responsavel": "ente_federativo"
        }

    def map_columns(self, df):
        # renomeia colunas conhecidas
        df = df.rename(columns=self.column_mapping)

        # remove colunas inválidas
        df = df[[c for c in df.columns if c in self.valid_columns]]

        return df