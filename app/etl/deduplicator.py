class Deduplicator:

    def drop_duplicates(self, df, keys: list):
        before = len(df)

        df = df.drop_duplicates(subset=keys, keep="last")

        after = len(df)

        if before != after:
            print(f"Deduplicados removidos: {before - after}")

        return df