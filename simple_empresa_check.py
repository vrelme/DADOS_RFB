#!/usr/bin/env python3
"""Simples verificação de dados na tabela empresa - versão básica."""

import sys
import mysql.connector
from app.config import Settings

def simple_check():
    """Verifica diferença entre empresa nos dois bancos."""
    try:
        # Conexão
        print("Conectando ao MySQL...")
        conn = mysql.connector.connect(
            host=Settings.DB_HOST,
            port=Settings.DB_PORT,
            user=Settings.DB_USER,
            password=Settings.DB_PASSWORD,
            database=Settings.IMPORT_DB_NAME
        )
        cursor = conn.cursor()
        
        # Contar tabela em rfb_import
        print(f"Consultando {Settings.IMPORT_DB_NAME}.empresa...")
        cursor.execute(f"SELECT COUNT(*) FROM `{Settings.IMPORT_DB_NAME}`.`empresa`")
        count_import = cursor.fetchone()[0]
        print(f"✓ {Settings.IMPORT_DB_NAME}.empresa: {count_import:,} registros")
        
        # Contar tabela em dados_rfb
        print(f"Consultando {Settings.DB_NAME}.empresa...")
        cursor.execute(f"SELECT COUNT(*) FROM `{Settings.DB_NAME}`.`empresa`")
        count_final = cursor.fetchone()[0]
        print(f"✓ {Settings.DB_NAME}.empresa: {count_final:,} registros")
        
        # Diferença
        diff = count_final - count_import
        print(f"\n📊 Diferença: {diff:+,} registros")
        
        if diff > 0:
            print(f"⚠️  Dados_rfb tem MAIS {diff:,} registros")
        elif diff < 0:
            print(f"⚠️  Dados_rfb tem MENOS {abs(diff):,} registros")
        else:
            print(f"✅ Dados IGUAIS")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    simple_check()
