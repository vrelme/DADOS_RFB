#!/usr/bin/env python3
"""Verifica discrepâncias na tabela empresa entre rfb_import e dados_rfb."""

from sqlalchemy import create_engine, text
from app.database import build_server_url
from app.config import Settings

def get_row_count(engine, db_name, table_name):
    """Conta registros em uma tabela."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM `{db_name}`.`{table_name}`"))
            row = result.fetchone()
            return row.cnt if row else 0
    except Exception as e:
        print(f"❌ Erro ao contar {db_name}.{table_name}: {e}")
        return None

def get_column_info(engine, db_name, table_name):
    """Obtém informações sobre as colunas de uma tabela."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SHOW COLUMNS FROM `{db_name}`.`{table_name}`"))
            rows = result.fetchall()
            return rows
    except Exception as e:
        print(f"❌ Erro ao obter estrutura de {db_name}.{table_name}: {e}")
        return None

def compare_checksums(engine, db_name1, db_name2, table_name):
    """Compara checksums das tabelas para detectar diferenças."""
    try:
        with engine.connect() as conn:
            # Para MySQL MyISAM
            result1 = conn.execute(text(f"CHECKSUM TABLE `{db_name1}`.`{table_name}`"))
            checksum1 = result1.fetchone()
            
            result2 = conn.execute(text(f"CHECKSUM TABLE `{db_name2}`.`{table_name}`"))
            checksum2 = result2.fetchone()
            
            return checksum1, checksum2
    except Exception as e:
        print(f"❌ Erro ao calcular checksums: {e}")
        return None, None

def main():
    """Main."""
    print("\n" + "="*107)
    print("🔍 COMPARAÇÃO DE DADOS: rfb_import vs dados_rfb")
    print("="*107 + "\n")
    
    # Setup
    db_name_import = Settings.IMPORT_DB_NAME  # rfb_import
    db_name_final = Settings.DB_NAME          # rfb_loader ou dados_rfb
    
    print(f"📊 Banco de Importação: {db_name_import}")
    print(f"📊 Banco de Produção:   {db_name_final}")
    print()
    
    # Criar engine para conexão ao servidor (sem db específico)
    server_url = build_server_url()
    engine = create_engine(server_url)
    
    # Contar registros
    print("📈 CONTAGEM DE REGISTROS NA TABELA 'empresa':")
    print("-" * 107)
    
    count_import = get_row_count(engine, db_name_import, "empresa")
    count_final = get_row_count(engine, db_name_final, "empresa")
    
    if count_import is not None:
        print(f"  {db_name_import}.empresa: {count_import:,} registros")
    if count_final is not None:
        print(f"  {db_name_final}.empresa:  {count_final:,} registros")
    
    if count_import is not None and count_final is not None:
        diff = count_final - count_import
        if diff == 0:
            print(f"  ✅ Counts IGUAIS")
        else:
            print(f"  ⚠️  DIFERENÇA: {diff:+,} registros")
    print()
    
    # Estrutura das tabelas
    print("🏗️  ESTRUTURA DAS TABELAS:")
    print("-" * 107)
    
    cols_import = get_column_info(engine, db_name_import, "empresa")
    cols_final = get_column_info(engine, db_name_final, "empresa")
    
    if cols_import:
        print(f"\n{db_name_import}.empresa ({len(cols_import)} colunas):")
        for col in cols_import[:5]:
            print(f"  - {col[0]}: {col[1]}")
        if len(cols_import) > 5:
            print(f"  ... e mais {len(cols_import) - 5} colunas")
    
    if cols_final:
        print(f"\n{db_name_final}.empresa ({len(cols_final)} colunas):")
        for col in cols_final[:5]:
            print(f"  - {col[0]}: {col[1]}")
        if len(cols_final) > 5:
            print(f"  ... e mais {len(cols_final) - 5} colunas")
    
    if cols_import and cols_final:
        if len(cols_import) != len(cols_final):
            print(f"\n⚠️  ESTRUTURA DIFERENTE: {len(cols_import)} vs {len(cols_final)} colunas")
        else:
            print(f"\n✅ Mesma quantidade de colunas")
    print()
    
    # Comparação de alguns registros
    print("📋 AMOSTRA DE DADOS (primeiros 3 registros):")
    print("-" * 107)
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT * FROM `{db_name_import}`.`empresa` LIMIT 3"))
            rows = result.fetchall()
            if rows:
                print(f"\n{db_name_import}.empresa:")
                for i, row in enumerate(rows, 1):
                    print(f"  Registro {i}: {dict(row)}")
            
            result = conn.execute(text(f"SELECT * FROM `{db_name_final}`.`empresa` LIMIT 3"))
            rows = result.fetchall()
            if rows:
                print(f"\n{db_name_final}.empresa:")
                for i, row in enumerate(rows, 1):
                    print(f"  Registro {i}: {dict(row)}")
    except Exception as e:
        print(f"❌ Erro ao recuperar amostra de dados: {e}")
    
    print()
    print("="*107)
    print("✅ Verificação concluída")
    print("="*107)

if __name__ == "__main__":
    main()
