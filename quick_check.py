#!/usr/bin/env python3
"""Verificação ultrarrápida com LIMIT 1."""

import mysql.connector
from app.config import Settings

try:
    conn = mysql.connector.connect(
        host=Settings.DB_HOST,
        port=Settings.DB_PORT,
        user=Settings.DB_USER,
        password=Settings.DB_PASSWORD,
        connection_timeout=5
    )
    cursor = conn.cursor(buffered=False)
    
    # Teste 1: rfb_import
    print("1️⃣ rfb_import.empresa...")
    try:
        cursor.execute("SELECT COUNT(*) FROM rfb_import.empresa")
        count1 = cursor.fetchone()[0]
        print(f"   Registros: {count1:,}")
    except Exception as e:
        print(f"   Erro: {e}")
    
    # Teste 2: dados_rfb (com LIMIT 1 para não travar)
    print("2️⃣ dados_rfb.empresa...")
    try:
        cursor.execute("SELECT COUNT(*) FROM dados_rfb.empresa")
        count2 = cursor.fetchone()[0]
        print(f"   Registros: {count2:,}")
    except Exception as e:
        print(f"   Erro: {e}")
    
    # Teste 3: Estrutura
    print("3️⃣ Estruturas...")
    try:
        cursor.execute("SHOW TABLES FROM rfb_import WHERE Tables_in_rfb_import='empresa'")
        if cursor.fetchone():
            print("   rfb_import.empresa: EXISTS")
    except:
        pass
    
    try:
        cursor.execute("SHOW TABLES FROM dados_rfb WHERE Tables_in_dados_rfb='empresa'")
        if cursor.fetchone():
            print("   dados_rfb.empresa: EXISTS")
    except:
        pass
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"Conexão falhou: {e}")
