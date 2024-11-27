import http.server
import socketserver
import os

# Caminho dos arquivos
DIRECTORY = os.path.abspath("D:\OPERACAO\INPUT_FILES")
print(DIRECTORY)

# Arquivos no site
Handler = http.server.SimpleHTTPRequestHandler
Handler.directory = DIRECTORY

with socketserver.TCPServer(("", 8000), Handler) as httpd:
    print("Site criado")
    httpd.serve_forever()  # Adiciona esta linha para iniciar o servidor
