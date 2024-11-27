import http.server
import socketserver
import os

# Caminho absoluto dos arquivos
DIRECTORY = os.path.abspath("D:/OPERACAO/INPUT_FILES")

# Arquivos no site
class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIRECTORY, **kwargs)

with socketserver.TCPServer(("", 8000), Handler) as httpd:
    print("Site criado")
    httpd.serve_forever()