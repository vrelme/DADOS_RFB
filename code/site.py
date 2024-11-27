import http.server
import socketserver

# Caminho dos arquivos
DIRECTORY = "D:/OPERACAO/INPUT_FILES" 

# Arquivos no site
Handler = http.server.SimpleHTTPRequestHandler
Handler.directory = DIRECTORY

with socketserver.TCPServer(("", 8000), Handler) as httpd:
    print("Site criado")
    httpd.serve_forever()  # Adiciona esta linha para iniciar o servidor
