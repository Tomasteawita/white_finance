from bs4 import BeautifulSoup

# Abre el archivo HTML
with open('ruta/al/archivo.html', 'r', encoding='utf-8') as file:
    contenido = file.read()

# Crea un objeto BeautifulSoup
soup = BeautifulSoup(contenido, 'lxml')

# Ahora puedes explorar el contenido del archivo HTML
print(soup.prettify())
