#!/bin/bash

# Configuración
DB_NAME="KhaBench"
DB_TYPE="plocal"
USERNAME="root"
PASSWORD="rootpwd"
SERVER_URL="http://192.168.50.101:2480"  # Ajusta si usas otro nodo

# Crear la base de datos
echo " Creando la base de datos '$DB_NAME'..."
response=$(curl -s -u "$USERNAME:$PASSWORD" -X POST \
  -d "name=$DB_NAME&type=$DB_TYPE" \
  "$SERVER_URL/database/$DB_NAME/$DB_TYPE")

# Mostrar respuesta con formato
echo "$response" | jq .

# Verificar si fue exitoso
if echo "$response" | grep -q '"error":false'; then
    echo "Base de datos '$DB_NAME' creada exitosamente."
else
    echo "Error al crear la base de datos:"
    exit 1
fi

echo " Configuración de la base de datos y usuario completada exitosamente."