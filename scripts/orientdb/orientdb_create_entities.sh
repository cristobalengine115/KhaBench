#!/bin/bash

# Configuración
DB_NAME="KhaBench"
USERNAME="root"
PASSWORD="rootpwd"
SQL_FILE="orientdb_create_schema.sql"  # Asegúrate de que el archivo esté en el servidor
SERVER_IP="192.168.50.101"  # IP del servidor donde corre OrientDB

# Verifica si el archivo SQL existe en el servidor
echo "Verificando si el archivo SQL existe en el servidor..."
ssh khabench@$SERVER_IP "test -f /home/khabench/scripts/$SQL_FILE"

if [ $? -ne 0 ]; then
    echo "Error: No se encontró el archivo SQL en /home/khabench/scripts/$SQL_FILE en el servidor."
    exit 1
fi

# Ejecutar el script de creación de entidades usando la consola de OrientDB
echo " Ejecutando el script de creación de entidades..."

ssh khabench@$SERVER_IP "/opt/orientdb/bin/console.sh <<EOF
CONNECT remote:localhost/$DB_NAME $USERNAME $PASSWORD;
LOAD SCRIPT /home/khabench/scripts/$SQL_FILE;
EXIT;
EOF"

# Validar la ejecución
if [ $? -eq 0 ]; then
    echo " Creación de entidades completada exitosamente."
else
    echo "Error al ejecutar el script."
    exit 1
fi