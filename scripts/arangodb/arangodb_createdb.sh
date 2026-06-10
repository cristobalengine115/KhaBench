#!/bin/bash

# Configuración
db_name="KhaBench"
coordinator_url="http://KHAB-1:8529"

# Verificar conexión con ArangoDB
echo "Verificando conexión con ArangoDB..."
curl --silent --fail "$coordinator_url/_api/version" > /dev/null
if [ $? -ne 0 ]; then
    echo "No se pudo conectar a ArangoDB en $coordinator_url"
    exit 1
fi
echo "Conexión exitosa con ArangoDB."

# Verificar si la base de datos existe
echo "Verificando si la base de datos '$db_name' existe..."
exists=$(curl --silent "$coordinator_url/_api/database" | jq -r '.result[].name' | grep -w "$db_name")

if [ "$exists" == "$db_name" ]; then
    echo " La base de datos '$db_name' ya existe."
    exit 0
fi

# Crear la base de datos
echo " Creando la base de datos '$db_name'..."
curl --silent --request POST \
    --data "{\"name\": \"$db_name\"}" \
    "$coordinator_url/_api/database"

if [ $? -eq 0 ]; then
    echo "Base de datos '$db_name' creada exitosamente."
else
    echo "Error al crear la base de datos."
    exit 1
fi