#!/bin/bash

# Configuración
DB_NAME="KhaBench"
USERNAME="root"
PASSWORD="root"
COORDINATOR_URL="http://khab-1:8529"

# Verificar conexión
echo "Verificando conexión con ArangoDB..."
if ! curl --silent --fail "$COORDINATOR_URL/_api/version" > /dev/null; then
    echo "No se pudo conectar a $COORDINATOR_URL"
    exit 1
fi
echo "Conexión exitosa."

# Eliminar grafo SocialNetwork si existe
echo "Verificando si el grafo 'SocialNetwork' existe..."
graph_exists=$(curl --silent -u "$USERNAME:$PASSWORD" "$COORDINATOR_URL/_db/$DB_NAME/_api/gharial/SocialNetwork" | jq -r '.error')

if [ "$graph_exists" == "false" ]; then
    echo "Eliminando grafo 'SocialNetwork'..."
    curl --silent -u "$USERNAME:$PASSWORD" --request DELETE "$COORDINATOR_URL/_db/$DB_NAME/_api/gharial/SocialNetwork"
    echo "|Grafo eliminado."
else
    echo "El grafo 'SocialNetwork' no existe o ya fue eliminado."
fi

# Obtener y eliminar colecciones
echo "Obteniendo colecciones..."
collections_json=$(curl --silent -u "$USERNAME:$PASSWORD" "$COORDINATOR_URL/_db/$DB_NAME/_api/collection")

if echo "$collections_json" | jq -e '.result' > /dev/null; then
    collections=$(echo "$collections_json" | jq -r '.result[] | select(.name != "_graphs") | .name')
    for col in $collections; do
        echo " Eliminando colección '$col'..."
        curl --silent -u "$USERNAME:$PASSWORD" --request DELETE "$COORDINATOR_URL/_db/$DB_NAME/_api/collection/$col"
        echo "'$col' eliminada."
    done
else
    echo "No se encontraron colecciones para eliminar."
fi

echo "Todas las colecciones y el grafo han sido eliminados (si existían)."