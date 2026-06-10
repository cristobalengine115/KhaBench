#!/bin/bash

# Configuración
NODE_IP="192.168.50.101"
USERNAME="Administrator"
PASSWORD="rootpwd"
BUCKET_NAME="KhaBench"

echo "Verificando conexión con Couchbase en $NODE_IP..."
if curl --silent -u "$USERNAME:$PASSWORD" http://$NODE_IP:8091/pools > /dev/null; then
    echo "Conexión exitosa con Couchbase."
else
    echo "No se pudo conectar a Couchbase en $NODE_IP:8091"
    exit 1
fi

# Verificar si el bucket ya existe
echo "Verificando si el bucket '$BUCKET_NAME' ya existe..."
bucket_exists=$(curl --silent -u "$USERNAME:$PASSWORD" http://$NODE_IP:8091/pools/default/buckets | jq -r '.[].name' | grep -w "$BUCKET_NAME")

if [ "$bucket_exists" == "$BUCKET_NAME" ]; then
    echo "El bucket '$BUCKET_NAME' ya existe."
    exit 0
fi

# Crear bucket remotamente vía SSH
echo " Creando el bucket '$BUCKET_NAME' en el nodo $NODE_IP..."
ssh khabench@$NODE_IP "/opt/couchbase/bin/couchbase-cli bucket-create \
  --cluster http://localhost:8091 \
  --username $USERNAME \
  --password $PASSWORD \
  --bucket $BUCKET_NAME \
  --bucket-type couchbase \
  --bucket-ramsize 256 \
  --bucket-replica 1 \
  --wait"

if [ $? -eq 0 ]; then
    echo "Bucket '$BUCKET_NAME' creado exitosamente."
else
    echo "Error al crear el bucket."
    exit 1
fi