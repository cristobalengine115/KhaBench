#!/bin/bash

# Configuración
NODE_IP="192.168.1.11"
USERNAME="root"
PASSWORD="rootpwd"
BUCKET_NAME="KhaBench"
SCOPE_NAME="default"

echo "🧹 Eliminando colecciones del scope '$SCOPE_NAME' en bucket '$BUCKET_NAME'..."

# Obtener lista de colecciones
collections=$(curl -s -u $USERNAME:$PASSWORD http://$NODE_IP:8091/pools/default/buckets/$BUCKET_NAME/scopes \
  | jq -r ".scopes[] | select(.name==\"$SCOPE_NAME\") | .collections[].name")

for col in $collections; do
  echo " Eliminando colección '$col'..."
  ssh khabench@$NODE_IP "/opt/couchbase/bin/couchbase-cli collection-manage \
    --cluster http://localhost:8091 \
    --username $USERNAME \
    --password $PASSWORD \
    --bucket $BUCKET_NAME \
    --drop-collection $SCOPE_NAME.$col"
done

echo "Todas las colecciones del scope '$SCOPE_NAME' fueron eliminadas."
