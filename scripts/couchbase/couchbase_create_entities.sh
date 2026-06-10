#!/bin/bash

# Configuración
NODE_IP="192.168.50.101"
USERNAME="Administrator"
PASSWORD="rootpwd"
BUCKET_NAME="KhaBench"
SCOPE_NAME="KhaBench"

ENTIDADES=(
  "CUSTOMER"
  "CUSTOMER_NORTH"
  "CUSTOMER_CENTER"
  "CUSTOMER_SOUTH"
  "PERSON"
  "PERSON_NORTH"
  "PERSON_CENTER"
  "PERSON_SOUTH"
  "VENDOR"
  "PRODUCT"
  "PRODUCT_CHEAP"
  "PRODUCT_EXPENSIVE"
  "ORDER"
  "ORDER_PRE_PANDEMIC"
  "ORDER_POST_PANDEMIC"
  "INVOICE"
  "FEEDBACK"
  "POST"
  "TAG"
)

EDGES=(
  "CUSTOMER_KNOWS_PERSON"
  "PERSON_HAS_INTEREST_TAG"
  "PERSON_NORTH_HAS_INTEREST_TAG"
  "PERSON_CENTER_HAS_INTEREST_TAG"
  "PERSON_SOUTH_HAS_INTEREST_TAG"
  "POST_HAS_CREATOR_PERSON"
  "POST_HAS_CREATOR_PERSON_NORTH"
  "POST_HAS_CREATOR_PERSON_CENTER"
  "POST_HAS_CREATOR_PERSON_SOUTH"
  "POST_HAS_TAG"
)

# Crear scope
echo "Creando scope '$SCOPE_NAME' en bucket '$BUCKET_NAME'..."
ssh khabench@$NODE_IP "/opt/couchbase/bin/couchbase-cli collection-manage \
  --cluster http://localhost:8091 \
  --username $USERNAME \
  --password $PASSWORD \
  --bucket $BUCKET_NAME \
  --create-scope $SCOPE_NAME"

# Crear colecciones de entidades + índice primario
for collection in "${ENTIDADES[@]}"; do
  echo "📁 Creando colección '$collection'..."
  ssh khabench@$NODE_IP "/opt/couchbase/bin/couchbase-cli collection-manage \
    --cluster http://localhost:8091 \
    --username $USERNAME \
    --password $PASSWORD \
    --bucket $BUCKET_NAME \
    --create-collection $SCOPE_NAME.$collection"

  echo " Creando índice primario para '$collection'..."
  ssh khabench@$NODE_IP "/opt/couchbase/bin/cbq \
    -u $USERNAME -p $PASSWORD -e http://localhost:8093 \
    -s 'CREATE PRIMARY INDEX ON \`$BUCKET_NAME\`.\`$SCOPE_NAME\`.\`$collection\`;'" > /dev/null
done

# Crear colecciones para edges + índice primario
for edge in "${EDGES[@]}"; do
  echo "🔗 Creando colección de relación '$edge'..."
  ssh khabench@$NODE_IP "/opt/couchbase/bin/couchbase-cli collection-manage \
    --cluster http://localhost:8091 \
    --username $USERNAME \
    --password $PASSWORD \
    --bucket $BUCKET_NAME \
    --create-collection $SCOPE_NAME.$edge"

  echo " Creando índice primario para '$edge'..."
  ssh khabench@$NODE_IP "/opt/couchbase/bin/cbq \
    -u $USERNAME -p $PASSWORD -e http://localhost:8093 \
    -s 'CREATE PRIMARY INDEX ON \`$BUCKET_NAME\`.\`$SCOPE_NAME\`.\`$edge\`;'" > /dev/null
done

echo "Todas las colecciones y sus índices fueron creados en el scope '$SCOPE_NAME'."