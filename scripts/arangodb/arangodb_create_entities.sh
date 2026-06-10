#!/bin/bash

# Configuración
DB_NAME="KhaBench"
USERNAME="root"
PASSWORD="tu_contraseña"
COORDINATOR_URL="http://KHAB-1:8529"

# Verificar conexión con ArangoDB
echo "Verificando conexión con ArangoDB..."
if ! curl --silent --fail "$COORDINATOR_URL/_api/version" > /dev/null; then
    echo "No se pudo conectar a ArangoDB en $COORDINATOR_URL"
    exit 1
fi
echo "Conexión exitosa con ArangoDB."

# Verificar si la base de datos existe
EXISTS=$(curl --silent --user "$USERNAME:$PASSWORD" "$COORDINATOR_URL/_api/database/user" | grep -o "$DB_NAME")
if [ "$EXISTS" == "$DB_NAME" ]; then
    echo " La base de datos '$DB_NAME' ya existe."
else
    echo " Creando la base de datos '$DB_NAME'..."
    curl --silent --user "$USERNAME:$PASSWORD" --request POST --data "{\"name\": \"$DB_NAME\"}" "$COORDINATOR_URL/_api/database"
    echo "Base de datos '$DB_NAME' creada exitosamente."
fi



# Crear colecciones con esquema
declare -A schemas
schemas["VENDOR"]='{
    "name": "VENDOR",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Vendor",
        "rule": {
            "type": "object",
            "properties": {
                "_key": {"type": "string"},
                "COUNTRY": {"type": "string"},
                "INDUSTRY": {"type": "string"}
            },
            "required": ["_key", "COUNTRY", "INDUSTRY"]
        }
    }
}'
schemas["CUSTOMER"]='{
    "name": "CUSTOMER",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Customer",
        "rule": {
            "type": "object",
            "properties": {
                "_key": { "type": "string" },
                "FIRST_NAME": { "type": "string" },
                "LAST_NAME": { "type": "string" },
                "GENDER": { "type": "string" },
                "BIRTHDAY": { "type": "string" },
                "CREATE_DATE": { "type": "string" },
                "LOCATION_IP": { "type": "string" },
                "BROWSER_USED": { "type": "string" },
                "PLACE": { "type": "number" }
            },
            "required": ["_key", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATE_DATE", "LOCATION_IP", "BROWSER_USED", "PLACE"]
        }
    }
}'
schemas["CUSTOMER_NORTH"]='{
    "name": "CUSTOMER_NORTH",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Customer",
        "rule": {
            "type": "object",
            "properties": {
                "_key": { "type": "string" },
                "FIRST_NAME": { "type": "string" },
                "LAST_NAME": { "type": "string" },
                "GENDER": { "type": "string" },
                "BIRTHDAY": { "type": "string" },
                "CREATE_DATE": { "type": "string" },
                "LOCATION_IP": { "type": "string" },
                "BROWSER_USED": { "type": "string" },
                "PLACE": { "type": "number" }
            },
            "required": ["_key", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATE_DATE", "LOCATION_IP", "BROWSER_USED", "PLACE"]
        }
    }
}'
schemas["CUSTOMER_CENTER"]='{
    "name": "CUSTOMER_CENTER",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Customer",
        "rule": {
            "type": "object",
            "properties": {
                "_key": { "type": "string" },
                "FIRST_NAME": { "type": "string" },
                "LAST_NAME": { "type": "string" },
                "GENDER": { "type": "string" },
                "BIRTHDAY": { "type": "string" },
                "CREATE_DATE": { "type": "string" },
                "LOCATION_IP": { "type": "string" },
                "BROWSER_USED": { "type": "string" },
                "PLACE": { "type": "number" }
            },
            "required": ["_key", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATE_DATE", "LOCATION_IP", "BROWSER_USED", "PLACE"]
        }
    }
}'
schemas["CUSTOMER_SOUTH"]='{
    "name": "CUSTOMER_SOUTH",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Customer",
        "rule": {
            "type": "object",
            "properties": {
                "_key": { "type": "string" },
                "FIRST_NAME": { "type": "string" },
                "LAST_NAME": { "type": "string" },
                "GENDER": { "type": "string" },
                "BIRTHDAY": { "type": "string" },
                "CREATE_DATE": { "type": "string" },
                "LOCATION_IP": { "type": "string" },
                "BROWSER_USED": { "type": "string" },
                "PLACE": { "type": "number" }
            },
            "required": ["_key", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATE_DATE", "LOCATION_IP", "BROWSER_USED", "PLACE"]
        }
    }
}'
schemas["PRODUCT"]='{
    "name": "PRODUCT",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Product",
        "rule": {
            "type": "object",
            "properties": {
                "_key": {"type": "string"},
                "TITLE": {"type": "string"},
                "PRICE": {"type": "number"},
                "IMG_URL": {"type": "string"},
                "SKU": {"type": "string"},
                "VENDOR_ID": {"type": "string"}
            },
            "required": ["_key", "TITLE", "PRICE", "IMG_URL", "SKU", "VENDOR_ID"]
        }
    }
}'
schemas["PRODUCT_CHEAP"]='{
    "name": "PRODUCT_CHEAP",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Product",
        "rule": {
            "type": "object",
            "properties": {
                "_key": {"type": "string"},
                "TITLE": {"type": "string"},
                "PRICE": {"type": "number"},
                "IMG_URL": {"type": "string"},
                "SKU": {"type": "string"},
                "VENDOR_ID": {"type": "string"}
            },
            "required": ["_key", "TITLE", "PRICE", "IMG_URL", "SKU", "VENDOR_ID"]
        }
    }
}'
schemas["PRODUCT_EXPENSIVE"]='{
    "name": "PRODUCT_EXPENSIVE",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Product",
        "rule": {
            "type": "object",
            "properties": {
                "_key": {"type": "string"},
                "TITLE": {"type": "string"},
                "PRICE": {"type": "number"},
                "IMG_URL": {"type": "string"},
                "SKU": {"type": "string"},
                "VENDOR_ID": {"type": "string"}
            },
            "required": ["_key", "TITLE", "PRICE", "IMG_URL", "SKU", "VENDOR_ID"]
        }
    }
}'
schemas["ORDER"]='{
    "name": "ORDER",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Order",
        "rule": {
            "type": "object",
            "properties": {
                "_key": {"type": "string"},
                "CUSTOMER_ID": {"type": "string"},
                "ORDER_ID": {"type": "string"},
                "TOTAL_PRICE": {"type": "number"},
                "ORDERLINE": {"type": "array"}
            },
            "required": ["_key", "CUSTOMER_ID", "ORDER_DATE", "TOTAL_PRICE", "ORDERLINE"]
        }
    }
}'
schemas["ORDER_PRE_PANDEMIC"]='{
    "name": "ORDER_PRE_PANDEMIC",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Order",
        "rule": {
            "type": "object",
            "properties": {
                "_key": {"type": "string"},
                "CUSTOMER_ID": {"type": "string"},
                "ORDER_ID": {"type": "string"},
                "TOTAL_PRICE": {"type": "number"},
                "ORDERLINE": {"type": "array"}
            },
            "required": ["_key", "CUSTOMER_ID", "ORDER_DATE", "TOTAL_PRICE", "ORDERLINE"]
        }
    }
}'
schemas["ORDER_POST_PANDEMIC"]='{
    "name": "ORDER_POST_PANDEMIC",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Order",
        "rule": {
            "type": "object",
            "properties": {
                "_key": {"type": "string"},
                "CUSTOMER_ID": {"type": "string"},
                "ORDER_ID": {"type": "string"},
                "TOTAL_PRICE": {"type": "number"},
                "ORDERLINE": {"type": "array"}
            },
            "required": ["_key", "CUSTOMER_ID", "ORDER_DATE", "TOTAL_PRICE", "ORDERLINE"]
        }
    }
}'
schemas["FEEDBACK"]='{
    "name": "FEEDBACK",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Feedback",
        "rule": {
            "type": "object",
            "properties": {
                "CUSTOMER_ID": {"type": "string"},
                "PRODUCT_ID": {"type": "string"},
                "RATE": {"type": "number"},
                "REVIEW": {"type": "string"}
            },
            "required": ["CUSTOMER_ID", "PRODUCT_ID", "RATE", "REVIEW"]
        }
    }
}'
schemas["INVOICE"]='{
    "name": "INVOICE",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Invoice",
        "rule": {
            "type": "object",
            "properties": {
                "_key": {"type": "string"},
                "CUSTOMER_ID": {"type": "string"},
                "ORDER_DATE": {"type": "string"},
                "TOTAL_PRICE": {"type": "number"},
                "ORDERLINE": {"type": "array"}
            },
            "required": ["_key", "CUSTOMER_ID", "ORDER_DATE", "TOTAL_PRICE", "ORDERLINE"]
        }
    }
}'
schemas["PERSON"]='{
    "name": "PERSON",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en PERSON",
        "rule": {
            "type": "object",
            "properties": {
                "_key": { "type": "string" },
                "FIRST_NAME": { "type": "string" },
                "LAST_NAME": { "type": "string" },
                "GENDER": { "type": "string" },
                "BIRTHDAY": { "type": "string" },
                "CREATE_DATE": { "type": "string" },
                "LOCATION_IP": { "type": "string" },
                "BROWSER_USED": { "type": "string" },
                "PLACE": { "type": "number" }
            },
            "required": ["_key", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATE_DATE", "LOCATION_IP", "BROWSER_USED", "PLACE"]
        }
    }
}'
schemas["PERSON_NORTH"]='{
    "name": "PERSON_NORTH",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en PERSON NORTH",
        "rule": {
            "type": "object",
            "properties": {
                "_key": { "type": "string" },
                "FIRST_NAME": { "type": "string" },
                "LAST_NAME": { "type": "string" },
                "GENDER": { "type": "string" },
                "BIRTHDAY": { "type": "string" },
                "CREATE_DATE": { "type": "string" },
                "LOCATION_IP": { "type": "string" },
                "BROWSER_USED": { "type": "string" },
                "PLACE": { "type": "number" }
            },
            "required": ["_key"]
        }
    }
}'
schemas["PERSON_CENTER"]='{
    "name": "PERSON_CENTER",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Customer",
        "rule": {
            "type": "object",
            "properties": {
                "_key": { "type": "string" },
                "FIRST_NAME": { "type": "string" },
                "LAST_NAME": { "type": "string" },
                "GENDER": { "type": "string" },
                "BIRTHDAY": { "type": "string" },
                "CREATE_DATE": { "type": "string" },
                "LOCATION_IP": { "type": "string" },
                "BROWSER_USED": { "type": "string" },
                "PLACE": { "type": "number" }
            },
            "required": ["_key", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATE_DATE", "LOCATION_IP", "BROWSER_USED", "PLACE"]
        }
    }
}'
schemas["PERSON_SOUTH"]='{
    "name": "PERSON_SOUTH",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Customer",
        "rule": {
            "type": "object",
            "properties": {
                "_key": { "type": "string" },
                "FIRST_NAME": { "type": "string" },
                "LAST_NAME": { "type": "string" },
                "GENDER": { "type": "string" },
                "BIRTHDAY": { "type": "string" },
                "CREATE_DATE": { "type": "string" },
                "LOCATION_IP": { "type": "string" },
                "BROWSER_USED": { "type": "string" },
                "PLACE": { "type": "number" }
            },
            "required": ["_key", "FIRST_NAME", "LAST_NAME", "GENDER", "BIRTHDAY", "CREATE_DATE", "LOCATION_IP", "BROWSER_USED", "PLACE"]
        }
    }
}'
schemas["TAG"]='{
    "name": "TAG",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Tag",
        "rule": {
            "type": "object",
            "properties": {
                "_key": {"type": "string"},
                "TITLE": {"type": "string"}
            },
            "required": ["_key","TITLE"]
        }
    }
}'
schemas["POST"]='{
    "name": "POST",
    "schema": {
        "level": "none",
        "message": "Datos inválidos en Post",
        "rule": {
            "type": "object",
            "properties": {
                "_key": { "type": "string" },
                "CREATE_DATE": { "type": "string", "default": "" },
                "IMAGE_FILE": { "type": "string" },
                "LOCATION_IP": { "type": "string" },
                "BROWSER_USED": { "type": "string" },
                "CONTENT": { "type": "string" },
                "LENGTH": { "type": "string" }
            },
            "required": ["_key", "CREATE_DATE", "IMAGE_FILE", "LOCATION_IP", "BROWSER_USED", "CONTENT", "LENGTH"]
        }
    }
}'

echo "Creando colecciones con esquemas..."
for collection in "${!schemas[@]}"; do
    echo "➕ Creando colección '$collection' con esquema..."
    curl --silent --user "$USERNAME:$PASSWORD" --request POST \
        --header "Content-Type: application/json" \
        --data "${schemas[$collection]}" \
        "$COORDINATOR_URL/_db/$DB_NAME/_api/collection" | jq '.'
    echo "Colección '$collection' creada con esquema."
done


# Crear relaciones (edges)
declare -A edges
edges["CUSTOMER_KNOWS_PERSON"]='{"from": ["CUSTOMER"], "to": ["PERSON"]}'

edges["PERSON_HAS_INTEREST_TAG"]='{"from": ["PERSON"], "to": ["TAG"]}'
edges["PERSON_NORTH_HAS_INTEREST_TAG"]='{"from": ["PERSON_NORTH"], "to": ["TAG"]}'
edges["PERSON_CENTER_HAS_INTEREST_TAG"]='{"from": ["PERSON_CENTER"], "to": ["TAG"]}'
edges["PERSON_SOUTH_HAS_INTEREST_TAG"]='{"from": ["PERSON_SOUTH"], "to": ["TAG"]}'

edges["POST_HAS_CREATOR_PERSON"]='{"from": ["POST"], "to": ["PERSON"]}'
edges["POST_HAS_CREATOR_PERSON_NORTH"]='{"from": ["POST"], "to": ["PERSON_NORTH"]}'
edges["POST_HAS_CREATOR_PERSON_CENTER"]='{"from": ["POST"], "to": ["PERSON_CENTER"]}'
edges["POST_HAS_CREATOR_PERSON_SOUTH"]='{"from": ["POST"], "to": ["PERSON_SOUTH"]}'

edges["POST_HAS_TAG"]='{"from": ["POST"], "to": ["TAG"]}'


#  Creación del Grafo en ArangoDB con edges personalizados
echo " Creando el grafo 'SocialNetwork' en ArangoDB..."
curl --silent --user "$USERNAME:$PASSWORD" --request POST \
    --header "Content-Type: application/json" \
    --data "{
        \"name\": \"SocialNetwork\",
        \"edgeDefinitions\": [
            {
                \"collection\": \"CUSTOMER_KNOWS_PERSON\",
                \"from\": [\"CUSTOMER\"],
                \"to\": [\"PERSON\"]
            },
            {
                \"collection\": \"PERSON_HAS_INTEREST_TAG\",
                \"from\": [\"PERSON\"],
                \"to\": [\"TAG\"]
            },
            {
                \"collection\": \"PERSON_NORTH_HAS_INTEREST_TAG\",
                \"from\": [\"PERSON_NORTH\"],
                \"to\": [\"TAG\"]
            },
            {
                \"collection\": \"PERSON_CENTER_HAS_INTEREST_TAG\",
                \"from\": [\"PERSON_CENTER\"],
                \"to\": [\"TAG\"]
            },
            {
                \"collection\": \"PERSON_SOUTH_HAS_INTEREST_TAG\",
                \"from\": [\"PERSON_SOUTH\"],
                \"to\": [\"TAG\"]
            },
            {
                \"collection\": \"POST_HAS_CREATOR_PERSON\",
                \"from\": [\"POST\"],
                \"to\": [\"PERSON\"]
            },
            {
                \"collection\": \"POST_HAS_CREATOR_PERSON_NORTH\",
                \"from\": [\"POST\"],
                \"to\": [\"PERSON_NORTH\"]
            },
            {
                \"collection\": \"POST_HAS_CREATOR_PERSON_CENTER\",
                \"from\": [\"POST\"],
                \"to\": [\"PERSON_CENTER\"]
            },
            {
                \"collection\": \"POST_HAS_CREATOR_PERSON_SOUTH\",
                \"from\": [\"POST\"],
                \"to\": [\"PERSON_SOUTH\"]
            },
            {
                \"collection\": \"POST_HAS_TAG\",
                \"from\": [\"POST\"],
                \"to\": [\"TAG\"]
            }
        ]
    }" \
    "$COORDINATOR_URL/_db/$DB_NAME/_api/gharial"

echo " Configuración completada."
