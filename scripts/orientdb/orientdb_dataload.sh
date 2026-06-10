#!/bin/bash

# Configuración de OrientDB
ORIENTDB_HOST="http://localhost:2480"
DB_NAME="KhaBench"
USERNAME="root"
PASSWORD="rootpwd"
HEADERS="Content-Type: application/json"

# Ruta del dataset
DATA_DIR="/home/khabench/Desktop/test/Dataset"

def delete_data(class_name):
    """Elimina todos los registros de una clase específica en la base de datos."""
    print(f"🚮 Borrando todos los registros de {class_name}...")

    # Comando SQL para eliminar todos los registros de la clase
    sql = f"DELETE FROM {class_name}"

    response = execute_query(sql, transaction=True)

    if response and "errors" in response:
        print(f"Error al borrar datos de {class_name}: {response['errors']}")
    else:
        print(f"Datos de {class_name} eliminados exitosamente.")

#  **Función para insertar datos en lotes**
insert_data_batch() {
    local class_name=$1
    local file_path=$2

    if [[ ! -f "$file_path" ]]; then
        echo " Archivo $file_path no encontrado. Saltando $class_name..."
        return
    fi

    echo "Cargando datos de $file_path en $class_name..."

    local batch_size=500
    local total_records=$(wc -l < "$file_path")
    local counter=0
    local batch="[]"

    while IFS='|' read -r CUSTOMER_ID FIRST_NAME LAST_NAME GENDER BIRTHDAY CREATE_DATE LOCATION_IP BROWSER_USED PLACE; do
        if [[ "$CUSTOMER_ID" == "CUSTOMER_ID" ]]; then continue; fi  # Saltar encabezado

        json_record=$(jq -n \
            --arg id "$CUSTOMER_ID" \
            --arg fn "$FIRST_NAME" \
            --arg ln "$LAST_NAME" \
            --arg gender "$GENDER" \
            --arg birth "$BIRTHDAY" \
            --arg create "$CREATE_DATE" \
            --arg ip "$LOCATION_IP" \
            --arg browser "$BROWSER_USED" \
            --arg place "$PLACE" \
            '{type: "cmd", language: "sql", command: ("INSERT INTO '"$class_name"' CONTENT {\"CUSTOMER_ID\": \"" + $id + "\", \"FIRST_NAME\": \"" + $fn + "\", \"LAST_NAME\": \"" + $ln + "\", \"GENDER\": \"" + $gender + "\", \"BIRTHDAY\": \"" + $birth + "\", \"CREATE_DATE\": \"" + $create + "\", \"LOCATION_IP\": \"" + $ip + "\", \"BROWSER_USED\": \"" + $browser + "\", \"PLACE\": " + ($place | tonumber) + " }") }')

        batch=$(jq --argjson batch "$batch" --argjson record "$json_record" '$batch + [$record]' <<< "[]")
        ((counter++))

        if (( counter % batch_size == 0 || counter == total_records )); then
            echo " Insertando lote hasta registro $counter / $total_records en [$class_name]..."

            # Validar que batch no esté vacío antes de hacer la petición
            if [[ "$batch" != "[]" ]]; then
                json_payload=$(jq -n --argjson ops "$batch" '{
                    transaction: true,
                    operations: ([{ type: "cmd", language: "sql", command: "BEGIN" }] + $ops + [{ type: "cmd", language: "sql", command: "COMMIT" }])
                }')

                response=$(curl -s -X POST -u "$USERNAME:$PASSWORD" -H "$HEADERS" -d "$json_payload" "$ORIENTDB_HOST/batch/$DB_NAME")
                echo "$response" | jq .

                if echo "$response" | jq -e '.errors' > /dev/null; then
                    echo "Error insertando en [$class_name]: $response"
                else
                    echo "Lote insertado correctamente en [$class_name]"
                fi
            fi
            batch="[]"  # Reiniciar lote
        fi
    done < "$file_path"
}

#  **Función para cargar Customer y sus fragmentos**
load_customer() {
    echo " Iniciando carga de Customer y sus fragmentos..."

    insert_data_batch "Customer_North" "$DATA_DIR/Customer/person_0_0_north.csv"
    insert_data_batch "Customer_Center" "$DATA_DIR/Customer/person_0_0_center.csv"
    insert_data_batch "Customer_South" "$DATA_DIR/Customer/person_0_0_south.csv"
    insert_data_batch "Customer" "$DATA_DIR/Customer/person_0_0.csv"

    echo " Carga de Customer y sus fragmentos completada."
}

#  **Ejecutar la carga**
load_customer