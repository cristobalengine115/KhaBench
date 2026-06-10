import os
import pandas as pd
import requests
import json
import unicodedata
import gc
import xml.etree.ElementTree as ET
from tqdm import tqdm
import time

# Configuración de OrientDB 
ORIENTDB_HOST = "http://192.168.50.101:2480"
DB_NAME = "KhaBench"
USERNAME = "root"
PASSWORD = "rootpwd"
HEADERS = {"Accept": "application/json"}

# Ruta de datos
DATA_DIR = "../../Dataset/"


def delete_edge_data(class_name):
    """Elimina todos los edges de una clase específica."""
    print(f"🚮 Borrando todos los edges de {class_name}...")

    sql = f"DELETE EDGE {class_name}"
    response = execute_query(sql, transaction=True)

    if response and "errors" in response:
        print(f"Error al borrar edges de {class_name}: {response['errors']}")
    else:
        print(f"Edges de {class_name} eliminados exitosamente.")

def delete_data(class_name):
    """Elimina todos los registros de una clase específica en la base de datos."""
    print(f"🚮 Borrando todos los registros de {class_name}...")

    # Comando SQL para eliminar todos los registros de la clase (vértices)
    sql = f"DELETE VERTEX {class_name}"
    response = execute_query(sql, transaction=True)

    if response and "errors" in response:
        print(f"Error al borrar datos de {class_name}: {response['errors']}")
    else:
        print(f"Datos de {class_name} eliminados exitosamente.")

def insert_batch(class_name, records, cluster_name, batch_size=5000):
    """Inserta datos en lotes pequeños para evitar consumo excesivo de memoria."""

    if not records:
        return

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]

        sql_statements = ["BEGIN"]  
        # Ahora agregamos el cluster en la sentencia de inserción
        sql_statements += [f"INSERT INTO {class_name} CLUSTER {cluster_name} CONTENT {json.dumps(record)}" for record in batch]
        sql_statements.append("COMMIT")  

        batch_query = ";\n".join(sql_statements)
        response = execute_query(batch_query, transaction=True)

        if response and "errors" in response:
            execute_query("ROLLBACK;", transaction=True)

        # 🧹 **Liberamos memoria tras cada lote**
        del batch, sql_statements, batch_query
        gc.collect()


def insert_batch_orders(class_name, records, cluster_name, batch_size=500):
    """Inserta órdenes en texto (sin RIDs). ORDERLINE = lista de STR."""
    if not records:
        return

    def q(v: str) -> str:
        # Comillas dobles y escape de comillas internas
        return '"' + str(v).replace('"', '\\"') + '"'

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        sql_statements = ["BEGIN"]

        for r in batch:
            # CUSTOMER_ID como string
            cid = q(r["CUSTOMER_ID"])
            # ORDER_DATE ya viene en 'YYYY-MM-DD' desde load_order_data
            od = q(r["ORDER_DATE"])
            # Lista de strings con comillas
            productos = ", ".join(q(p) for p in r["ORDERLINE"] if p)

            stmt = (
                f'INSERT INTO {class_name} CLUSTER {cluster_name} SET '
                f'ORDER_ID = {q(r["ORDER_ID"])}, '
                f'CUSTOMER_ID = {cid}, '
                f'ORDER_DATE = {od}, '
                f'TOTAL_PRICE = {float(r["TOTAL_PRICE"])}, '
                f'ORDERLINE = [{productos}]'
            )
            sql_statements.append(stmt)

        sql_statements.append("COMMIT")
        batch_query = ";\n".join(sql_statements)
        response = execute_query(batch_query, transaction=True)
        if response and "errors" in response:
            print("Error al insertar lote de órdenes. Ejecutando ROLLBACK...")
            execute_query("ROLLBACK;", transaction=True)

def insert_edge_batch(class_name, edges, cluster_name, batch_size=5000):
    if not edges:
        return

    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        statements = ["BEGIN"]
        for edge in batch:
            statements.append(
                f'CREATE EDGE {class_name} CLUSTER {cluster_name} FROM {edge["from"]} TO {edge["to"]} SET creationDate = "{edge["creationDate"]}"'
            )
        statements.append("COMMIT")
        query = ";\n".join(statements)
        response = execute_query(query, transaction=True)

        if response and "errors" in response:
            print(f"Error en batch: {response['errors']}")

def insert_edge_batch_simple(class_name, edges, cluster_name, batch_size=5000):
    if not edges:
        return

    for i in range(0, len(edges), batch_size):
        batch = edges[i:i + batch_size]
        statements = ["BEGIN"]
        
        for edge in batch:
            # Usamos la misma sintaxis de CREATE EDGE, pero sin creationDate
            statements.append(
                f'CREATE EDGE {class_name} CLUSTER {cluster_name} FROM {edge["from"]} TO {edge["to"]}'
            )
        
        statements.append("COMMIT")
        query = ";\n".join(statements)
        response = execute_query(query, transaction=True)

        if response and "errors" in response:
            print(f"Error en batch: {response['errors']}")


def execute_query(sql, transaction=False):
    url = f"{ORIENTDB_HOST}/batch/{DB_NAME}"
    data = {
        "transaction": transaction,
        "operations": [{"type": "cmd", "language": "sql", "command": sql}]
    }
    try:
        response = requests.post(url, auth=(USERNAME, PASSWORD), json=data, headers=HEADERS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error en query ({response.status_code}): {response.text}")
            return None
    except Exception as e:
        print(f"Excepción en query: {e}")
        return None


#  **Carga de Datos para `Customer` y sus Fragmentos con Transacción**
def load_customer_data(entity_name, file_name, cluster_name):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {entity_name}...")
        return

    print(f"Cargando {file_name} en {entity_name}...")
    df = pd.read_csv(file_path, sep="|", dtype=str, quotechar='"',
                     skipinitialspace=True, on_bad_lines="skip")

    df.rename(columns={
        "ID": "CUSTOMER_ID",
        "FIRSTNAME": "FIRST_NAME",
        "LASTNAME": "LAST_NAME",
        "GENDER": "GENDER",
        "BIRTHDAY": "BIRTHDAY",
        "CREATION_DATE": "CREATE_DATE",
        "LOCATION_IP": "LOCATION_IP",
        "BROWSER_USED": "BROWSER_USED",
        "PLACE": "PLACE"
    }, inplace=True)

    df["BIRTHDAY"] = pd.to_datetime(df["BIRTHDAY"], errors="coerce").dt.normalize().dt.strftime("%Y-%m-%d")
    df["CREATE_DATE"] = pd.to_datetime(df["CREATE_DATE"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df["PLACE"] = pd.to_numeric(df["PLACE"], errors="coerce").fillna(0).astype(int)

    print(f" Insertando {len(df)} registros en {entity_name}...")
    insert_batch(entity_name, df.to_dict(orient="records"), cluster_name)
    print(f"Carga de {entity_name} completada.")

def get_tag_rids():
    print("Cargando RIDs de Tag...")
    query = "SELECT TAG_ID, @rid FROM Tag"
    result = execute_query(query)
    rids = {record["TAG_ID"]: record["@rid"] for record in result.get("result", [])}
    print(f"Se cargaron {len(rids)} RIDs de Tag.")
    return rids

def load_vendor_data(entity_name, file_name, cluster_name):
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {entity_name}...")
        return

    print(f"Cargando {file_path} en {entity_name}...")

    df = pd.read_csv(file_path, sep=",", dtype=str, quotechar='"', skipinitialspace=True, on_bad_lines="skip")

    #  Verificar que contiene las columnas necesarias
    expected_columns = {"VENDOR_ID", "COMPANY", "COUNTRY", "INDUSTRY"}
    missing_columns = expected_columns - set(df.columns)
    if missing_columns:
        print(f"ERROR: Faltan las columnas {missing_columns} en el CSV de {entity_name}.")
        return

    # Convertir tipos de datos
    df["VENDOR_ID"] = df["VENDOR_ID"].astype(str).str.strip()
    df["COMPANY"] = df["COMPANY"].astype(str).str.strip()
    df["COUNTRY"] = df["COUNTRY"].astype(str).str.strip()
    df["INDUSTRY"] = df["INDUSTRY"].astype(str).str.strip()

    #  Imprimir un ejemplo de los datos antes de insertarlos
    print(f"Primeras filas de {entity_name}:\n{df.head()}")

    print(f" Insertando {len(df)} registros en {entity_name} dentro de una transacción...")
    insert_batch(entity_name, df.to_dict(orient="records"), cluster_name)
    print(f"Carga de {entity_name} completada.")

def load_product_data(entity_name, file_name, cluster_name):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" {file_path} no encontrado. Saltando {entity_name}..."); return

    print(f"Cargando {file_path} en {entity_name}...")
    df = pd.read_csv(file_path, sep=",", dtype=str, quotechar='"',
                     skipinitialspace=True, on_bad_lines="skip")

    df["PRICE"]     = pd.to_numeric(df["PRICE"], errors="coerce").fillna(0).astype(float)
    for c in ["PRODUCT_ID","TITLE","IMG_URL","SKU","VENDOR_ID"]:
        df[c] = df[c].astype(str).str.strip()

    #  sin LINK a Vendor; solo guardamos la llave de negocio VENDOR_ID
    print(f" Insertando {len(df)} registros en {entity_name}...")
    insert_batch(entity_name, df.to_dict(orient="records"), cluster_name)
    print("Product listo.")


def load_person_data(entity_name, file_name, cluster_name):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {entity_name}...")
        return

    print(f"Cargando {file_name} en {entity_name}...")
    df = pd.read_csv(file_path, sep="|", dtype=str, quotechar='"',
                     skipinitialspace=True, on_bad_lines="skip")

    df.rename(columns={
        "ID": "PERSON_ID",
        "FIRSTNAME": "FIRST_NAME",
        "LASTNAME": "LAST_NAME",
        "GENDER": "GENDER",
        "BIRTHDAY": "BIRTHDAY",
        "CREATION_DATE": "CREATE_DATE",
        "LOCATION_IP": "LOCATION_IP",
        "BROWSER_USED": "BROWSER_USED",
        "PLACE": "PLACE"
    }, inplace=True)

    df["BIRTHDAY"] = pd.to_datetime(df["BIRTHDAY"], errors="coerce").dt.normalize().dt.strftime("%Y-%m-%d")
    df["CREATE_DATE"] = pd.to_datetime(df["CREATE_DATE"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df["PLACE"] = pd.to_numeric(df["PLACE"], errors="coerce").fillna(0).astype(int)

    print(f" Insertando {len(df)} registros en {entity_name}...")
    insert_batch(entity_name, df.to_dict(orient="records"), cluster_name)
    print(f"Carga de {entity_name} completada.")


def load_order_data(class_name, file_name, cluster_name):
    """Lee JSON lines y guarda claves de negocio en texto (sin RIDs)."""
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {class_name}...")
        return

    print(f"Cargando {file_path} en {class_name}...")
    orders = []
    with open(file_path, "r") as f:
        for line in f:
            try:
                orders.append(json.loads(line.strip()))
            except json.JSONDecodeError:
                continue

    records = []
    for order in orders:
        oid = str(order.get("ORDER_ID", "")).strip()
        cid = str(order.get("CUSTOMER_ID", "")).strip()

        # Fecha a YYYY-MM-DD; si es inválida, se omite el registro
        try:
            od = pd.to_datetime(order.get("ORDER_DATE", ""), errors="raise").strftime("%Y-%m-%d")
        except Exception:
            continue

        try:
            tp = float(order.get("TOTAL_PRICE", 0))
        except Exception:
            tp = 0.0

        # Lista de PRODUCT_ID como strings
        products = []
        for item in order.get("ORDER_LINE", []):
            pid = str(item.get("PRODUCT_ID", "")).strip()
            if pid:
                products.append(pid)

        if not (oid and cid and products):
            # Requiere id de orden, cliente y al menos un producto
            continue

        records.append({
            "ORDER_ID": oid,
            "CUSTOMER_ID": cid,    # ← texto, no RID
            "ORDER_DATE": od,
            "TOTAL_PRICE": tp,
            "ORDERLINE": products  # ← lista de strings, no RIDs
        })

    print(f"Total de órdenes válidas a insertar: {len(records)}")
    insert_batch_orders(class_name, records, cluster_name)



def load_post_data(class_name, file_name, cluster_name):
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {class_name}...")
        return

    print(f"Cargando {file_path} en {class_name}...")

    df = pd.read_csv(
        file_path,
        sep="|",
        dtype=str,
        quotechar='"',
        skipinitialspace=True,
        on_bad_lines="skip"
    )

    # Renombrar columnas si es necesario
    df.rename(columns={
        "POST_ID": "POST_ID",
        "IMAGE_FILE": "IMAGE_FILE",
        "CREATE_DATE": "CREATE_DATE",
        "LOCATION_IP": "LOCATION_IP",
        "BROWSER_USED": "BROWSER_USED",
        "LANGUAGE": "LANGUAGE",
        "CONTENT": "CONTENT",
        "LENGTH": "LENGTH"
    }, inplace=True)

    # Convertir tipos
    df["POST_ID"] = df["POST_ID"].astype(str).str.strip()
    df["IMAGE_FILE"] = df["IMAGE_FILE"].fillna("").astype(str)
    df["CREATE_DATE"] = pd.to_datetime(df["CREATE_DATE"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df["LOCATION_IP"] = df["LOCATION_IP"].astype(str)
    df["BROWSER_USED"] = df["BROWSER_USED"].astype(str)
    df["LANGUAGE"] = df["LANGUAGE"].astype(str)
    df["CONTENT"] = df["CONTENT"].astype(str)
    df["LENGTH"] = pd.to_numeric(df["LENGTH"], errors="coerce").fillna(0).astype(int)

    print(f" Insertando {len(df)} registros en {class_name}...")
    insert_batch(class_name, df.to_dict(orient="records"), cluster_name)
    print(f"Carga de {class_name} completada.")

def load_tag_data(class_name, file_name, cluster_name):
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {class_name}...")
        return

    print(f"Cargando {file_path} en {class_name}...")

    df = pd.read_csv(
        file_path,
        sep="|",
        dtype=str,
        quotechar='"',
        skipinitialspace=True,
        on_bad_lines="skip"
    )

    # Renombrar columnas antes de acceder a ellas
    df.rename(columns={"xID": "TAG_ID", "TITLE": "TITLE"}, inplace=True)

    # Normalizar datos
    df["TAG_ID"] = df["TAG_ID"].astype(str).str.strip()
    df["TITLE"] = df["TITLE"].astype(str).str.strip()

    print(f" Insertando {len(df)} registros en {class_name}...")
    insert_batch(class_name, df.to_dict(orient="records"), cluster_name)
    print(f"Carga de {class_name} completada.")

def load_post_data(class_name, file_name, cluster_name):
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {class_name}...")
        return

    print(f"Cargando {file_path} en {class_name}...")

    df = pd.read_csv(
        file_path,
        sep="|",
        dtype=str,
        quotechar='"',
        skipinitialspace=True,
        on_bad_lines="skip"
    )

    # Renombrar columnas si es necesario
    df.rename(columns={
        "POST_ID": "POST_ID",
        "IMAGE_FILE": "IMAGE_FILE",
        "CREATE_DATE": "CREATE_DATE",
        "LOCATION_IP": "LOCATION_IP",
        "BROWSER_USED": "BROWSER_USED",
        "LANGUAGE": "LANGUAGE",
        "CONTENT": "CONTENT",
        "LENGTH": "LENGTH"
    }, inplace=True)

    # Convertir tipos
    df["POST_ID"] = df["POST_ID"].astype(str).str.strip()
    df["IMAGE_FILE"] = df["IMAGE_FILE"].fillna("").astype(str)
    df["CREATE_DATE"] = pd.to_datetime(df["CREATE_DATE"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df["LOCATION_IP"] = df["LOCATION_IP"].astype(str)
    df["BROWSER_USED"] = df["BROWSER_USED"].astype(str)
    df["LANGUAGE"] = df["LANGUAGE"].astype(str)
    df["CONTENT"] = df["CONTENT"].astype(str)
    df["LENGTH"] = pd.to_numeric(df["LENGTH"], errors="coerce").fillna(0).astype(int)

    print(f" Insertando {len(df)} registros en {class_name}...")
    insert_batch(class_name, df.to_dict(orient="records"), cluster_name)
    print(f"Carga de {class_name} completada.")



def load_customer_knows_person_edge(class_name, file_name, cluster_name):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {class_name}...")
        return

    print(f"Cargando {file_path} en {class_name}...")

    df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")
    df.dropna(subset=["from", "to"], inplace=True)
    df["from"] = df["from"].astype(str).str.strip()
    df["to"] = df["to"].astype(str).str.strip()
    df["creationDate"] = pd.to_datetime(df["creationDate"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    from_ids = df["from"].unique().tolist()
    to_ids = df["to"].unique().tolist()

    from_query = f'SELECT CUSTOMER_ID, @rid FROM Customer WHERE CUSTOMER_ID IN [{", ".join(["\"" + cid + "\"" for cid in from_ids])}]'
    to_query = f'SELECT PERSON_ID, @rid FROM Person WHERE PERSON_ID IN [{", ".join(["\"" + pid + "\"" for pid in to_ids])}]'

    from_result = execute_query(from_query)
    to_result = execute_query(to_query)

    from_rid_map = {record["CUSTOMER_ID"]: record["@rid"] for record in from_result.get("result", [])}
    to_rid_map = {record["PERSON_ID"]: record["@rid"] for record in to_result.get("result", [])}

    print(f"RIDs de Customer (ejemplo): {list(from_rid_map.items())[:5]}")
    print(f"RIDs de Person (ejemplo): {list(to_rid_map.items())[:5]}")

    edges = []
    for _, row in df.iterrows():
        from_id = row["from"]
        to_id = row["to"]
        date = row["creationDate"]

        if from_id in from_rid_map and to_id in to_rid_map:
            edges.append({
                "from": from_rid_map[from_id],
                "to": to_rid_map[to_id],
                "creationDate": date
            })

    print(f" Insertando {len(edges)} edges en {class_name}...")
    insert_edge_batch(class_name, edges, cluster_name)
    print(f"Carga de {class_name} completada.")

def load_person_has_interest_tag(class_name, file_name, cluster_name):
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {class_name}...")
        return

    print(f"Cargando {file_path} en {class_name}...")

    df = pd.read_csv(file_path, sep="|", dtype=str, quotechar='"', skipinitialspace=True, on_bad_lines="skip")
    df["PERSON_ID"] = df["PERSON_ID"].astype(str).str.strip()
    df["TAG_ID"] = df["TAG_ID"].astype(str).str.strip()

    person_ids = df["PERSON_ID"].unique().tolist()
    tag_ids = df["TAG_ID"].unique().tolist()

    quoted_pids = ', '.join(['"%s"' % pid for pid in person_ids])
    quoted_tids = ', '.join(['"%s"' % tid for tid in tag_ids])

    person_query = f"SELECT PERSON_ID, @rid FROM Person WHERE PERSON_ID IN [{quoted_pids}]"
    tag_query = f"SELECT TAG_ID, @rid FROM Tag WHERE TAG_ID IN [{quoted_tids}]"

    person_result = execute_query(person_query)
    tag_result = execute_query(tag_query)

    person_rids = {record["PERSON_ID"]: record["@rid"] for record in person_result.get("result", [])}
    tag_rids = {record["TAG_ID"]: record["@rid"] for record in tag_result.get("result", [])}

    edges = []
    omitidos = []
    for _, row in df.iterrows():
        pid = row["PERSON_ID"]
        tid = row["TAG_ID"]
        f = person_rids.get(pid)
        t = tag_rids.get(tid)
        if f and t:
            edges.append({
                "from": f,
                "to": t
            })
        else:
            omitidos.append({"PERSON_ID": pid, "TAG_ID": tid})

    print(f" Insertando {len(edges)} relaciones en {class_name}...")
    insert_edge_batch_simple(class_name, edges, cluster_name)  # Pasar también el nombre del cluster


def query_rids_in_chunks(entity, id_field, ids, chunk_size=1000):
    rid_map = {}
    for i in range(0, len(ids), chunk_size):
        sub_ids = ids[i:i + chunk_size]
        quoted = ', '.join(['"%s"' % _id for _id in sub_ids])
        query = f"SELECT {id_field}, @rid FROM {entity} WHERE {id_field} IN [{quoted}]"
        result = execute_query(query)
        if result and "result" in result:
            for record in result["result"]:
                rid_map[record[id_field]] = record["@rid"]
    return rid_map

def generate_rid_map(class_name, id_field):
    print(f"Cargando RIDs de {class_name}...")
    query = f"SELECT {id_field}, @rid FROM {class_name}"
    result = execute_query(query)

    if result is None or "result" not in result:
        print(f"Error al consultar RIDs para {class_name}. La base no respondió.")
        return {}

    rid_map = {record[id_field]: record["@rid"] for record in result.get("result", [])}
    print(f"Se cargaron {len(rid_map)} RIDs de {class_name}.")
    return rid_map

def load_post_has_creator_person(class_name, file_name, post_rids_map, person_rids_map, cluster_name, chunksize=50000):
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {class_name}...")
        return

    print(f"Cargando {file_path} en {class_name}...")

    total_inserted = 0
    chunk_idx = 1

    for chunk in pd.read_csv(file_path, sep="|", dtype=str, chunksize=chunksize):
        print(f"\n Procesando chunk #{chunk_idx}...")
        start_time = time.time()

        chunk["POST_ID"] = chunk["POST_ID"].astype(str).str.strip()
        chunk["PERSON_ID"] = chunk["PERSON_ID"].astype(str).str.strip()

        edges = []
        for _, row in chunk.iterrows():
            f = post_rids_map.get(row["POST_ID"])
            t = person_rids_map.get(row["PERSON_ID"])
            if f and t:
                edges.append({"from": t, "to": f})

        insert_edge_batch_simple(class_name, edges, cluster_name)  # Se pasa el nombre del cluster
        inserted_count = len(edges)
        total_inserted += inserted_count
        elapsed = round(time.time() - start_time, 2)
        print(f"Chunk #{chunk_idx} insertó {inserted_count} edges en {elapsed} s")

        chunk_idx += 1

    print(f"\nTotal de relaciones insertadas en {class_name}: {total_inserted}")


def load_post_has_tag(class_name, file_name, post_rids_map, tag_rids_map, cluster_name, chunksize=70000):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {class_name}...")
        return

    print(f"Cargando {file_path} en {class_name}...")
    total_inserted = 0
    chunk_num = 1

    for chunk in pd.read_csv(file_path, sep="|", dtype=str, chunksize=chunksize):
        print(f"\n Procesando chunk #{chunk_num}...")
        t0 = time.time()
        chunk["POST_ID"] = chunk["POST_ID"].astype(str).str.strip()
        chunk["TAG_ID"] = chunk["TAG_ID"].astype(str).str.strip()

        edges = []
        for _, row in chunk.iterrows():
            f = post_rids_map.get(row["POST_ID"])
            t = tag_rids_map.get(row["TAG_ID"])
            if f and t:
                edges.append({"from": f, "to": t})

        insert_edge_batch_simple(class_name, edges, cluster_name)  # Se pasa el nombre del cluster
        t1 = time.time()
        print(f"Chunk #{chunk_num} insertó {len(edges)} edges en {round(t1 - t0, 2)} s")
        total_inserted += len(edges)
        chunk_num += 1

    print(f"\nTotal de relaciones insertadas en {class_name}: {total_inserted}")


# delete_data("Vendor")
# load_vendor_data("Vendor", "Vendor/Vendor.csv", "vendor_global_cluster")


# delete_data("Customer")
# load_customer_data("Customer", "Customer/person_0_0.csv", "customer_global_cluster")
# delete_data("Customer_North")
# load_customer_data("Customer_North", "Customer/person_0_0_north.csv", "customer_north_cluster")
# delete_data("Customer_Center")
# load_customer_data("Customer_Center", "Customer/person_0_0_center.csv", "customer_center_cluster")
# delete_data("Customer_South")
# load_customer_data("Customer_South", "Customer/person_0_0_south.csv", "customer_south_cluster")



# delete_data("Product")
# load_product_data("Product", "Product/Product.csv", "product_global_cluster")
# delete_data("Product_Cheap")
# load_product_data("Product_Cheap", "Product/Product_Cheap.csv", "product_cheap_cluster")
# delete_data("Product_Expensive")
# load_product_data("Product_Expensive", "Product/Product_Expensive.csv", "product_expensive_cluster")


# delete_data("Order")
# load_order_data("Order", "Order/Order.json", "order_global_cluster")
delete_data("Order_Pre_Pandemic")
load_order_data("Order_Pre_Pandemic", "Order/Order_Pre_Pandemic.json", "order_pre_pandemic_cluster")
# delete_data("Order_Post_Pandemic")
# load_order_data("Order_Post_Pandemic", "Order/Order_Post_Pandemic.json", "order_post_pandemic_cluster")

# delete_data("Feedback")
# load_feedback_data("Feedback", "Feedback/Feedback.csv", "feedback_global_cluster")

# delete_data("Invoice")
# load_invoice_data("Invoice", "Invoice/Invoice.xml", "invoice_global_cluster")


# delete_data("PERSON")
# load_person_data("Person", "Customer/person_0_0.csv", "person_global_cluster")
# delete_data("PERSON_NORTH")
# load_person_data("Person_North", "Customer/person_0_0_north.csv", "person_north_cluster")
# delete_data("PERSON_CENTER")
# load_person_data("Person_Center", "Customer/person_0_0_center.csv", "person_center_cluster")
# delete_data("PERSON_SOUTH")
# load_person_data("Person_South", "Customer/person_0_0_south.csv", "person_south_cluster")

# delete_data("Tag")
# load_tag_data("Tag", "SocialNetwork/Tag.csv", "tag_global_cluster")

# delete_data("Post")
# load_post_data("Post", "SocialNetwork/Post.csv", "post_global_cluster")



# delete_edge_data("CUSTOMER_KNOWS_PERSON")
# load_customer_knows_person_edge("CUSTOMER_KNOWS_PERSON", "SocialNetwork/person_knows_person.csv", "customer_knows_person_global_cluster")


# delete_edge_data("PERSON_HAS_INTEREST_TAG")
# load_person_has_interest_tag("PERSON_HAS_INTEREST_TAG", "SocialNetwork/person_has_interest_tag.csv", "person_global_has_interest_tag_cluster")
# delete_edge_data("PERSON_NORTH_HAS_INTEREST_TAG")
# load_person_has_interest_tag("PERSON_NORTH_HAS_INTEREST_TAG", "SocialNetwork/person_north_has_interest_tag.csv", "person_north_has_interest_tag_cluster")
# delete_edge_data("PERSON_CENTER_HAS_INTEREST_TAG")
# load_person_has_interest_tag("PERSON_CENTER_HAS_INTEREST_TAG", "SocialNetwork/person_center_has_interest_tag.csv", "person_center_has_interest_tag_cluster")
# delete_edge_data("PERSON_SOUTH_HAS_INTEREST_TAG")
# load_person_has_interest_tag("PERSON_SOUTH_HAS_INTEREST_TAG", "SocialNetwork/person_south_has_interest_tag.csv", "person_south_has_interest_tag_cluster")



# # post_rids_map = generate_rid_map("Post", "POST_ID")
# person_rids_map = generate_rid_map("Person", "PERSON_ID")
# tag_rids_map = generate_rid_map("Tag", "TAG_ID")


# delete_edge_data("POST_HAS_TAG")
# load_post_has_tag("POST_HAS_TAG", "SocialNetwork/post_has_tag.csv", post_rids_map, tag_rids_map, "post_has_tag_global_cluster")

# delete_edge_data("POST_HAS_CREATOR_PERSON")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON", "SocialNetwork/post_has_creator_person.csv", post_rids_map, person_rids_map, "post_has_creator_person_global_cluster")
# delete_edge_data("POST_HAS_CREATOR_PERSON_NORTH")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON_NORTH", "SocialNetwork/post_has_creator_person_north.csv", post_rids_map, person_rids_map, "post_has_creator_person_north_cluster")

# delete_edge_data("POST_HAS_CREATOR_PERSON_CENTER")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON_CENTER", "SocialNetwork/post_has_creator_person_center.csv", post_rids_map, person_rids_map, "post_has_creator_person_center_cluster")
# delete_edge_data("POST_HAS_CREATOR_PERSON_SOUTH")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON_SOUTH", "SocialNetwork/post_has_creator_person_south.csv", post_rids_map, person_rids_map, "post_has_creator_person_south_cluster")
