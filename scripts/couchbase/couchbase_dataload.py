import os
import pandas as pd
import json
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.exceptions import CouchbaseException
import xml.etree.ElementTree as ET

# Configuración general
CB_BUCKET = "KhaBench"
CB_SCOPE = "KhaBench"
CB_USERNAME = "Administrator"
CB_PASSWORD = "rootpwd"
DATA_DIR = "../../Dataset/"  # Ruta local en tu Mac

def delete_data(collection_name, node_ip):
    print(f"🚮 Conectando a nodo {node_ip} para borrar la colección '{collection_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        print(f"Conectado a {node_ip}")
    except Exception as e:
        print(f"Error de conexión: {e}")
        return

    try:
        query = f"DELETE FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`{collection_name}`"
        result = cluster.query(query)
        count = 0
        for _ in result:
            count += 1
        print(f"Se eliminaron los documentos de '{collection_name}' desde el nodo {node_ip}")
    except Exception as e:
        print(f"Error al eliminar documentos de '{collection_name}': {e}")

def load_vendor_data(collection_name, file_name, node_ip):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    print(f"Leyendo archivo {file_path}...")
    df = pd.read_csv(file_path, sep=",", dtype=str, quotechar='"', skipinitialspace=True, on_bad_lines="skip")

    required_columns = {"VENDOR_ID", "COMPANY", "COUNTRY", "INDUSTRY"}
    if not required_columns.issubset(set(df.columns)):
        print(f"Faltan columnas requeridas: {required_columns - set(df.columns)}")
        return

    df["VENDOR_ID"] = df["VENDOR_ID"].astype(str).str.strip()
    records = df.to_dict(orient="records")

    inserted = 0
    for record in records:
        doc_id = record["VENDOR_ID"]
        try:
            cb_collection.upsert(doc_id, record)
            inserted += 1
        except CouchbaseException as e:
            print(f" Error insertando {doc_id}: {e}")

    print(f"Se insertaron {inserted} documentos en la colección '{collection_name}' usando el nodo {node_ip}")

from concurrent.futures import ThreadPoolExecutor, as_completed

def load_customer_data(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    print(f"Leyendo archivo {file_path}...")
    df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")

    required_columns = {"ID", "FIRSTNAME", "LASTNAME", "PLACE"}
    if not required_columns.issubset(set(df.columns)):
        print(f"Faltan columnas requeridas: {required_columns - set(df.columns)}")
        return

    df["PLACE"] = pd.to_numeric(df["PLACE"], errors="coerce")
    df["CUSTOMER_ID"] = df["ID"].astype(str).str.strip()

    records = df.to_dict(orient="records")
    total_inserted = 0

    def insert_doc(doc):
        try:
            cb_collection.upsert(doc["CUSTOMER_ID"], doc)
            return 1
        except Exception as e:
            print(f" Error en {doc['CUSTOMER_ID']}: {e}")
            return 0

    print(f" Iniciando inserción paralela con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            total_inserted += future.result()

    print(f"Se insertaron {total_inserted} documentos en '{collection_name}' desde nodo {node_ip}")

def load_product_data(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        vendor_collection = bucket.scope(CB_SCOPE).collection("VENDOR")
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    print(f"Leyendo archivo {file_path}...")
    df = pd.read_csv(file_path, sep=",", dtype=str, quotechar='"', skipinitialspace=True, on_bad_lines="skip")

    required_columns = {"PRODUCT_ID", "TITLE", "PRICE", "IMG_URL", "SKU", "VENDOR_ID"}
    if not required_columns.issubset(set(df.columns)):
        print(f"Faltan columnas requeridas: {required_columns - set(df.columns)}")
        return

    df["PRODUCT_ID"] = df["PRODUCT_ID"].astype(str).str.strip()
    df["VENDOR_ID"] = df["VENDOR_ID"].astype(str).str.strip()
    df["PRICE"] = pd.to_numeric(df["PRICE"], errors="coerce")

    #  Obtener vendor IDs válidos
    print(" Precargando Vendor IDs...")
    try:
        query = f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`VENDOR`"
        result = cluster.query(query)
        valid_vendor_ids = set(row["id"] for row in result)
    except Exception as e:
        print(f"Error al obtener vendors: {e}")
        return

    print(f"Vendors válidos encontrados: {len(valid_vendor_ids)}")

    # Filtrar productos con vendor inválido
    df = df[df["VENDOR_ID"].isin(valid_vendor_ids)]

    records = df.to_dict(orient="records")
    total_inserted = 0

    def insert_doc(doc):
        try:
            cb_collection.upsert(doc["PRODUCT_ID"], doc)
            return 1
        except Exception as e:
            print(f" Error en {doc['PRODUCT_ID']}: {e}")
            return 0

    print(f" Iniciando inserción paralela con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            total_inserted += future.result()

    print(f"Se insertaron {total_inserted} documentos en '{collection_name}' desde nodo {node_ip}")

def load_feedback_data(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    print(f"Leyendo archivo {file_path}...")
    df = pd.read_csv(file_path, sep="|", dtype=str, quotechar='"', skipinitialspace=True, on_bad_lines="skip")

    required_columns = {"CUSTOMER_ID", "PRODUCT_ID", "RATE", "REVIEW"}
    if not required_columns.issubset(set(df.columns)):
        print(f"Faltan columnas requeridas: {required_columns - set(df.columns)}")
        return

    df["CUSTOMER_ID"] = df["CUSTOMER_ID"].astype(str).str.strip()
    df["PRODUCT_ID"] = df["PRODUCT_ID"].astype(str).str.strip()

    print(" Precargando CUSTOMER_IDs...")
    try:
        customer_query = f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`CUSTOMER`"
        customer_result = cluster.query(customer_query)
        valid_customers = set(row["id"] for row in customer_result)
    except Exception as e:
        print(f"Error al obtener customers: {e}")
        return

    #  Obtener PRODUCT_IDs válidos
    print(" Precargando PRODUCT_IDs...")
    try:
        product_query = f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`PRODUCT`"
        product_result = cluster.query(product_query)
        valid_products = set(row["id"] for row in product_result)
    except Exception as e:
        print(f"Error al obtener products: {e}")
        return

    print(f"Customers válidos: {len(valid_customers)}, Products válidos: {len(valid_products)}")

    # Filtrar registros válidos
    df = df[df["CUSTOMER_ID"].isin(valid_customers) & df["PRODUCT_ID"].isin(valid_products)]

    records = df.to_dict(orient="records")
    inserted = 0

    def insert_doc(doc):
        try:
            doc_id = f"{doc['CUSTOMER_ID']}_{doc['PRODUCT_ID']}"
            cb_collection.upsert(doc_id, doc)
            return 1
        except Exception as e:
            print(f" Error en {doc_id}: {e}")
            return 0

    print(f" Iniciando inserción paralela con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            inserted += future.result()

    print(f"Se insertaron {inserted} documentos en '{collection_name}' desde nodo {node_ip}")


def load_order_data(collection_name, file_name, node_ip, max_workers=10, batch_size=500):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    print(f"Leyendo archivo {file_path}...")
    orders = []
    with open(file_path, "r") as f:
        for line in f:
            try:
                order = json.loads(line.strip())
                orders.append(order)
            except json.JSONDecodeError:
                continue

    print(f" Total de órdenes en archivo: {len(orders)}")

    all_customer_ids = {order["CUSTOMER_ID"] for order in orders}
    all_product_ids = {item["PRODUCT_ID"] for order in orders for item in order.get("ORDER_LINE", [])}

    print(" Precargando CUSTOMER_IDs...")
    try:
        customer_query = f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`CUSTOMER`"
        customer_result = cluster.query(customer_query)
        valid_customers = set(row["id"] for row in customer_result)
    except Exception as e:
        print(f"Error al obtener customers: {e}")
        return

    print(" Precargando PRODUCT_IDs...")
    try:
        product_query = f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`PRODUCT`"
        product_result = cluster.query(product_query)
        valid_products = set(row["id"] for row in product_result)
    except Exception as e:
        print(f"Error al obtener products: {e}")
        return

    print(f"Customers válidos: {len(valid_customers)}, Products válidos: {len(valid_products)}")

    records = []
    for order in orders:
        cust_id = order.get("CUSTOMER_ID")
        if cust_id not in valid_customers:
            continue

        orderline = []
        for item in order.get("ORDER_LINE", []):
            pid = item.get("PRODUCT_ID")
            if pid in valid_products:
                orderline.append(pid)

        if not orderline:
            continue

        try:
            order_date = pd.to_datetime(order["ORDER_DATE"], errors="raise").strftime("%Y-%m-%d")
        except Exception:
            continue

        record = {
            "ORDER_ID": order["ORDER_ID"],
            "CUSTOMER_ID": cust_id,
            "ORDER_DATE": order_date,
            "TOTAL_PRICE": order["TOTAL_PRICE"],
            "ORDERLINE": orderline
        }
        records.append(record)

    print(f" Órdenes válidas a insertar: {len(records)}")

    inserted = 0
    def insert_doc(doc):
        try:
            cb_collection.upsert(doc["ORDER_ID"], doc)
            return 1
        except Exception as e:
            print(f" Error insertando {doc['ORDER_ID']}: {e}")
            return 0

    print(f" Insertando en paralelo con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            inserted += future.result()

    print(f"Se insertaron {inserted} documentos en '{collection_name}' desde nodo {node_ip}")


def load_invoice_data(collection_name, file_name, node_ip, max_workers=10, batch_size=500):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    print(f"Leyendo archivo XML {file_path}...")
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
    except Exception as e:
        print(f"Error leyendo XML: {e}")
        return

    # Precargar claves válidas
    print(" Precargando CUSTOMER_IDs y PRODUCT_IDs...")
    try:
        customer_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`CUSTOMER`")
        valid_customers = set(row["id"] for row in customer_result)

        product_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`PRODUCT`")
        valid_products = set(row["id"] for row in product_result)
    except Exception as e:
        print(f"Error al precargar referencias: {e}")
        return

    records = []
    for invoice in root.findall("Invoice"):
        order_id = invoice.findtext("ORDER_ID")
        cust_id = invoice.findtext("CUSTOMER_ID")
        order_date = invoice.findtext("ORDER_DATE")
        total_price = invoice.findtext("TOTAL_PRICE")

        if cust_id not in valid_customers:
            continue

        orderline_nodes = invoice.findall("ORDER_LINE")
        product_ids = [n.findtext("PRODUCT_ID") for n in orderline_nodes if n.findtext("PRODUCT_ID") in valid_products]

        if not product_ids:
            continue

        try:
            order_date_fmt = pd.to_datetime(order_date, errors="raise").strftime("%Y-%m-%d")
        except Exception:
            continue

        doc = {
            "ORDER_ID": order_id,
            "CUSTOMER_ID": cust_id,
            "ORDER_DATE": order_date_fmt,
            "TOTAL_PRICE": float(total_price),
            "ORDERLINE": product_ids
        }
        records.append(doc)

    print(f" Facturas válidas a insertar: {len(records)}")
    inserted = 0

    def insert_doc(doc):
        try:
            cb_collection.upsert(doc["ORDER_ID"], doc)
            return 1
        except Exception as e:
            print(f" Error en {doc['ORDER_ID']}: {e}")
            return 0

    print(f" Insertando en paralelo con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            inserted += future.result()

    print(f"Se insertaron {inserted} facturas en '{collection_name}' desde nodo {node_ip}")

def load_person_data(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    print(f"Leyendo archivo {file_path}...")
    df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")

    required_columns = {"ID", "FIRSTNAME", "LASTNAME", "PLACE"}
    if not required_columns.issubset(set(df.columns)):
        print(f"Faltan columnas requeridas: {required_columns - set(df.columns)}")
        return

    df["PLACE"] = pd.to_numeric(df["PLACE"], errors="coerce")
    df["CUSTOMER_ID"] = df["ID"].astype(str).str.strip()

    records = df.to_dict(orient="records")
    total_inserted = 0

    def insert_doc(doc):
        try:
            cb_collection.upsert(doc["CUSTOMER_ID"], doc)
            return 1
        except Exception as e:
            print(f" Error en {doc['CUSTOMER_ID']}: {e}")
            return 0

    print(f" Iniciando inserción paralela con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            total_inserted += future.result()

    print(f"Se insertaron {total_inserted} documentos en '{collection_name}' desde nodo {node_ip}")

def load_tag_data(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")
    df.rename(columns={"xID": "TAG_ID", "TITLE": "TITLE"}, inplace=True)

    df["TAG_ID"] = df["TAG_ID"].astype(str).str.strip()
    df["TITLE"] = df["TITLE"].astype(str).str.strip()

    records = df.to_dict(orient="records")
    inserted = 0

    def insert_doc(doc):
        try:
            cb_collection.upsert(doc["TAG_ID"], doc)
            return 1
        except Exception as e:
            print(f" Error en {doc['TAG_ID']}: {e}")
            return 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            inserted += future.result()

    print(f"Se insertaron {inserted} documentos en '{collection_name}' desde nodo {node_ip}")

def load_post_data(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")

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

    df["POST_ID"] = df["POST_ID"].astype(str).str.strip()
    df["CREATE_DATE"] = pd.to_datetime(df["CREATE_DATE"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df["LENGTH"] = pd.to_numeric(df["LENGTH"], errors="coerce").fillna(0).astype(int)

    records = df.to_dict(orient="records")
    inserted = 0

    def insert_doc(doc):
        try:
            cb_collection.upsert(doc["POST_ID"], doc)
            return 1
        except Exception as e:
            print(f" Error en {doc['POST_ID']}: {e}")
            return 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            inserted += future.result()

    print(f"Se insertaron {inserted} documentos en '{collection_name}' desde nodo {node_ip}")

def load_customer_knows_person(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")
    df.dropna(subset=["from", "to"], inplace=True)

    df["from"] = df["from"].astype(str).str.strip()
    df["to"] = df["to"].astype(str).str.strip()
    df["creationDate"] = pd.to_datetime(df["creationDate"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    print(" Precargando CUSTOMER_IDs y PERSON_IDs...")

    try:
        customer_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`CUSTOMER`")
        valid_customers = set(row["id"] for row in customer_result)

        person_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`PERSON`")
        valid_persons = set(row["id"] for row in person_result)
    except Exception as e:
        print(f"Error al precargar referencias: {e}")
        return

    print(f"Clientes válidos: {len(valid_customers)}, Personas válidas: {len(valid_persons)}")

    edges = []
    for _, row in df.iterrows():
        f = row["from"]
        t = row["to"]
        if f in valid_customers and t in valid_persons:
            edges.append({
                "_from": f,
                "_to": t,
                "creationDate": row["creationDate"]
            })

    print(f" Relaciones válidas a insertar: {len(edges)}")
    inserted = 0

    def insert_doc(doc):
        try:
            edge_id = f"{doc['_from']}_{doc['_to']}"
            cb_collection.upsert(edge_id, doc)
            return 1
        except Exception as e:
            print(f" Error en edge {doc['_from']} → {doc['_to']}: {e}")
            return 0

    print(f" Insertando en paralelo con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(edges), batch_size):
            batch = edges[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            inserted += future.result()

    print(f"Se insertaron {inserted} edges en '{collection_name}' desde nodo {node_ip}")

def load_post_has_creator_person(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")
    df.dropna(subset=["POST_ID", "PERSON_ID"], inplace=True)

    df["POST_ID"] = df["POST_ID"].astype(str).str.strip()
    df["PERSON_ID"] = df["PERSON_ID"].astype(str).str.strip()

    print(" Precargando POST_IDs y PERSON_IDs...")

    try:
        post_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`POST`")
        valid_posts = set(row["id"] for row in post_result)

        person_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`PERSON`")
        valid_persons = set(row["id"] for row in person_result)
    except Exception as e:
        print(f"Error al precargar referencias: {e}")
        return

    print(f"Posts válidos: {len(valid_posts)}, Personas válidas: {len(valid_persons)}")

    edges = []
    for _, row in df.iterrows():
        f = row["PERSON_ID"]
        t = row["POST_ID"]
        if f in valid_persons and t in valid_posts:
            edges.append({
                "_from": f,
                "_to": t
            })

    print(f" Relaciones válidas a insertar: {len(edges)}")
    inserted = 0

    def insert_doc(doc):
        try:
            edge_id = f"{doc['_from']}_{doc['_to']}"
            cb_collection.upsert(edge_id, doc)
            return 1
        except Exception as e:
            print(f" Error en edge {doc['_from']} → {doc['_to']}: {e}")
            return 0

    print(f" Insertando en paralelo con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(edges), batch_size):
            batch = edges[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            inserted += future.result()

    print(f"Se insertaron {inserted} edges en '{collection_name}' desde nodo {node_ip}")

def load_person_has_interest_tag(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")
    df.dropna(subset=["PERSON_ID", "TAG_ID"], inplace=True)
    df["PERSON_ID"] = df["PERSON_ID"].astype(str).str.strip()
    df["TAG_ID"] = df["TAG_ID"].astype(str).str.strip()

    print(" Precargando PERSON_IDs y TAG_IDs...")

    try:
        person_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`PERSON`")
        valid_persons = set(row["id"] for row in person_result)

        tag_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`TAG`")
        valid_tags = set(row["id"] for row in tag_result)
    except Exception as e:
        print(f"Error al precargar referencias: {e}")
        return

    print(f"Personas válidas: {len(valid_persons)}, Tags válidos: {len(valid_tags)}")

    edges = []
    for _, row in df.iterrows():
        f = row["PERSON_ID"]
        t = row["TAG_ID"]
        if f in valid_persons and t in valid_tags:
            edges.append({
                "_from": f,
                "_to": t
            })

    print(f" Relaciones válidas a insertar: {len(edges)}")
    inserted = 0

    def insert_doc(doc):
        try:
            edge_id = f"{doc['_from']}_{doc['_to']}"
            cb_collection.upsert(edge_id, doc)
            return 1
        except Exception as e:
            print(f" Error en edge {doc['_from']} → {doc['_to']}: {e}")
            return 0

    print(f" Insertando en paralelo con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(edges), batch_size):
            batch = edges[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            inserted += future.result()

    print(f"Se insertaron {inserted} edges en '{collection_name}' desde nodo {node_ip}")

def load_post_has_tag(collection_name, file_name, node_ip, max_workers=10, batch_size=1000):
    print(f" Conectando a nodo {node_ip} para cargar '{collection_name}' desde '{file_name}'...")

    try:
        cluster = Cluster(f"couchbase://{node_ip}", ClusterOptions(
            PasswordAuthenticator(CB_USERNAME, CB_PASSWORD)))
        bucket = cluster.bucket(CB_BUCKET)
        cb_collection = bucket.scope(CB_SCOPE).collection(collection_name)
        print(f"Conectado correctamente a {node_ip}")
    except Exception as e:
        print(f"Error conectando al nodo {node_ip}: {e}")
        return

    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")
    df.dropna(subset=["POST_ID", "TAG_ID"], inplace=True)
    df["POST_ID"] = df["POST_ID"].astype(str).str.strip()
    df["TAG_ID"] = df["TAG_ID"].astype(str).str.strip()

    print(" Precargando POST_IDs y TAG_IDs...")

    try:
        post_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`POST`")
        valid_posts = set(row["id"] for row in post_result)

        tag_result = cluster.query(f"SELECT META().id FROM `{CB_BUCKET}`.`{CB_SCOPE}`.`TAG`")
        valid_tags = set(row["id"] for row in tag_result)
    except Exception as e:
        print(f"Error al precargar referencias: {e}")
        return

    print(f"Posts válidos: {len(valid_posts)}, Tags válidos: {len(valid_tags)}")

    edges = []
    for _, row in df.iterrows():
        f = row["POST_ID"]
        t = row["TAG_ID"]
        if f in valid_posts and t in valid_tags:
            edges.append({
                "_from": f,
                "_to": t
            })

    print(f" Relaciones válidas a insertar: {len(edges)}")
    inserted = 0

    def insert_doc(doc):
        try:
            edge_id = f"{doc['_from']}_{doc['_to']}"
            cb_collection.upsert(edge_id, doc)
            return 1
        except Exception as e:
            print(f" Error en edge {doc['_from']} → {doc['_to']}: {e}")
            return 0

    print(f" Insertando en paralelo con {max_workers} hilos...")
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, len(edges), batch_size):
            batch = edges[i:i + batch_size]
            futures.extend([executor.submit(insert_doc, doc) for doc in batch])

        for future in as_completed(futures):
            inserted += future.result()

    print(f"Se insertaron {inserted} edges en '{collection_name}' desde nodo {node_ip}")

load_vendor_data("VENDOR", "Vendor/Vendor.csv", "192.168.50.104")

load_customer_data("CUSTOMER", "Customer/person_0_0.csv", "192.168.50.104")            # Nodo 4
load_customer_data("CUSTOMER_NORTH", "Customer/person_0_0_north.csv", "192.168.50.101")  # Nodo 1
load_customer_data("CUSTOMER_CENTER", "Customer/person_0_0_center.csv", "192.168.50.102")# Nodo 2
load_customer_data("CUSTOMER_SOUTH", "Customer/person_0_0_south.csv", "192.168.50.103") # Nodo 3

load_product_data("PRODUCT", "Product/Product.csv", "192.168.50.104")  # Nodo 4
load_product_data("PRODUCT_CHEAP", "Product/Product_Cheap.csv", "192.168.50.101")  # Nodo 1
load_product_data("PRODUCT_EXPENSIVE", "Product/product_Expensive.csv", "192.168.50.103")  # Nodo 1

load_feedback_data("FEEDBACK", "Feedback/Feedback.csv", "192.168.50.104")

load_order_data("ORDER", "Order/Order.json", "192.168.50.104")  # Nodo 4
load_order_data("ORDER_PRE_PANDEMIC", "Order/Order_Pre_Pandemic.json", "192.168.50.102")  # Nodo 1
load_order_data("ORDER_POST_PANDEMIC", "Order/Order_Post_Pandemic.json", "192.168.50.103")  # Nodo 2

load_invoice_data("INVOICE", "Invoice/Invoice.xml", "192.168.50.104")

load_person_data("PERSON", "Customer/person_0_0.csv", "192.168.50.104")            # Nodo 4
load_person_data("PERSON_NORTH", "Customer/person_0_0_north.csv", "192.168.50.101")  # Nodo 1
load_person_data("PERSON_CENTER", "Customer/person_0_0_center.csv", "192.168.50.102")# Nodo 2
load_person_data("PERSON_SOUTH", "Customer/person_0_0_south.csv", "192.168.50.103") # Nodo 3

# load_tag_data("TAG", "SocialNetwork/Tag.csv", "192.168.50.104")      # Nodo 1

# load_post_data("POST", "SocialNetwork/Post.csv", "192.168.50.104")    # Nodo 4

# load_customer_knows_person("CUSTOMER_KNOWS_PERSON", "SocialNetwork/person_knows_person.csv", "192.168.50.104")

# load_post_has_tag("POST_HAS_TAG", "SocialNetwork/post_has_tag.csv", "192.168.50.104")

# load_post_has_creator_person("POST_HAS_CREATOR_PERSON", "SocialNetwork/post_has_creator_person.csv", "192.168.50.104")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON_NORTH", "SocialNetwork/post_has_creator_person_north.csv", "192.168.50.101")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON_CENTER", "SocialNetwork/post_has_creator_person_center.csv", "192.168.50.102")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON_SOUTH", "SocialNetwork/post_has_creator_person_south.csv", "192.168.50.103")

# # load_person_has_interest_tag("PERSON_HAS_INTEREST_TAG", "SocialNetwork/person_has_interest_tag.csv", "192.168.50.104")
# load_person_has_interest_tag("PERSON_NORTH_HAS_INTEREST_TAG", "SocialNetwork/person_north_has_interest_tag.csv", "192.168.50.101")
# load_person_has_interest_tag("PERSON_CENTER_HAS_INTEREST_TAG", "SocialNetwork/person_center_has_interest_tag.csv", "192.168.50.102")
# load_person_has_interest_tag("PERSON_SOUTH_HAS_INTEREST_TAG", "SocialNetwork/person_south_has_interest_tag.csv", "192.168.50.103")

