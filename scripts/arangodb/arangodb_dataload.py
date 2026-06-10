import pandas as pd
from arango import ArangoClient
import os
import json
import xml.etree.ElementTree as ET
import numpy as np


from arango import ArangoClient

#  Configuración
ARANGO_URL = "http://192.168.50.101:8529"
USERNAME = "root"
PASSWORD = "root"

DATA_DIR = "../../Dataset/"

client = ArangoClient(hosts=ARANGO_URL)

try:
    db = client.db(DB_NAME, username=USERNAME, password=PASSWORD)
    if db.has_collection("POST"):
        print("Conexión establecida correctamente con ArangoDB.")
    else:
        print(" Conectado, pero la colección POST no existe.")
except Exception as e:
    print(f"Error al conectar con la base de datos: {e}")


def get_id_map(collection_name, keys):
    if not keys:
        return {}
    query = f"""
        FOR doc IN {collection_name}
            FILTER doc._key IN @keys
            RETURN {{ key: doc._key, id: doc._id }}
    """
    try:
        cursor = db.aql.execute(query, bind_vars={"keys": keys})
        return {doc["key"]: doc["id"] for doc in cursor}
    except Exception as e:
        print(f"Error precargando {collection_name}: {e}")
        return {}

def delete_data(collection_name):
    """Elimina todos los documentos de una colección."""
    try:
        collection = db.collection(collection_name)
        collection.truncate()
        print(f"Documentos de {collection_name} eliminados.")
    except Exception as e:
        print(f"Error al eliminar datos de {collection_name}: {e}")


def delete_edge_data(edge_collection_name):
    """Elimina todos los edges de una colección de tipo edge."""
    try:
        collection = db.collection(edge_collection_name)
        collection.truncate()
        print(f"Edges de {edge_collection_name} eliminados.")
    except Exception as e:
        print(f"Error al eliminar edges de {edge_collection_name}: {e}")


def insert_batch(collection_name, records, batch_size=5000):
    """Inserta documentos por lotes en una colección normal."""
    if not records:
        print(f" No hay registros para insertar en {collection_name}")
        return

    try:
        collection = db.collection(collection_name)
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            collection.insert_many(batch, overwrite=True)
            print(f"Insertado batch de {len(batch)} en {collection_name}")
    except Exception as e:
        print(f"Error al insertar en {collection_name}: {e}")

def insert_batch_orders(collection_name, records, batch_size=5000):
    """Inserta órdenes que contienen arreglos de referencias (ORDERLINE)."""
    if not records:
        return

    try:
        collection = db.collection(collection_name)
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            for r in batch:
                r["_key"] = r["ORDER_ID"]
            collection.insert_many(batch, overwrite=True)
            print(f"Insertado batch de {len(batch)} en {collection_name}")
    except Exception as e:
        print(f"Error en insert_batch_orders: {e}")

def insert_edge_batch(edge_collection_name, edges, batch_size=5000):
    """Inserta edges en lotes. Cada edge debe tener _from y _to (y opcionalmente otros campos)."""
    if not edges:
        return

    try:
        collection = db.collection(edge_collection_name)
        for i in range(0, len(edges), batch_size):
            batch = edges[i:i + batch_size]
            collection.insert_many(batch, overwrite=True)
            print(f"Insertado batch de {len(batch)} edges en {edge_collection_name}")
    except Exception as e:
        print(f"Error al insertar edges en {edge_collection_name}: {e}")



def load_customer_data( collection_name, file_name):
    """Carga datos en la colección CUSTOMER (o fragmentos), con validación básica."""
    file_path = os.path.join(DATA_DIR, file_name)
    print(f"Cargando {file_name} en {collection_name}...")

    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    try:
        df = pd.read_csv(file_path, sep="|", dtype=str, encoding="utf-8", on_bad_lines="skip")

        required_columns = ["ID", "FIRSTNAME", "LASTNAME", "PLACE"]
        for col in required_columns:
            if col not in df.columns:
                print(f"Falta la columna requerida: {col}")
                return

        df["PLACE"] = pd.to_numeric(df["PLACE"], errors="coerce")

        df["_key"] = df["ID"]

        # Insertar
        insert_batch(collection_name, df.to_dict(orient="records"))

    except Exception as e:
        print(f"Error al cargar {file_name} en {collection_name}: {e}")


def load_vendor_data(collection_name, file_name):
    """Carga datos en la colección VENDOR de ArangoDB."""
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {collection_name}...")
        return

    print(f"Cargando {file_path} en {collection_name}...")

    try:
        df = pd.read_csv(file_path, sep=",", dtype=str, quotechar='"', skipinitialspace=True, on_bad_lines="skip")

        # Validar columnas requeridas
        expected_columns = {"VENDOR_ID", "COMPANY", "COUNTRY", "INDUSTRY"}
        missing_columns = expected_columns - set(df.columns)
        if missing_columns:
            print(f"ERROR: Faltan las columnas {missing_columns} en el CSV de {collection_name}.")
            return

        # Asignar _key desde VENDOR_ID
        df["_key"] = df["VENDOR_ID"]

        # Insertar en ArangoDB
        insert_batch(collection_name, df.to_dict(orient="records"))
        print(f"Carga de {collection_name} completada.")

    except Exception as e:
        print(f"Error al cargar {collection_name}: {e}")

def load_product_data(collection_name, file_name):
    """Carga productos en ArangoDB, referenciando a Vendor por _id."""
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {collection_name}...")
        return

    print(f"Cargando {file_path} en {collection_name}...")

    try:
        df = pd.read_csv(file_path, sep=",", dtype=str, quotechar='"', skipinitialspace=True, on_bad_lines="skip")

        # Validar columnas necesarias
        expected_columns = {"PRODUCT_ID", "TITLE", "PRICE", "IMG_URL", "SKU", "VENDOR_ID"}
        missing_columns = expected_columns - set(df.columns)
        if missing_columns:
            print(f"ERROR: Faltan las columnas {missing_columns} en {file_name}.")
            return

        # Convertir PRICE a float
        df["PRICE"] = pd.to_numeric(df["PRICE"], errors="coerce")

        # Obtener mapeo VENDOR_ID → VENDOR/_key
        print(" Consultando VENDORs existentes...")
        vendor_collection = db.collection("VENDOR")
        vendor_docs = vendor_collection.all()
        vendor_map = {v["VENDOR_ID"]: f'VENDOR/{v["_key"]}' for v in vendor_docs}

        print(f"Total vendors en memoria: {len(vendor_map)}")

        # Reemplazar VENDOR_ID por _id
        df["VENDOR_ID"] = df["VENDOR_ID"].map(vendor_map)

        #  Filtrar registros inválidos
        df = df.dropna(subset=["VENDOR_ID", "PRICE"])
        df["_key"] = df["PRODUCT_ID"]

        # Insertar productos
        insert_batch(collection_name, df.to_dict(orient="records"))
        print(f"Carga de {collection_name} completada.")

    except Exception as e:
        print(f"Error al procesar {collection_name}: {e}")


def load_feedback_data(collection_name, file_name):
    """Carga feedback validando referencias a CUSTOMER y PRODUCT (versión optimizada)."""
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {collection_name}...")
        return

    print(f"Cargando {file_path} en {collection_name}...")

    batch_size = 10000
    total_inserted = 0
    invalid_records = []

    #  Precargar todos los _id de CUSTOMER y PRODUCT una vez
    print(" Precargando CUSTOMER y PRODUCT...")
    customer_map = {doc["_key"]: doc["_id"] for doc in db.collection("CUSTOMER").all()}
    product_map = {doc["_key"]: doc["_id"] for doc in db.collection("PRODUCT").all()}
    print(f"CUSTOMER cargados: {len(customer_map)} | PRODUCT cargados: {len(product_map)}")

    #  Procesar por lotes
    chunks = pd.read_csv(file_path, sep="|", dtype=str, quotechar='"', skipinitialspace=True, on_bad_lines="skip", chunksize=batch_size)

    for i, df in enumerate(chunks):
        print(f" Procesando lote {i+1} con {len(df)} registros...")

        batch_records = []
        for _, row in df.iterrows():
            cust_id = str(row["CUSTOMER_ID"]).strip()
            prod_id = str(row["PRODUCT_ID"]).strip()

            if cust_id in customer_map and prod_id in product_map:
                try:
                    record = {
                        "_key": f"{cust_id}_{prod_id}",
                        "CUSTOMER_ID": customer_map[cust_id],
                        "PRODUCT_ID": product_map[prod_id],
                        "RATE": float(row["RATE"]),
                        "REVIEW": row["REVIEW"].strip() if pd.notna(row["REVIEW"]) else ""
                    }
                    batch_records.append(record)
                except Exception:
                    invalid_records.append(row.to_dict())
            else:
                invalid_records.append(row.to_dict())

        insert_batch(collection_name, batch_records, batch_size=batch_size)
        total_inserted += len(batch_records)

    print(f"Carga de {collection_name} completada.")
    print(f" Registros insertados: {total_inserted}")
    print(f" Registros omitidos: {len(invalid_records)}")

    if invalid_records:
        print("Primeros registros descartados:")
        for rec in invalid_records[:10]:
            print(rec)

def load_order_data(collection_name, file_name):
    """Carga órdenes en ArangoDB validando referencias y transformando ORDERLINE."""
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {collection_name}...")
        return

    print(f"Cargando {file_path} en {collection_name}...")

    try:
        orders = []
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    order = json.loads(line.strip())
                    orders.append(order)
                except json.JSONDecodeError:
                    continue

        if not orders:
            print(" No se encontraron órdenes válidas.")
            return

        # IDs únicos
        customer_ids = {order["CUSTOMER_ID"] for order in orders}
        product_ids = {item["PRODUCT_ID"] for order in orders for item in order.get("ORDER_LINE", [])}

        #  Precargar CUSTOMER y PRODUCT
        customer_map = {doc["_key"]: doc["_id"] for doc in db.collection("CUSTOMER").all() if doc["_key"] in customer_ids}
        product_map = {doc["_key"]: doc["_id"] for doc in db.collection("PRODUCT").all() if doc["_key"] in product_ids}

        print(f"Customers válidos: {len(customer_map)} | Products válidos: {len(product_map)}")

        valid_records = []
        for order in orders:
            cust_id = order.get("CUSTOMER_ID")
            order_id = order.get("ORDER_ID")
            raw_date = order.get("ORDER_DATE")
            total_price = order.get("TOTAL_PRICE")
            raw_items = order.get("ORDER_LINE", [])

            # Validar CUSTOMER
            if cust_id not in customer_map:
                continue

            # Validar productos
            product_rids = [product_map.get(item.get("PRODUCT_ID")) for item in raw_items]
            product_rids = [rid for rid in product_rids if rid is not None]

            if not product_rids:
                continue

            try:
                parsed_date = pd.to_datetime(raw_date, errors="raise").strftime("%Y-%m-%d")
                total_price = float(total_price)
            except Exception as e:
                print(f" Fecha o precio inválido en ORDER_ID {order_id}: {e}")
                continue

            record = {
                "_key": order_id,
                "CUSTOMER_ID": customer_map[cust_id],
                "ORDER_DATE": parsed_date,
                "TOTAL_PRICE": total_price,
                "ORDERLINE": product_rids
            }
            valid_records.append(record)

        insert_batch(collection_name, valid_records)
        print(f"Total órdenes insertadas: {len(valid_records)}")

    except Exception as e:
        print(f"Error procesando órdenes: {e}")

def load_invoice_data(collection_name, file_name):
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {collection_name}...")
        return

    print(f"Cargando {file_path} en {collection_name}...")

    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        invoices_raw = []
        for invoice in root.findall("Invoice"):
            order_id   = (invoice.findtext("ORDER_ID") or "").strip()
            customer_id= (invoice.findtext("CUSTOMER_ID") or "").strip()
            order_date = (invoice.findtext("ORDER_DATE") or "").strip()
            total_price= (invoice.findtext("TOTAL_PRICE") or "").strip()

            if not order_id:
                continue

            product_ids = []
            for line in invoice.findall("ORDER_LINE"):
                pid = line.findtext("PRODUCT_ID")
                if pid: product_ids.append(pid.strip())

            invoices_raw.append({
                "ORDER_ID": order_id,          
                "CUSTOMER_ID": customer_id,
                "ORDER_DATE": order_date,
                "TOTAL_PRICE": total_price,
                "PRODUCT_IDS": product_ids
            })

        # Precarga referencias
        customer_ids = {inv["CUSTOMER_ID"] for inv in invoices_raw}
        product_ids  = {pid for inv in invoices_raw for pid in inv["PRODUCT_IDS"]}

        customer_map = {doc["_key"]: doc["_id"] for doc in db.collection("CUSTOMER").all() if doc["_key"] in customer_ids}
        product_map  = {doc["_key"]: doc["_id"] for doc in db.collection("PRODUCT").all() if doc["_key"] in product_ids}

        print(f"Clientes encontrados: {len(customer_map)} | Productos encontrados: {len(product_map)}")

        valid_records = []
        for inv in invoices_raw:
            oid  = inv["ORDER_ID"]
            cid  = inv["CUSTOMER_ID"]
            pids = [product_map.get(pid) for pid in inv["PRODUCT_IDS"] if pid in product_map]

            if cid not in customer_map or not pids:
                continue

            try:
                formatted_date = pd.to_datetime(inv["ORDER_DATE"], errors="raise").strftime("%Y-%m-%d")
                total_price = float(inv["TOTAL_PRICE"])
            except Exception:
                continue

            record = {
                "_key": oid,
                "ORDER_ID": oid,
                "CUSTOMER_ID": customer_map[cid],
                "ORDER_DATE": formatted_date,
                "TOTAL_PRICE": total_price,
                "ORDERLINE": pids
            }
            valid_records.append(record)

        insert_batch(collection_name, valid_records)
        print(f"Total facturas insertadas: {len(valid_records)}")

    except Exception as e:
        print(f"Error al procesar {collection_name}: {e}")


def load_person_data( collection_name, file_name):
    """Carga datos en la colección CUSTOMER (o fragmentos), con validación básica."""
    file_path = os.path.join(DATA_DIR, file_name)
    print(f"Cargando {file_name} en {collection_name}...")

    if not os.path.exists(file_path):
        print(f"Archivo no encontrado: {file_path}")
        return

    try:
        df = pd.read_csv(file_path, sep="|", dtype=str, encoding="utf-8", on_bad_lines="skip")

        # Validar columnas necesarias
        required_columns = ["ID", "FIRSTNAME", "LASTNAME", "PLACE"]
        for col in required_columns:
            if col not in df.columns:
                print(f"Falta la columna requerida: {col}")
                return

        # Asegurar que PLACE sea numérico
        df["PLACE"] = pd.to_numeric(df["PLACE"], errors="coerce")

        # Generar _key desde CUSTOMER_ID
        df["_key"] = df["ID"]

        # Insertar
        insert_batch(collection_name, df.to_dict(orient="records"))

    except Exception as e:
        print(f"Error al cargar {file_name} en {collection_name}: {e}")

def load_tag_data(collection_name, file_name):
    """Carga los datos de Tag en ArangoDB desde archivo CSV."""
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {collection_name}...")
        return

    print(f"Cargando {file_path} en {collection_name}...")

    try:
        df = pd.read_csv(
            file_path,
            sep="|",
            dtype=str,
            quotechar='"',
            skipinitialspace=True,
            on_bad_lines="skip"
        )

        df.rename(columns={"xID": "TAG_ID", "TITLE": "TITLE"}, inplace=True)

        # Validación
        if "TAG_ID" not in df.columns or "TITLE" not in df.columns:
            print(f"Columnas requeridas no encontradas en {file_name}")
            return

        df["TAG_ID"] = df["TAG_ID"].astype(str).str.strip()
        df["TITLE"] = df["TITLE"].astype(str).str.strip()
        df["_key"] = df["TAG_ID"]

        insert_batch(collection_name, df.to_dict(orient="records"))
        print(f"Carga de {collection_name} completada con {len(df)} registros.")
    except Exception as e:
        print(f"Error procesando {collection_name}: {e}")

def load_post_data(collection_name, file_name):
    """Carga los datos de Post en ArangoDB desde archivo CSV."""
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {collection_name}...")
        return

    print(f"Cargando {file_path} en {collection_name}...")

    try:
        df = pd.read_csv(
            file_path,
            sep="|",
            dtype=str,
            quotechar='"',
            skipinitialspace=True,
            on_bad_lines="skip"
        )

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
        df["IMAGE_FILE"] = df["IMAGE_FILE"].fillna("").astype(str)
        df["CREATE_DATE"] = pd.to_datetime(df["CREATE_DATE"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        df["LOCATION_IP"] = df["LOCATION_IP"].astype(str)
        df["BROWSER_USED"] = df["BROWSER_USED"].astype(str)
        df["LANGUAGE"] = df["LANGUAGE"].astype(str)
        df["CONTENT"] = df["CONTENT"].astype(str)
        df["LENGTH"] = pd.to_numeric(df["LENGTH"], errors="coerce").fillna(0).astype(int)
        df["_key"] = df["POST_ID"]

        insert_batch(collection_name, df.to_dict(orient="records"))
        print(f"Carga de {collection_name} completada con {len(df)} registros.")
    except Exception as e:
        print(f"Error procesando {collection_name}: {e}")


def load_customer_knows_person_edge(edge_collection, file_name):
    """Carga la relación CUSTOMER_KNOWS_PERSON como edges en ArangoDB."""
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {edge_collection}...")
        return

    print(f"Cargando {file_path} en {edge_collection}...")

    try:
        df = pd.read_csv(file_path, sep="|", dtype=str, on_bad_lines="skip")
        df.dropna(subset=["from", "to"], inplace=True)
        df["from"] = df["from"].astype(str).str.strip()
        df["to"] = df["to"].astype(str).str.strip()
        df["creationDate"] = pd.to_datetime(df["creationDate"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

        from_ids = df["from"].unique().tolist()
        to_ids = df["to"].unique().tolist()

        # Precargar CUSTOMER y PERSON
        customer_map = {doc["_key"]: doc["_id"] for doc in db.collection("CUSTOMER").all() if doc["_key"] in from_ids}
        person_map = {doc["_key"]: doc["_id"] for doc in db.collection("PERSON").all() if doc["_key"] in to_ids}

        print(f"CUSTOMER encontrados: {len(customer_map)}")
        print(f"PERSON encontrados: {len(person_map)}")

        edges = []
        for _, row in df.iterrows():
            from_id = row["from"]
            to_id = row["to"]
            date = row["creationDate"]

            if from_id in customer_map and to_id in person_map:
                edges.append({
                    "_from": customer_map[from_id],
                    "_to": person_map[to_id],
                    "creationDate": date
                })

        insert_edge_batch(edge_collection, edges)
        print(f"Carga de {edge_collection} completada con {len(edges)} relaciones.")

    except Exception as e:
        print(f"Error al cargar {edge_collection}: {e}")

def load_person_has_interest_tag(edge_collection, file_name):
    """Carga relaciones de PERSON_HAS_INTEREST_TAG como edges en ArangoDB."""
    file_path = os.path.join(DATA_DIR, file_name)

    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {edge_collection}...")
        return

    print(f"Cargando {file_path} en {edge_collection}...")

    try:
        df = pd.read_csv(file_path, sep="|", dtype=str, quotechar='"', skipinitialspace=True, on_bad_lines="skip")

        df["PERSON_ID"] = df["PERSON_ID"].astype(str).str.strip()
        df["TAG_ID"] = df["TAG_ID"].astype(str).str.strip()

        person_ids = df["PERSON_ID"].unique().tolist()
        tag_ids = df["TAG_ID"].unique().tolist()

        # Precargar PERSON y TAG
        person_map = {doc["_key"]: doc["_id"] for doc in db.collection("PERSON").all() if doc["_key"] in person_ids}
        tag_map = {doc["_key"]: doc["_id"] for doc in db.collection("TAG").all() if doc["_key"] in tag_ids}

        print(f"PERSON encontrados: {len(person_map)}")
        print(f"TAG encontrados: {len(tag_map)}")

        edges = []
        omitidos = []
        for _, row in df.iterrows():
            pid = row["PERSON_ID"]
            tid = row["TAG_ID"]
            if pid in person_map and tid in tag_map:
                edges.append({
                    "_from": person_map[pid],
                    "_to": tag_map[tid]
                })
            else:
                omitidos.append({"PERSON_ID": pid, "TAG_ID": tid})

        insert_edge_batch(edge_collection, edges)
        print(f"Carga de {edge_collection} completada con {len(edges)} relaciones.")
        print(f" Registros omitidos: {len(omitidos)}")

    except Exception as e:
        print(f"Error al cargar {edge_collection}: {e}")

def load_post_has_creator_person(edge_collection, file_name, chunksize=50000):
    """Carga edges POST_HAS_CREATOR_PERSON usando AQL para precarga parcial por chunk."""
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {edge_collection}...")
        return

    print(f"Cargando {file_path} en {edge_collection}...")

    total_inserted = 0
    chunk_idx = 1

    try:
        for chunk in pd.read_csv(file_path, sep="|", dtype=str, chunksize=chunksize):
            print(f"\n Procesando chunk #{chunk_idx}...")

            chunk["POST_ID"] = chunk["POST_ID"].astype(str).str.strip()
            chunk["PERSON_ID"] = chunk["PERSON_ID"].astype(str).str.strip()

            post_ids = chunk["POST_ID"].unique().tolist()
            person_ids = chunk["PERSON_ID"].unique().tolist()

            # Precarga eficiente con AQL
            post_id_map = get_id_map("POST", post_ids)
            person_id_map = get_id_map("PERSON", person_ids)

            edges = []
            for _, row in chunk.iterrows():
                from_id = person_id_map.get(row["PERSON_ID"])
                to_id = post_id_map.get(row["POST_ID"])
                if from_id and to_id:
                    edges.append({
                        "_from": from_id,
                        "_to": to_id
                    })

            insert_edge_batch(edge_collection, edges)
            inserted_count = len(edges)
            total_inserted += inserted_count
            print(f"Chunk #{chunk_idx} insertó {inserted_count}")

            chunk_idx += 1

        print(f"\nTotal de relaciones insertadas en {edge_collection}: {total_inserted}")

    except Exception as e:
        print(f"Error procesando {edge_collection}: {e}")



def load_post_has_tag(edge_collection, file_name, chunksize=70000):
    """Carga edges POST_HAS_TAG usando precarga AQL por chunk."""
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f" Archivo {file_path} no encontrado. Saltando {edge_collection}...")
        return

    print(f"Cargando {file_path} en {edge_collection}...")

    total_inserted = 0
    chunk_num = 1

    try:
        for chunk in pd.read_csv(file_path, sep="|", dtype=str, chunksize=chunksize):
            print(f"\n Procesando chunk #{chunk_num}...")

            chunk["POST_ID"] = chunk["POST_ID"].astype(str).str.strip()
            chunk["TAG_ID"] = chunk["TAG_ID"].astype(str).str.strip()

            post_ids = chunk["POST_ID"].unique().tolist()
            tag_ids = chunk["TAG_ID"].unique().tolist()

            post_id_map = get_id_map("POST", post_ids)
            tag_id_map = get_id_map("TAG", tag_ids)

            edges = []
            for _, row in chunk.iterrows():
                from_id = post_id_map.get(row["POST_ID"])
                to_id = tag_id_map.get(row["TAG_ID"])
                if from_id and to_id:
                    edges.append({
                        "_from": from_id,
                        "_to": to_id
                    })

            insert_edge_batch(edge_collection, edges)
            print(f"Chunk #{chunk_num} insertó {len(edges)} edges ")
            total_inserted += len(edges)
            chunk_num += 1

        print(f"\nTotal de relaciones insertadas en {edge_collection}: {total_inserted}")

    except Exception as e:
        print(f"Error procesando {edge_collection}: {e}")

# load_customer_data("CUSTOMER", "Customer/person_0_0.csv")
# load_customer_data("CUSTOMER_NORTH", "Customer/person_0_0_north.csv")
# load_customer_data("CUSTOMER_CENTER", "Customer/person_0_0_center.csv")
# load_customer_data("CUSTOMER_SOUTH", "Customer/person_0_0_south.csv")

# load_vendor_data("VENDOR", "Vendor/Vendor.csv")

# load_product_data("PRODUCT", "Product/Product.csv")
# load_product_data("PRODUCT_CHEAP", "Product/Product_Cheap.csv")
# load_product_data("PRODUCT_EXPENSIVE", "Product/Product_Expensive.csv")

# load_feedback_data("FEEDBACK", "Feedback/Feedback.csv")

# load_order_data("ORDER", "Order/Order.json")
# load_order_data("ORDER_POST_PANDEMIC", "Order/Order_Post_Pandemic.json")
# load_order_data("ORDER_PRE_PANDEMIC", "Order/Order_Pre_Pandemic.json")

#load_invoice_data("INVOICE", "Invoice/Invoice.xml")


# load_person_data("PERSON", "Customer/person_0_0.csv")
# load_person_data("PERSON_NORTH", "Customer/person_0_0_north.csv")
# load_person_data("PERSON_CENTER", "Customer/person_0_0_center.csv")
# load_person_data("PERSON_SOUTH", "Customer/person_0_0_south.csv")

# load_tag_data("TAG", "SocialNetwork/Tag.csv")

# load_post_data("POST", "SocialNetwork/Post.csv")

# load_customer_knows_person_edge("CUSTOMER_KNOWS_PERSON", "SocialNetwork/person_knows_person.csv")

# load_person_has_interest_tag("PERSON_HAS_INTEREST_TAG", "SocialNetwork/person_has_interest_tag.csv")
# load_person_has_interest_tag("PERSON_NORTH_HAS_INTEREST_TAG", "SocialNetwork/person_north_has_interest_tag.csv")
# load_person_has_interest_tag("PERSON_CENTER_HAS_INTEREST_TAG", "SocialNetwork/person_center_has_interest_tag.csv")
# load_person_has_interest_tag("PERSON_SOUTH_HAS_INTEREST_TAG", "SocialNetwork/person_south_has_interest_tag.csv")

# load_post_has_creator_person("POST_HAS_CREATOR_PERSON", "SocialNetwork/post_has_creator_person.csv")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON_NORTH", "SocialNetwork/post_has_creator_person_north.csv")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON_CENTER", "SocialNetwork/post_has_creator_person_center.csv")
# load_post_has_creator_person("POST_HAS_CREATOR_PERSON_SOUTH", "SocialNetwork/post_has_creator_person_south.csv")

# load_post_has_tag("POST_HAS_TAG", "SocialNetwork/post_has_tag.csv")