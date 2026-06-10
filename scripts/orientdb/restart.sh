package transactions.tests;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.TransactionOptions;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class T3_TestDemo {

    private static final String ORIENT_HOST = "192.168.50.101";
    private static final String ORIENT_DB   = "KhaBench";
    private static final String ORIENT_USER = "root";
    private static final String ORIENT_PWD  = "rootpwd";

    private static final String ARANGO_HOST = "192.168.50.101";
    private static final String ARANGO_DB   = "KhaBench";

    // Cambia aquí el ORDER_ID a operar
    private static final String ORDER_ID    = "7d20b3b1-1aa6-43c8-b847-914b7976914b";

    public static void main(String[] args) {
        System.out.println("=== T3: Borrar en Orient (VERTEX) y nulificar INVOICE en Arango (con ANTES/DESPUÉS) ===");

        OrientDB orient = null;
        ODatabaseSession oSess = null;
        ArangoDB adb = null;
        ArangoDatabase aDb = null;

        boolean orientOk = false;
        boolean arangoOk = false;

        try {
            // -------- ORIENT: conexión + TX --------
            orient = new OrientDB("remote:" + ORIENT_HOST, OrientDBConfig.defaultConfig());
            oSess = orient.open(ORIENT_DB, ORIENT_USER, ORIENT_PWD);
            oSess.begin();

            // (Opcional) mostrar cuántos hay antes en Orient
            long antesOrient = 0;
            try (OResultSet rs = oSess.query(
                    "SELECT COUNT(*) AS c FROM ORDER_PRE_PANDEMIC WHERE ORDER_ID = ?", ORDER_ID)) {
                if (rs.hasNext()) {
                    Object c = rs.next().getProperty("c");
                    if (c instanceof Number) antesOrient = ((Number) c).longValue();
                }
            }
            System.out.println("[Orient] COUNT antes: " + antesOrient);

            // 1) BORRAR EN ORIENT (VERTEX)
            String delSql = "DELETE VERTEX ORDER_PRE_PANDEMIC WHERE ORDER_ID = ?";
            oSess.command(delSql, ORDER_ID);
            // Verificación rápida
            long despuesOrient = 0;
            try (OResultSet rs = oSess.query(
                    "SELECT COUNT(*) AS c FROM ORDER_PRE_PANDEMIC WHERE ORDER_ID = ?", ORDER_ID)) {
                if (rs.hasNext()) {
                    Object c = rs.next().getProperty("c");
                    if (c instanceof Number) despuesOrient = ((Number) c).longValue();
                }
            }
            System.out.println("[Orient] COUNT después: " + despuesOrient);
            if (despuesOrient != 0) throw new RuntimeException("No se pudo borrar la orden en OrientDB");
            orientOk = true;
            System.out.println("✔ Orden borrada en OrientDB");

            // -------- ARANGO: conexión --------
            adb = new ArangoDB.Builder().host(ARANGO_HOST, 8529).build();
            aDb = adb.db(ARANGO_DB);

            // AQL común
            final String AQL_FIND_INVOICES = "FOR i IN INVOICE FILTER i.OrderId == @id RETURN i";
            Map<String, Object> bind = Map.of("id", ORDER_ID);

            // --- ANTES ---
            System.out.println(" Invoices ANTES:");
            List<BaseDocument> antes = new ArrayList<>();
            try (ArangoCursor<BaseDocument> cur = aDb.query(
                    AQL_FIND_INVOICES, BaseDocument.class, bind, new AqlQueryOptions())) {
                while (cur.hasNext()) {
                    BaseDocument d = cur.next();
                    antes.add(d);
                    System.out.println(" - _key=" + d.getKey() + ", OrderId=" + d.getAttribute("OrderId"));
                }
            }

            // 2) NULIFICAR EN ARANGO (transacción JS)
            String action = """
                function (params) {
                  const db = require('@arangodb').db;
                  const col = db._collection("INVOICE");
                  if (!col) throw new Error("No existe colección INVOICE");
                  const cursor = db._query(
                    "FOR i IN INVOICE FILTER i.OrderId == @id RETURN i",
                    { id: params.id }
                  );
                  let n = 0;
                  while (cursor.hasNext()) {
                    const doc = cursor.next();
                    col.update(doc._key, { OrderId: null, STATUS: "UNLINKED_SIMPLE_TX" });
                    n++;
                  }
                  return n;
                }
                """;

            TransactionOptions opts = new TransactionOptions()
                    .readCollections("INVOICE")
                    .writeCollections("INVOICE")
                    .params(Map.of("id", ORDER_ID));

            Integer touched = aDb.transaction(action, Integer.class, opts);
            System.out.println("✔ Invoices nulificados en ArangoDB: " + (touched != null ? touched : 0));
            arangoOk = true;

            // --- DESPUÉS ---
            System.out.println(" Invoices DESPUÉS:");
            List<BaseDocument> despues = new ArrayList<>();
            try (ArangoCursor<BaseDocument> cur = aDb.query(
                    AQL_FIND_INVOICES, BaseDocument.class, bind, new AqlQueryOptions())) {
                while (cur.hasNext()) {
                    BaseDocument d = cur.next();
                    despues.add(d);
                    System.out.println(" - _key=" + d.getKey() + ", OrderId=" + d.getAttribute("OrderId"));
                }
            }
            if (despues.isEmpty()) {
                System.out.println(" - (sin registros con ese OrderId)");
            }

            // -------- COMMIT GLOBAL --------
            if (orientOk && arangoOk) {
                oSess.commit();
                System.out.println("Commit GLOBAL exitoso (Orient + Arango)");
            } else {
                throw new RuntimeException("Alguna fase falló, rollback forzado");
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            try {
                if (oSess != null) {
                    oSess.rollback();
                    System.out.println("↩ Rollback ejecutado en OrientDB");
                }
            } catch (Exception ex) {
                System.err.println("Error en rollback: " + ex.getMessage());
            }
        } finally {
            if (oSess != null) oSess.close();
            if (orient != null) orient.close();
            if (adb != null) adb.shutdown();
        }
    }
}