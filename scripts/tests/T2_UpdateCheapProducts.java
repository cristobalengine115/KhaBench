package transactions;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.TransactionOptions;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.Collectors;

public class T2_UpdateCheapProducts {

    static class Args {
        final Map<String,String> m = new LinkedHashMap<>();
        Args(String[] a){ for(int i=0;i<a.length-1;i+=2) if(a[i].startsWith("--")) m.put(a[i].substring(2),a[i+1]); }
        String  s(String k,String d){ return m.getOrDefault(k,d); }
        int     i(String k,int d){ try{ return Integer.parseInt(m.get(k)); }catch(Exception e){ return d; } }
        long    l(String k,long d){ try{ return Long.parseLong(m.get(k)); }catch(Exception e){ return d; } }
        double  d(String k,double D){ try{ return Double.parseDouble(m.get(k)); }catch(Exception e){ return D; } }
        boolean b(String k,boolean D){ String v=m.get(k); return v==null?D:("1".equals(v)||"true".equalsIgnoreCase(v)); }
    }

    static class Stat {
        final List<Long> lat = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger errors = new AtomicInteger(0);
        void add(long ms){ lat.add(ms); }
        void err(){ errors.incrementAndGet(); }
        long pct(double p){
            if(lat.isEmpty()) return 0;
            List<Long> v=new ArrayList<>(lat); v.sort(Long::compareTo);
            int idx=Math.max(0,Math.min((int)Math.ceil(p*v.size())-1,v.size()-1));
            return v.get(idx);
        }
        double avg(){
            return lat.isEmpty()?0.0:lat.stream().mapToLong(Long::longValue).average().orElse(0.0);
        }
        double throughput(double secs){ return secs>0?(double)lat.size()/secs:0.0; }
    }

    static class Ryow {
        final AtomicInteger probes = new AtomicInteger(0);
        final AtomicInteger hits   = new AtomicInteger(0);
        final List<Long> ttc = Collections.synchronizedList(new ArrayList<>());
        void probe(boolean ok, long ms){ probes.incrementAndGet(); if(ok) hits.incrementAndGet(); ttc.add(ms); }
        long pct(double p){
            if(ttc.isEmpty()) return 0;
            List<Long> v=new ArrayList<>(ttc); v.sort(Long::compareTo);
            int idx=Math.max(0,Math.min((int)Math.ceil(p*v.size())-1,v.size()-1));
            return v.get(idx);
        }
    }

    static class SysSampler implements Runnable {
        volatile boolean running = true;
        final List<Double> cpuSamples = Collections.synchronizedList(new ArrayList<>());
        final List<Long>   memSamples = Collections.synchronizedList(new ArrayList<>());

        private long[] readCpuTicks() throws Exception {
            String line = Files.readAllLines(Paths.get("/proc/stat")).get(0);
            String[] p = line.trim().split("\\s+");
            long user=Long.parseLong(p[1]),nice=Long.parseLong(p[2]),
                 sys=Long.parseLong(p[3]),idle=Long.parseLong(p[4]),
                 iowait=Long.parseLong(p[5]),irq=Long.parseLong(p[6]),
                 softirq=Long.parseLong(p[7]);
            long total=user+nice+sys+idle+iowait+irq+softirq;
            return new long[]{total-idle-iowait, total};
        }

        private long readMemUsedMb() throws Exception {
            long total=0, available=0;
            for(String line : Files.readAllLines(Paths.get("/proc/meminfo"))){
                if(line.startsWith("MemTotal:"))     total     = Long.parseLong(line.replaceAll("[^0-9]",""));
                if(line.startsWith("MemAvailable:")) available = Long.parseLong(line.replaceAll("[^0-9]",""));
            }
            return (total-available)/1024;
        }

        @Override
        public void run(){
            try {
                long[] prev = readCpuTicks();
                while(running){
                    Thread.sleep(500);
                    long[] cur = readCpuTicks();
                    long dBusy=cur[0]-prev[0], dTotal=cur[1]-prev[1];
                    cpuSamples.add(dTotal>0?100.0*dBusy/dTotal:0.0);
                    memSamples.add(readMemUsedMb());
                    prev = cur;
                }
            } catch(Exception ignored){}
        }

        double avgCpu(){ return cpuSamples.isEmpty()?0.0:cpuSamples.stream().mapToDouble(Double::doubleValue).average().orElse(0.0); }
        double maxCpu(){ return cpuSamples.isEmpty()?0.0:cpuSamples.stream().mapToDouble(Double::doubleValue).max().orElse(0.0); }
        long   avgMem(){ return memSamples.isEmpty()?0L:(long)memSamples.stream().mapToLong(Long::longValue).average().orElse(0.0); }
        long   maxMem(){ return memSamples.isEmpty()?0L:memSamples.stream().mapToLong(Long::longValue).max().orElse(0L); }
    }

    static boolean dropCaches(String host, String sshUser){
        try {
            ProcessBuilder pb = new ProcessBuilder(
                "ssh","-o","StrictHostKeyChecking=no",
                sshUser+"@"+host,
                "sync && echo 3 | sudo tee /proc/sys/vm/drop_caches"
            );
            pb.redirectErrorStream(true);
            Process proc = pb.start();
            String out = new String(proc.getInputStream().readAllBytes());
            int code = proc.waitFor();
            System.out.println("drop_caches "+host+" -> exit="+code+" out="+out.trim());
            return code == 0;
        } catch(Exception e){
            System.err.println("drop_caches failed on "+host+": "+e.getMessage());
            return false;
        }
    }

    static String esc(String s){ return s==null?"null":"\""+s.replace("\\","\\\\").replace("\"","\\\"")+"\""; }

    static void writeJSON(String outDir, String dataset, int threads, String scen, int duration,
                          String arangoMode, String orientMode,
                          Stat oR, Stat oW, Stat aR, Stat aW, Stat e2e,
                          Ryow ryO, Ryow ryA,
                          double elapsed, double avgCpu, double maxCpu, long avgMem, long maxMem,
                          boolean coldFlushOk){
        try {
            new File(outDir).mkdirs();
            String path = outDir + "/metrics_raw.json";
            String json =
                "{\n" +
                "  \"meta\": {\"engine\": \"multi\", \"tx\": \"T2\", \"dataset\": "+esc(dataset)+", \"threads\": "+threads+",\n" +
                "            \"scenario\": "+esc(scen)+", \"duration_s\": "+duration+",\n" +
                "            \"elapsed_s\": "+String.format("%.3f",elapsed)+",\n" +
                "            \"cold_flush_ok\": "+(coldFlushOk?1:0)+",\n" +
                "            \"consistency_modes\": {\"arango\": "+esc(arangoMode)+", \"orient\": "+esc(orientMode)+"}},\n" +
                "  \"ops\": {\n" +
                "    \"orient\": {\n" +
                "      \"read\":  {\"p50\":"+oR.pct(0.50)+",\"p95\":"+oR.pct(0.95)+",\"p99\":"+oR.pct(0.99)+",\"p999\":"+oR.pct(0.999)+",\"avg_ms\":"+String.format("%.2f",oR.avg())+",\"throughput\":"+String.format("%.4f",oR.throughput(elapsed))+",\"errors\":"+oR.errors.get()+"},\n" +
                "      \"write\": {\"p50\":"+oW.pct(0.50)+",\"p95\":"+oW.pct(0.95)+",\"p99\":"+oW.pct(0.99)+",\"p999\":"+oW.pct(0.999)+",\"avg_ms\":"+String.format("%.2f",oW.avg())+",\"throughput\":"+String.format("%.4f",oW.throughput(elapsed))+",\"errors\":"+oW.errors.get()+"}},\n" +
                "    \"arango\": {\n" +
                "      \"read\":  {\"p50\":"+aR.pct(0.50)+",\"p95\":"+aR.pct(0.95)+",\"p99\":"+aR.pct(0.99)+",\"p999\":"+aR.pct(0.999)+",\"avg_ms\":"+String.format("%.2f",aR.avg())+",\"throughput\":"+String.format("%.4f",aR.throughput(elapsed))+",\"errors\":"+aR.errors.get()+"},\n" +
                "      \"write\": {\"p50\":"+aW.pct(0.50)+",\"p95\":"+aW.pct(0.95)+",\"p99\":"+aW.pct(0.99)+",\"p999\":"+aW.pct(0.999)+",\"avg_ms\":"+String.format("%.2f",aW.avg())+",\"throughput\":"+String.format("%.4f",aW.throughput(elapsed))+",\"errors\":"+aW.errors.get()+"}},\n" +
                "    \"end_to_end\": {\"p50\":"+e2e.pct(0.50)+",\"p95\":"+e2e.pct(0.95)+",\"p99\":"+e2e.pct(0.99)+",\"p999\":"+e2e.pct(0.999)+",\"avg_ms\":"+String.format("%.2f",e2e.avg())+"}\n" +
                "  },\n" +
                "  \"ryow\": {\n" +
                "    \"orient\": {\"total_probes\":"+ryO.probes.get()+",\"immediate_hits\":"+ryO.hits.get()+",\"misses\":"+(ryO.probes.get()-ryO.hits.get())+",\n" +
                "                \"time_to_consistency_ms\": {\"p50\":"+ryO.pct(0.50)+",\"p95\":"+ryO.pct(0.95)+",\"p99\":"+ryO.pct(0.99)+",\"p999\":"+ryO.pct(0.999)+"}},\n" +
                "    \"arango\": {\"total_probes\":"+ryA.probes.get()+",\"immediate_hits\":"+ryA.hits.get()+",\"misses\":"+(ryA.probes.get()-ryA.hits.get())+",\n" +
                "                \"time_to_consistency_ms\": {\"p50\":"+ryA.pct(0.50)+",\"p95\":"+ryA.pct(0.95)+",\"p99\":"+ryA.pct(0.99)+",\"p999\":"+ryA.pct(0.999)+"}}\n" +
                "  },\n" +
                "  \"system\": {\"cpu_avg_pct\":"+String.format("%.2f",avgCpu)+",\"cpu_max_pct\":"+String.format("%.2f",maxCpu)+
                              ",\"mem_avg_mb\":"+avgMem+",\"mem_max_mb\":"+maxMem+
                              ",\"disk_iops\":0,\"disk_lat_ms\":0,\"net_mbps\":0}\n" +
                "}\n";
            try(FileWriter w=new FileWriter(path)){ w.write(json); }
        } catch(Exception e){ System.err.println("No pude escribir metrics_raw.json: "+e.getMessage()); }
    }

    static class Worker implements Callable<Void> {
        final String oHost, oDb, oUser, oPwd, aHost, aDbNm, aMode, oMode;
        final int aPort, batchOrders;
        final double factor;
        final long probeTimeoutMs, deadline;
        final Stat oRead, oWrite, aRead, aWrite, e2e;
        final Ryow ryO, ryA;

        Worker(String oHost, String oDb, String oUser, String oPwd,
               String aHost, int aPort, String aDbNm, String aMode, String oMode,
               double factor, long probeTimeoutMs, int batchOrders, long deadline,
               Stat oRead, Stat oWrite, Stat aRead, Stat aWrite, Stat e2e,
               Ryow ryO, Ryow ryA){
            this.oHost=oHost; this.oDb=oDb; this.oUser=oUser; this.oPwd=oPwd;
            this.aHost=aHost; this.aPort=aPort; this.aDbNm=aDbNm;
            this.aMode=aMode; this.oMode=oMode;
            this.factor=factor; this.probeTimeoutMs=probeTimeoutMs;
            this.batchOrders=batchOrders; this.deadline=deadline;
            this.oRead=oRead; this.oWrite=oWrite; this.aRead=aRead; this.aWrite=aWrite; this.e2e=e2e;
            this.ryO=ryO; this.ryA=ryA;
        }

        @Override
        public Void call(){
            OrientDB orient = new OrientDB("remote:"+oHost, OrientDBConfig.defaultConfig());
            ArangoDB arango = new ArangoDB.Builder().host(aHost, aPort).build();
            ArangoDatabase aDb = arango.db(aDbNm);

            try {
                while(System.currentTimeMillis() < deadline){
                    long t0 = System.currentTimeMillis();
                    ODatabaseSession oSess = null;
                    try {
                        oSess = orient.open(oDb, oUser, oPwd);
                        oSess.begin();

                        List<String> products = leerTodosLosProductos(oSess);

                        Map<String,Double> deltas = new LinkedHashMap<>();
                        for(String pid : products){
                            long t = System.currentTimeMillis();
                            Double before = leerPrecioOrient(oSess, pid);
                            oRead.add(System.currentTimeMillis()-t);
                            if(before == null) continue;

                            t = System.currentTimeMillis();
                            subirPrecioOrient(oSess, pid, factor);
                            oWrite.add(System.currentTimeMillis()-t);

                            long start = System.currentTimeMillis(); boolean ok=false; Double after=null;
                            while(System.currentTimeMillis()-start < probeTimeoutMs){
                                long tr = System.currentTimeMillis();
                                after = leerPrecioOrient(oSess, pid);
                                oRead.add(System.currentTimeMillis()-tr);
                                if(after != null && Math.abs(after - before*factor) < 1e-6){ ok=true; break; }
                                try{ Thread.sleep(10); }catch(InterruptedException ignore){}
                            }
                            ryO.probe(ok, System.currentTimeMillis()-start);
                            if(after != null) deltas.put(pid, after - before);
                        }

                        if(!deltas.isEmpty()){
                            List<String> allOrderKeys = new ArrayList<>();
                            for(String pid : deltas.keySet()){
                                long t = System.currentTimeMillis();
                                allOrderKeys.addAll(buscarOrdersPorProducto(aDb, pid, batchOrders));
                                aRead.add(System.currentTimeMillis()-t);
                            }
                            allOrderKeys = allOrderKeys.stream().distinct().collect(Collectors.toList());

                            if(!allOrderKeys.isEmpty()){
                                String action = """
                                    function (params) {
                                      const db = require('@arangodb').db;
                                      const keys  = params.keys  || [];
                                      const delta = params.delta || {};
                                      const colO  = db._collection('ORDER');
                                      const colI  = db._collection('INVOICE');
                                      for (const k of keys) {
                                        const o = colO.document(k);
                                        if (!o) continue;
                                        const base = Number.isFinite(Number(o.TotalPrice ?? o.TOTAL_PRICE)) ? Number(o.TotalPrice ?? o.TOTAL_PRICE) : 0;
                                        let inc = 0;
                                        if (Array.isArray(o.ORDERLINE)) {
                                          for (const p of o.ORDERLINE) {
                                            const id = String(p).replace(/^Product\\/|^PRODUCT\\//,'').trim();
                                            if (Object.prototype.hasOwnProperty.call(delta, id)) {
                                              const d = Number(delta[id]);
                                              if (Number.isFinite(d)) inc += d;
                                            }
                                          }
                                        }
                                        if (inc !== 0) {
                                          const newTotal = Number((base + inc).toFixed(2));
                                          colO.update(k, { TOTAL_PRICE: newTotal, TotalPrice: newTotal });
                                          const orderId = (o.ORDER_ID && String(o.ORDER_ID).trim()) ? String(o.ORDER_ID).trim() : String(o._key).trim();
                                          db._query(`FOR i IN INVOICE FILTER i.ORDER_ID == @oid OR i.ORDER_ID == CONCAT('ORDER/', @oid) OR i._key == @oid UPDATE i WITH { TOTAL_PRICE: @t, TotalPrice: @t } IN INVOICE`, { oid: orderId, t: newTotal });
                                        }
                                      }
                                    }
                                """;
                                TransactionOptions opts = new TransactionOptions()
                                    .readCollections("ORDER","INVOICE")
                                    .writeCollections("ORDER","INVOICE")
                                    .waitForSync("B".equalsIgnoreCase(aMode))
                                    .params(Map.of("keys", allOrderKeys, "delta", deltas));

                                long tw = System.currentTimeMillis();
                                aDb.transaction(action, Void.class, opts);
                                aWrite.add(System.currentTimeMillis()-tw);

                                int sample = Math.min(20, allOrderKeys.size());
                                for(String key : allOrderKeys.subList(0, sample)){
                                    long start = System.currentTimeMillis(); boolean ok=false;
                                    while(System.currentTimeMillis()-start < probeTimeoutMs){
                                        long tr = System.currentTimeMillis();
                                        Double newTotal = leerTotalOrder(aDb, key);
                                        aRead.add(System.currentTimeMillis()-tr);
                                        if(newTotal != null && newTotal > 0){ ok=true; break; }
                                        try{ Thread.sleep(10); }catch(InterruptedException ignore){}
                                    }
                                    ryA.probe(ok, System.currentTimeMillis()-start);
                                }
                            }
                        }

                        oSess.commit();

                    } catch(Exception e){
                        oWrite.err(); aWrite.err();
                        try{ if(oSess != null) oSess.rollback(); }catch(Exception ignore){}
                    } finally {
                        if(oSess != null) oSess.close();
                    }

                    e2e.add(System.currentTimeMillis()-t0);
                }
            } finally {
                arango.shutdown();
                orient.close();
            }
            return null;
        }
    }

    public static void main(String[] argv) throws Exception {
        Args A = new Args(argv);

        String  outDir          = A.s("out_dir","./out");
        String  dataset         = new File(A.s("dataset_dir","S")).getName();
        int     threads         = A.i("threads", 1);
        int     duration        = A.i("duration", 60);
        String  scen            = A.s("scenario","normal");
        boolean warmup          = A.b("warmup_only", false);
        String  oHost           = A.s("orient_host","192.168.50.102");
        String  oDb             = A.s("orient_db","KhaBench");
        String  oUser           = A.s("orient_user","root");
        String  oPwd            = A.s("orient_pwd","rootpwd");
        String  oMode           = A.s("orient_mode","A");
        String  aHost           = A.s("arango_host","192.168.50.101");
        int     aPort           = A.i("arango_port", 8529);
        String  aDbNm           = A.s("arango_db","KhaBench");
        String  aMode           = A.s("arango_mode","A");
        double  factor          = A.d("price_factor", 1.10);
        long    probeTimeoutMs  = A.l("probe_timeout_ms", 500);
        int     batchOrders     = A.i("batch_orders", 200);
        String  sshUser         = A.s("ssh_user","pi");

        System.out.printf("=== T2 runner === dataset=%s thr=%d scen=%s orient[%s] arango[%s] factor=%.2f%n",
                          dataset, threads, scen, oHost, aHost, factor);

        if(warmup){
            Stat dummy=new Stat();
            writeJSON(outDir,dataset,threads,scen,duration,aMode,oMode,
                      dummy,dummy,dummy,dummy,dummy,new Ryow(),new Ryow(),0,0,0,0,0,false);
            System.out.println("Warm-up only. Fin.");
            return;
        }

        SysSampler sampler = new SysSampler();
        Thread samplerThread = new Thread(sampler);
        samplerThread.setDaemon(true);
        samplerThread.start();

        boolean coldFlushOk = false;
        if("cold".equalsIgnoreCase(scen)){
            System.out.println("Cold cache: limpiando nodos...");
            boolean a = dropCaches(aHost, sshUser);
            boolean b = dropCaches(oHost, sshUser);
            coldFlushOk = a && b;
            Thread.sleep(3000);
        }

        Stat oRead=new Stat(), oWrite=new Stat(), aRead=new Stat(), aWrite=new Stat(), e2e=new Stat();
        Ryow ryO=new Ryow(), ryA=new Ryow();

        long deadline = System.currentTimeMillis() + duration * 1000L;
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        for(int t=0; t<threads; t++)
            pool.submit(new Worker(oHost,oDb,oUser,oPwd,aHost,aPort,aDbNm,aMode,oMode,
                                   factor,probeTimeoutMs,batchOrders,deadline,
                                   oRead,oWrite,aRead,aWrite,e2e,ryO,ryA));
        pool.shutdown();
        pool.awaitTermination(duration+30, TimeUnit.SECONDS);

        sampler.running = false;
        samplerThread.join(2000);

        System.out.printf("Iteraciones e2e: %d%n", e2e.lat.size());
        System.out.printf("RYOW Orient probes=%d hits=%d  Arango probes=%d hits=%d%n",
                          ryO.probes.get(), ryO.hits.get(), ryA.probes.get(), ryA.hits.get());
        System.out.printf("CPU avg=%.1f%% max=%.1f%%  RAM avg=%dMB max=%dMB%n",
                          sampler.avgCpu(), sampler.maxCpu(), sampler.avgMem(), sampler.maxMem());

        writeJSON(outDir,dataset,threads,scen,duration,aMode,oMode,
                  oRead,oWrite,aRead,aWrite,e2e,ryO,ryA,
                  (double)duration,
                  sampler.avgCpu(),sampler.maxCpu(),sampler.avgMem(),sampler.maxMem(),
                  coldFlushOk);

        System.out.println("Métricas escritas en: "+outDir+"/metrics_raw.json");
    }

    private static List<String> leerTodosLosProductos(ODatabaseSession db){
        List<String> ids = new ArrayList<>();
        try(OResultSet rs = db.query("SELECT PRODUCT_ID FROM PRODUCT_EXPENSIVE")){
            while(rs.hasNext()){
                OResult r = rs.next();
                String id = r.getProperty("PRODUCT_ID");
                if(id != null && !id.isBlank()) ids.add(id.trim());
            }
        }
        return ids;
    }

    private static Double leerPrecioOrient(ODatabaseSession db, String productId){
        try(OResultSet rs = db.query("SELECT PRICE FROM PRODUCT_EXPENSIVE WHERE PRODUCT_ID = ? LIMIT 1", productId)){
            if(rs.hasNext()){
                Number n = rs.next().getProperty("PRICE");
                return n != null ? n.doubleValue() : null;
            }
        }
        return null;
    }

    private static void subirPrecioOrient(ODatabaseSession db, String productId, double factor){
        db.command("UPDATE PRODUCT_EXPENSIVE SET PRICE = PRICE * ? WHERE PRODUCT_ID = ?", factor, productId);
    }

    private static List<String> buscarOrdersPorProducto(ArangoDatabase db, String productId, int limit){
        String aql = """
            LET pid = @pid
            FOR o IN `ORDER`
              FILTER LENGTH(
                FOR p IN o.ORDERLINE
                  LET id = REGEX_REPLACE(TO_STRING(p), "^Product/|^PRODUCT/", "")
                  FILTER id == pid
                  RETURN 1
              ) > 0
              LIMIT @limit
              RETURN o._key
        """;
        return db.query(aql, String.class, Map.of("pid",productId,"limit",limit), new AqlQueryOptions()).asListRemaining();
    }

    private static Double leerTotalOrder(ArangoDatabase db, String key){
        String aql = "RETURN TO_NUMBER(DOCUMENT('ORDER', @k).TotalPrice)";
        List<Double> r = db.query(aql, Double.class, Map.of("k",key), null).asListRemaining();
        return r.isEmpty() ? null : r.get(0);
    }
}