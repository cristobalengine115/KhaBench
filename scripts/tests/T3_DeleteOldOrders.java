package transactions;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
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

public class T3_DeleteOldOrders {

    static class Args {
        final Map<String,String> m = new LinkedHashMap<>();
        Args(String[] a){ for(int i=0;i<a.length-1;i+=2) if(a[i].startsWith("--")) m.put(a[i].substring(2),a[i+1]); }
        String  s(String k,String d){ return m.getOrDefault(k,d); }
        int     i(String k,int d){ try{ return Integer.parseInt(m.get(k)); }catch(Exception e){ return d; } }
        boolean b(String k,boolean d){ String v=m.get(k); return v==null?d:("1".equals(v)||"true".equalsIgnoreCase(v)); }
    }

    static class Stat {
        final List<Long> lat = Collections.synchronizedList(new ArrayList<>());
        final AtomicInteger errors = new AtomicInteger(0);
        void add(long ms){ lat.add(ms); }
        void err(){ errors.incrementAndGet(); }
        long pct(double p){
            if(lat.isEmpty()) return 0;
            List<Long> s=new ArrayList<>(lat); Collections.sort(s);
            int idx=Math.max(0,Math.min((int)Math.ceil(p*s.size())-1,s.size()-1));
            return s.get(idx);
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

    static void writeJSON(String outDir, String dataset, int threads, String mode, String scen,
                          int duration, boolean warmup, Stat read, Stat write, Ryow ryow,
                          double elapsed, double avgCpu, double maxCpu, long avgMem, long maxMem,
                          boolean coldFlushOk){
        new File(outDir).mkdirs();
        String path = outDir + "/metrics_raw.json";
        String json =
            "{\n" +
            "  \"meta\": {\"engine\": \"arangodb+orientdb\", \"tx\": \"T3\", \"dataset\": "+esc(dataset)+", \"threads\": "+threads+",\n" +
            "            \"consistency_mode\": "+esc(mode)+", \"scenario\": "+esc(scen)+", \"duration_s\": "+duration+",\n" +
            "            \"elapsed_s\": "+String.format("%.3f",elapsed)+", \"iterations\": "+write.lat.size()+",\n" +
            "            \"cold_flush_ok\": "+(coldFlushOk?1:0)+", \"warmup_only\": "+(warmup?1:0)+"},\n" +
            "  \"ops\": {\n" +
            "    \"read\":  {\"p50\":"+read.pct(0.50)+",\"p95\":"+read.pct(0.95)+",\"p99\":"+read.pct(0.99)+",\"p999\":"+read.pct(0.999)+",\"avg_ms\":"+String.format("%.2f",read.avg())+",\"throughput\":"+String.format("%.4f",read.throughput(elapsed))+",\"errors\":"+read.errors.get()+"},\n" +
            "    \"write\": {\"p50\":"+write.pct(0.50)+",\"p95\":"+write.pct(0.95)+",\"p99\":"+write.pct(0.99)+",\"p999\":"+write.pct(0.999)+",\"avg_ms\":"+String.format("%.2f",write.avg())+",\"throughput\":"+String.format("%.4f",write.throughput(elapsed))+",\"errors\":"+write.errors.get()+"}\n" +
            "  },\n" +
            "  \"ryow\": {\"total_probes\":"+ryow.probes.get()+",\"immediate_hits\":"+ryow.hits.get()+",\"misses\":"+(ryow.probes.get()-ryow.hits.get())+",\n" +
            "             \"time_to_consistency_ms\": {\"p50\":"+ryow.pct(0.50)+",\"p95\":"+ryow.pct(0.95)+",\"p99\":"+ryow.pct(0.99)+",\"p999\":"+ryow.pct(0.999)+"}},\n" +
            "  \"system\": {\"cpu_avg_pct\":"+String.format("%.2f",avgCpu)+",\"cpu_max_pct\":"+String.format("%.2f",maxCpu)+
                          ",\"mem_avg_mb\":"+avgMem+",\"mem_max_mb\":"+maxMem+
                          ",\"disk_iops\":0,\"disk_lat_ms\":0,\"net_mbps\":0}\n" +
            "}\n";
        try(FileWriter w=new FileWriter(path)){ w.write(json); }
        catch(Exception e){ System.err.println("No pude escribir "+path+": "+e.getMessage()); }
    }

    static class Worker implements Callable<Void> {
        final String oHost, oDb, oUser, oPwd, aHost, aDbNm, orderId, mode;
        final int    aPort;
        final long   probeTimeoutMs, deadline;
        final boolean dryRun;
        final Stat   read, write;
        final Ryow   ryow;

        Worker(String oHost, String oDb, String oUser, String oPwd,
               String aHost, int aPort, String aDbNm,
               String orderId, String mode, long probeTimeoutMs,
               boolean dryRun, long deadline,
               Stat read, Stat write, Ryow ryow){
            this.oHost=oHost; this.oDb=oDb; this.oUser=oUser; this.oPwd=oPwd;
            this.aHost=aHost; this.aPort=aPort; this.aDbNm=aDbNm;
            this.orderId=orderId; this.mode=mode;
            this.probeTimeoutMs=probeTimeoutMs; this.dryRun=dryRun;
            this.deadline=deadline;
            this.read=read; this.write=write; this.ryow=ryow;
        }

        @Override
        public Void call(){
            OrientDB orient = new OrientDB("remote:"+oHost, OrientDBConfig.defaultConfig());
            ArangoDB arango = new ArangoDB.Builder().host(aHost, aPort).build();
            ArangoDatabase aDb = arango.db(aDbNm);

            try {
                while(System.currentTimeMillis() < deadline){
                    ODatabaseSession oSess = null;
                    try {
                        oSess = orient.open(oDb, oUser, oPwd);
                        oSess.begin();

                        long t = System.currentTimeMillis();
                        Map<String,Object> orderBefore = leerOrderOrient(oSess, orderId);
                        read.add(System.currentTimeMillis()-t);

                        if(!dryRun){
                            if(orderBefore != null){
                                t = System.currentTimeMillis();
                                oSess.command("DELETE VERTEX ORDER_PRE_PANDEMIC WHERE ORDER_ID = ?", orderId);
                                write.add(System.currentTimeMillis()-t);
                            }

                            t = System.currentTimeMillis();
                            List<String> invoiceKeys = buscarInvoiceKeys(aDb, orderId);
                            read.add(System.currentTimeMillis()-t);

                            if(!invoiceKeys.isEmpty()){
                                String action = """
                                    function (params) {
                                      const db = require('@arangodb').db;
                                      const col = db._collection("INVOICE");
                                      for (const k of (params.keys || [])) col.update(k, { ORDER_ID: null });
                                    }
                                """;
                                t = System.currentTimeMillis();
                                aDb.transaction(action, Void.class,
                                    new TransactionOptions()
                                        .readCollections("INVOICE")
                                        .writeCollections("INVOICE")
                                        .waitForSync("B".equalsIgnoreCase(mode))
                                        .params(Map.of("keys", invoiceKeys)));
                                write.add(System.currentTimeMillis()-t);

                                long start = System.currentTimeMillis(); boolean ok=false;
                                while(System.currentTimeMillis()-start < probeTimeoutMs){
                                    long tr = System.currentTimeMillis();
                                    long remain = countInvoicesWithOrderId(aDb, orderId);
                                    read.add(System.currentTimeMillis()-tr);
                                    if(remain == 0){ ok=true; break; }
                                    try{ Thread.sleep(10); }catch(InterruptedException ignore){}
                                }
                                ryow.probe(ok, System.currentTimeMillis()-start);
                            }

                            oSess.commit();
                        } else {
                            oSess.rollback();
                        }

                    } catch(Exception e){
                        write.err();
                        try{ if(oSess != null) oSess.rollback(); }catch(Exception ignore){}
                    } finally {
                        if(oSess != null) oSess.close();
                    }
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

        String  outDir         = A.s("out_dir","./out");
        String  dataset        = new File(A.s("dataset_dir","S")).getName();
        int     threads        = A.i("threads", 1);
        int     duration       = A.i("duration", 120);
        String  mode           = A.s("consistency_mode","A");
        String  scen           = A.s("scenario","normal");
        boolean warmup         = A.b("warmup_only", false);
        String  oHost          = A.s("orient_host","192.168.50.101");
        String  oDb            = A.s("orient_db","KhaBench");
        String  oUser          = A.s("orient_user","root");
        String  oPwd           = A.s("orient_pwd","rootpwd");
        String  aHost          = A.s("arango_host","192.168.50.101");
        int     aPort          = A.i("arango_port", 8529);
        String  aDbNm          = A.s("arango_db","KhaBench");
        String  orderId        = A.s("order_id","b02ea8f4-9d6f-46c4-9e3a-1647f0a76f77").trim();
        boolean dryRun         = A.b("dry_run", false);
        long    probeTimeoutMs = 500L;
        String  sshUser        = A.s("ssh_user","pi");

        System.out.printf("=== T3 DeleteOldOrders === dataset=%s thr=%d dur=%ds mode=%s scen=%s orderId=%s dry=%s%n",
                          dataset, threads, duration, mode, scen, orderId, dryRun);

        if(warmup){
            writeJSON(outDir,dataset,threads,mode,scen,duration,true,
                      new Stat(),new Stat(),new Ryow(),0,0,0,0,0,false);
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

        Stat read=new Stat(), write=new Stat();
        Ryow ryow=new Ryow();

        long deadline = System.currentTimeMillis() + duration * 1000L;
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        for(int t=0; t<threads; t++)
            pool.submit(new Worker(oHost,oDb,oUser,oPwd,aHost,aPort,aDbNm,
                                   orderId,mode,probeTimeoutMs,dryRun,deadline,
                                   read,write,ryow));
        pool.shutdown();
        pool.awaitTermination(duration+30, TimeUnit.SECONDS);

        sampler.running = false;
        samplerThread.join(2000);

        System.out.printf("Iteraciones write: %d  RYOW probes=%d hits=%d%n",
                          write.lat.size(), ryow.probes.get(), ryow.hits.get());
        System.out.printf("CPU avg=%.1f%% max=%.1f%%  RAM avg=%dMB max=%dMB%n",
                          sampler.avgCpu(), sampler.maxCpu(), sampler.avgMem(), sampler.maxMem());

        writeJSON(outDir,dataset,threads,mode,scen,duration,false,
                  read,write,ryow,(double)duration,
                  sampler.avgCpu(),sampler.maxCpu(),sampler.avgMem(),sampler.maxMem(),
                  coldFlushOk);

        System.out.println("Métricas escritas en: "+outDir+"/metrics_raw.json");
    }


    private static Map<String,Object> leerOrderOrient(ODatabaseSession db, String orderId){
        String sql = "SELECT ORDER_ID, ORDER_DATE, TOTAL_PRICE, CUSTOMER_ID, ORDERLINE " +
                     "FROM ORDER_PRE_PANDEMIC WHERE ORDER_ID = ? LIMIT 1";
        try(OResultSet rs = db.query(sql, orderId)){
            if(!rs.hasNext()) return null;
            OResult r = rs.next();
            Map<String,Object> out = new LinkedHashMap<>();
            out.put("ORDER_ID",    r.getProperty("ORDER_ID"));
            out.put("ORDER_DATE",  r.getProperty("ORDER_DATE"));
            out.put("TOTAL_PRICE", r.getProperty("TOTAL_PRICE"));
            out.put("CUSTOMER_ID", r.getProperty("CUSTOMER_ID"));
            Object ol = r.getProperty("ORDERLINE");
            List<String> products = new ArrayList<>();
            if(ol instanceof Iterable) for(Object x : (Iterable<?>)ol) if(x!=null) products.add(String.valueOf(x));
            out.put("ORDERLINE", products);
            return out;
        }
    }

    private static List<String> buscarInvoiceKeys(ArangoDatabase db, String orderId){
        String aql = """
            FOR i IN INVOICE
              FILTER i.ORDER_ID == @id
                  OR i.ORDER_ID == CONCAT('ORDER/', @id)
                  OR i._key      == @id
              RETURN i._key
        """;
        return db.query(aql, String.class, Map.of("id",orderId), new AqlQueryOptions()).asListRemaining();
    }

    private static long countInvoicesWithOrderId(ArangoDatabase db, String orderId){
        String aql = """
            RETURN LENGTH(
              FOR i IN INVOICE
                FILTER i.ORDER_ID == @oid
                    OR i.ORDER_ID == CONCAT('ORDER/', @oid)
                    OR i._key      == @oid
                RETURN 1
            )
        """;
        List<Long> res = db.query(aql, Long.class, Map.of("oid",orderId), new AqlQueryOptions()).asListRemaining();
        return res.isEmpty()?0:res.get(0);
    }
}