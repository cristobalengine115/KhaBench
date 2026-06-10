package transactions;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.model.AqlQueryOptions;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class C1_ValidateOrdersByRegion {

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

    static class SysSampler implements Runnable {
        volatile boolean running = true;
        final List<Double> cpuSamples = Collections.synchronizedList(new ArrayList<>());
        final List<Long>   memSamples = Collections.synchronizedList(new ArrayList<>());

        private long[] readCpuTicks() throws Exception {
            String line = Files.readAllLines(Paths.get("/proc/stat")).get(0);
            String[] p = line.trim().split("\\s+");
            long user=Long.parseLong(p[1]), nice=Long.parseLong(p[2]),
                 sys=Long.parseLong(p[3]),  idle=Long.parseLong(p[4]),
                 iowait=Long.parseLong(p[5]),irq=Long.parseLong(p[6]),
                 softirq=Long.parseLong(p[7]);
            long total = user+nice+sys+idle+iowait+irq+softirq;
            long busy  = total-idle-iowait;
            return new long[]{busy, total};
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
                    cpuSamples.add(dTotal>0 ? 100.0*dBusy/dTotal : 0.0);
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

    static boolean dropCaches(String host, String sshUser) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                "ssh", "-o", "StrictHostKeyChecking=no",
                sshUser + "@" + host,
                "sync && echo 3 | sudo tee /proc/sys/vm/drop_caches"
            );
            pb.redirectErrorStream(true);
            Process proc = pb.start();
            String out = new String(proc.getInputStream().readAllBytes());
            int code = proc.waitFor();
            System.out.println("drop_caches " + host + " -> exit=" + code + " out=" + out.trim());
            return code == 0;
        } catch(Exception e){
            System.err.println("drop_caches failed on " + host + ": " + e.getMessage());
            return false;
        }
    }

    static long measureRyow(ArangoDatabase db) {
        String probeKey = "ryow_" + UUID.randomUUID().toString().substring(0,8);
        long latency = -1;
        try {
            // Escritura del documento probe
            String insertAql = "INSERT {_key: @k, _ryow: true} INTO ORDER_POST_PANDEMIC";
            Map<String,Object> bind = new HashMap<>();
            bind.put("k", probeKey);
            db.query(insertAql, Void.class, bind, new AqlQueryOptions());

            long t0 = System.currentTimeMillis();

            // Lectura inmediata — mide tiempo hasta visibilidad
            String readAql = "RETURN LENGTH(FOR o IN ORDER_POST_PANDEMIC FILTER o._key == @k RETURN o)";
            ArangoCursor<Long> cur = db.query(readAql, Long.class, bind, new AqlQueryOptions());
            long found = cur.hasNext() ? cur.next() : 0L;
            latency = System.currentTimeMillis() - t0;

            if(found == 0) latency = -1;

            // Limpieza
            String delAql = "REMOVE {_key: @k} IN ORDER_POST_PANDEMIC";
            db.query(delAql, Void.class, bind, new AqlQueryOptions());

        } catch(Exception e){
            System.err.println("RYOW probe error: " + e.getMessage());
        }
        return latency;
    }

    static class Worker implements Callable<Void> {
        final OrientDB      orient;
        final ArangoDatabase arangoDB;
        final Stat          orientStat;
        final Stat          arangoStat;
        final AtomicLong    northRef, centerRef, southRef;
        final long          deadline;

        Worker(OrientDB orient, ArangoDatabase arangoDB,
               Stat orientStat, Stat arangoStat,
               AtomicLong northRef, AtomicLong centerRef, AtomicLong southRef,
               long deadline){
            this.orient=orient; this.arangoDB=arangoDB;
            this.orientStat=orientStat; this.arangoStat=arangoStat;
            this.northRef=northRef; this.centerRef=centerRef; this.southRef=southRef;
            this.deadline=deadline;
        }

        @Override
        public Void call(){
            while(System.currentTimeMillis() < deadline){
                Set<String> northIds  = fetchCustomerIds(orient, "CUSTOMER_NORTH",  orientStat);
                Set<String> centerIds = fetchCustomerIds(orient, "CUSTOMER_CENTER", orientStat);
                Set<String> southIds  = fetchCustomerIds(orient, "CUSTOMER_SOUTH",  orientStat);

                northRef.set(countOrders(arangoDB, northIds,  arangoStat));
                centerRef.set(countOrders(arangoDB, centerIds, arangoStat));
                southRef.set(countOrders(arangoDB, southIds,  arangoStat));
            }
            return null;
        }
    }
    static String esc(String s){ return s==null?"null":"\""+s.replace("\\","\\\\").replace("\"","\\\"")+"\""; }

    static void writeJSON(String outDir, String dataset, int threads, String mode, String scen,
                          int duration, boolean warmup,
                          Stat orientStat, Stat arangoStat,
                          long totalNorth, long totalCenter, long totalSouth,
                          double elapsed, double avgCpu, double maxCpu, long avgMem, long maxMem,
                          long ryowP50, long ryowP95, long ryowP99, int ryowSamples,
                          boolean coldFlushOk) {
        new File(outDir).mkdirs();
        String path = outDir + "/metrics_raw.json";
        String json =
            "{\n" +
            "  \"meta\": {\n" +
            "    \"engine\": \"multi\", \"tx\": \"C1\", \"dataset\": "+esc(dataset)+",\n" +
            "    \"threads\": "+threads+", \"consistency_mode\": "+esc(mode)+",\n" +
            "    \"scenario\": "+esc(scen)+", \"duration_s\": "+duration+",\n" +
            "    \"elapsed_s\": "+String.format("%.3f",elapsed)+", \"iterations\": "+orientStat.lat.size()+",\n" +
            "    \"cold_flush_ok\": "+(coldFlushOk?1:0)+", \"warmup_only\": "+(warmup?1:0)+"\n" +
            "  },\n" +
            "  \"ops\": {\n" +
            "    \"orient_read\": {\"p50\": "+orientStat.pct(0.50)+", \"p95\": "+orientStat.pct(0.95)+", \"p99\": "+orientStat.pct(0.99)+
                               ", \"avg_ms\": "+String.format("%.2f",orientStat.avg())+
                               ", \"throughput\": "+String.format("%.4f",orientStat.throughput(elapsed))+
                               ", \"samples\": "+orientStat.lat.size()+", \"errors\": "+orientStat.errors.get()+"},\n" +
            "    \"arango_read\": {\"p50\": "+arangoStat.pct(0.50)+", \"p95\": "+arangoStat.pct(0.95)+", \"p99\": "+arangoStat.pct(0.99)+
                               ", \"avg_ms\": "+String.format("%.2f",arangoStat.avg())+
                               ", \"throughput\": "+String.format("%.4f",arangoStat.throughput(elapsed))+
                               ", \"samples\": "+arangoStat.lat.size()+", \"errors\": "+arangoStat.errors.get()+"}\n" +
            "  },\n" +
            "  \"ryow\": {\"p50_ms\": "+ryowP50+", \"p95_ms\": "+ryowP95+", \"p99_ms\": "+ryowP99+", \"samples\": "+ryowSamples+"},\n" +
            "  \"result\": {\n" +
            "    \"orders_north\": "+totalNorth+", \"orders_center\": "+totalCenter+", \"orders_south\": "+totalSouth+"\n" +
            "  },\n" +
            "  \"system\": {\"cpu_avg_pct\": "+String.format("%.2f",avgCpu)+", \"cpu_max_pct\": "+String.format("%.2f",maxCpu)+
                          ", \"mem_avg_mb\": "+avgMem+", \"mem_max_mb\": "+maxMem+
                          ", \"disk_iops\": 0, \"disk_lat_ms\": 0, \"net_mbps\": 0}\n" +
            "}\n";
        try(FileWriter w=new FileWriter(path)){ w.write(json); }
        catch(Exception e){ System.err.println("No pude escribir "+path+": "+e.getMessage()); }
    }
    public static void main(String[] args) throws Exception {
        Args A = new Args(args);

        String  outDir   = A.s("out_dir","./out");
        String  dataset  = new File(A.s("dataset_dir","S")).getName();
        int     threads  = A.i("threads", 1);
        int     duration = A.i("duration", 60);
        String  mode     = A.s("consistency_mode","A");
        String  scen     = A.s("scenario","normal");
        boolean warmup   = A.b("warmup_only", false);
        String  sshUser  = A.s("ssh_user", "pi");
        int     ryowN    = A.i("ryow_samples", 30);

        System.out.println("=== CONSULTA C1: Validar órdenes post-pandemia por región ===");

        if(warmup){
            writeJSON(outDir,dataset,threads,mode,scen,duration,true,
                      new Stat(),new Stat(),0,0,0,0,0,0,0,0,0,0,0,0,false);
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
            boolean a = dropCaches("192.168.50.101", sshUser);
            boolean b = dropCaches("192.168.50.102", sshUser);
            coldFlushOk = a && b;
            Thread.sleep(3000);
        }

        OrientDB       orient   = new OrientDB("remote:192.168.50.102", "root", "rootpwd", OrientDBConfig.defaultConfig());
        ArangoDB       arango   = new ArangoDB.Builder().host("192.168.50.101", 8529).build();
        ArangoDatabase arangoDB = arango.db("KhaBench");

        Stat orientStat = new Stat();
        Stat arangoStat = new Stat();
        AtomicLong northRef  = new AtomicLong(0);
        AtomicLong centerRef = new AtomicLong(0);
        AtomicLong southRef  = new AtomicLong(0);

        List<Long> ryowLats = new ArrayList<>();
        System.out.println("Midiendo RYOW (" + ryowN + " muestras)...");
        for(int r=0; r<ryowN; r++){
            long lat = measureRyow(arangoDB);
            if(lat >= 0) ryowLats.add(lat);
        }

        long deadline = System.currentTimeMillis() + duration * 1000L;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<Void>> futures = new ArrayList<>();

        try {
            for(int t=0; t<threads; t++)
                futures.add(pool.submit(new Worker(orient, arangoDB,
                                                   orientStat, arangoStat,
                                                   northRef, centerRef, southRef,
                                                   deadline)));
            pool.shutdown();
            pool.awaitTermination(duration + 10, TimeUnit.SECONDS);

        } finally {
            orient.close();
            arango.shutdown();
        }

        sampler.running = false;
        samplerThread.join(2000);

        Collections.sort(ryowLats);
        long ryowP50=0, ryowP95=0, ryowP99=0;
        if(!ryowLats.isEmpty()){
            ryowP50 = ryowLats.get(Math.max(0,(int)Math.ceil(0.50*ryowLats.size())-1));
            ryowP95 = ryowLats.get(Math.max(0,(int)Math.ceil(0.95*ryowLats.size())-1));
            ryowP99 = ryowLats.get(Math.max(0,(int)Math.ceil(0.99*ryowLats.size())-1));
        }

        System.out.println("Iteraciones totales (todos los hilos): " + orientStat.lat.size());
        System.out.println("Órdenes post-pandemia por región (última iteración):");
        System.out.println("  NORTE:  " + northRef.get());
        System.out.println("  CENTRO: " + centerRef.get());
        System.out.println("  SUR:    " + southRef.get());
        System.out.printf("RYOW p50=%dms p95=%dms p99=%dms (%d muestras)%n",
                          ryowP50, ryowP95, ryowP99, ryowLats.size());
        System.out.printf("CPU avg=%.1f%% max=%.1f%%  RAM avg=%dMB max=%dMB%n",
                          sampler.avgCpu(), sampler.maxCpu(), sampler.avgMem(), sampler.maxMem());
        System.out.println("=== CONSULTA FINALIZADA ===");

        writeJSON(outDir, dataset, threads, mode, scen, duration, false,
                  orientStat, arangoStat,
                  northRef.get(), centerRef.get(), southRef.get(),
                  (double) duration,
                  sampler.avgCpu(), sampler.maxCpu(), sampler.avgMem(), sampler.maxMem(),
                  ryowP50, ryowP95, ryowP99, ryowLats.size(), coldFlushOk);
    }


    public static Set<String> fetchCustomerIds(OrientDB orient, String className, Stat st) {
        Set<String> ids = new HashSet<>();
        long t = System.currentTimeMillis();
        try(ODatabaseSession db = orient.open("KhaBench", "root", "rootpwd")){
            try(OResultSet rs = db.query("SELECT CUSTOMER_ID FROM " + className)){
                while(rs.hasNext())
                    ids.add(rs.next().getProperty("CUSTOMER_ID"));
            }
        } catch(Exception e){
            st.err();
        }
        st.add(System.currentTimeMillis()-t);
        return ids;
    }

    public static long countOrders(ArangoDatabase db, Set<String> customerIds, Stat st) {
        if(customerIds.isEmpty()) return 0L;

        final int    CHUNK = 1000;
        List<String> ids   = new ArrayList<>(customerIds);
        long         total = 0L;
        long         t     = System.currentTimeMillis();

        String aql =
            "RETURN LENGTH(FOR o IN ORDER_POST_PANDEMIC " +
            "  FILTER PARSE_IDENTIFIER(o.CUSTOMER_ID).key IN @ids " +
            "  COLLECT cust = o.CUSTOMER_ID RETURN cust)";

        try {
            for(int i=0; i<ids.size(); i+=CHUNK){
                List<String> slice = ids.subList(i, Math.min(i+CHUNK, ids.size()));
                Map<String,Object> bind = new HashMap<>();
                bind.put("ids", slice);
                ArangoCursor<Long> cur = db.query(aql, Long.class, bind, new AqlQueryOptions());
                total += cur.hasNext() ? cur.next() : 0L;
            }
        } catch(Exception e){
            st.err();
        }

        st.add(System.currentTimeMillis()-t);
        return total;
    }
}