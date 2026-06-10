package transactions;

import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class C6_ValidateUpdatedFeedback {

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
        final List<Double> cpuSamples  = Collections.synchronizedList(new ArrayList<>());
        final List<Long>   memSamples  = Collections.synchronizedList(new ArrayList<>());

        private long[] readCpuTicks() throws Exception {
            String line = Files.readAllLines(Paths.get("/proc/stat")).get(0);
            String[] p = line.trim().split("\\s+");
            long user=Long.parseLong(p[1]), nice=Long.parseLong(p[2]),
                 sys=Long.parseLong(p[3]),  idle=Long.parseLong(p[4]),
                 iowait=Long.parseLong(p[5]),irq=Long.parseLong(p[6]),
                 softirq=Long.parseLong(p[7]);
            long total = user+nice+sys+idle+iowait+irq+softirq;
            long busy  = total - idle - iowait;
            return new long[]{busy, total};
        }

        private long readMemUsedMb() throws Exception {
            long total=0, available=0;
            for(String line : Files.readAllLines(Paths.get("/proc/meminfo"))){
                if(line.startsWith("MemTotal:"))     total     = Long.parseLong(line.replaceAll("[^0-9]",""));
                if(line.startsWith("MemAvailable:")) available = Long.parseLong(line.replaceAll("[^0-9]",""));
            }
            return (total - available) / 1024;
        }

        @Override
        public void run(){
            try {
                long[] prev = readCpuTicks();
                while(running){
                    Thread.sleep(500);
                    long[] cur = readCpuTicks();
                    long dBusy  = cur[0]-prev[0];
                    long dTotal = cur[1]-prev[1];
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

    static String esc(String s){ return s==null?"null":"\""+s.replace("\\","\\\\").replace("\"","\\\"")+"\""; }

    static void writeJSON(String outDir, String dataset, int threads, String mode, String scen,
                          int duration, boolean warmup, Stat read, int matches, double elapsed,
                          double avgCpu, double maxCpu, long avgMem, long maxMem,
                          long ryowP50, long ryowP95, long ryowP99, int ryowSamples) {
        new File(outDir).mkdirs();
        String path = outDir + "/metrics_raw.json";
        String json =
            "{\n" +
            "  \"meta\": {\"engine\": \"couchbase\", \"tx\": \"C6\", \"dataset\": "+esc(dataset)+", \"threads\": "+threads+",\n" +
            "            \"consistency_mode\": "+esc(mode)+", \"scenario\": "+esc(scen)+", \"duration_s\": "+duration+",\n" +
            "            \"elapsed_s\": "+String.format("%.3f",elapsed)+", \"iterations\": "+read.lat.size()+", \"warmup_only\": "+(warmup?1:0)+"},\n" +
            "  \"ops\": {\"read\": {\"p50\": "+read.pct(0.50)+", \"p95\": "+read.pct(0.95)+", \"p99\": "+read.pct(0.99)+
                       ", \"p999\": "+read.pct(0.999)+", \"avg_ms\": "+String.format("%.2f",read.avg())+
                       ", \"throughput\": "+String.format("%.4f",read.throughput(elapsed))+
                       ", \"samples\": "+read.lat.size()+", \"errors\": "+read.errors.get()+"}},\n" +
            "  \"ryow\": {\"p50_ms\": "+ryowP50+", \"p95_ms\": "+ryowP95+", \"p99_ms\": "+ryowP99+", \"samples\": "+ryowSamples+"},\n" +
            "  \"result\": {\"matches\": "+matches+"},\n" +
            "  \"system\": {\"cpu_avg_pct\": "+String.format("%.2f",avgCpu)+", \"cpu_max_pct\": "+String.format("%.2f",maxCpu)+
                          ", \"mem_avg_mb\": "+avgMem+", \"mem_max_mb\": "+maxMem+", \"disk_iops\": 0, \"disk_lat_ms\": 0, \"net_mbps\": 0}\n" +
            "}\n";
        try(FileWriter w=new FileWriter(path)){ w.write(json); }
        catch(Exception e){ System.err.println("No pude escribir "+path+": "+e.getMessage()); }
    }

    static void flushColdCache(Cluster cluster, String bucketNm) {
        try {
            // Flushing via Management API de Couchbase
            cluster.buckets().flushBucket(bucketNm);
            System.out.println("Cold cache: bucket flushed -> " + bucketNm);
            Thread.sleep(3000); // espera propagación
        } catch(Exception e){
            System.err.println("No se pudo hacer flush del bucket (verifica que flush esté habilitado): " + e.getMessage());
        }
    }


    static long measureRyow(Cluster cluster, String bucketNm, String scopeNm, String collNm) {
        String probeId  = "ryow_probe_" + UUID.randomUUID().toString().substring(0,8);
        String probeVal = "ryow_" + System.currentTimeMillis();
        long   latency  = -1;

        try {
            // Escritura
            cluster.bucket(bucketNm).scope(scopeNm).collection(collNm)
                   .upsert(probeId, JsonObject.create().put("REVIEW", probeVal).put("_ryow", true));

            long t0 = System.currentTimeMillis();

            String n1ql = "SELECT COUNT(*) AS c FROM `"+bucketNm+"`.`"+scopeNm+"`.`"+collNm+
                          "` WHERE META().id = $id";
            QueryOptions opts = QueryOptions.queryOptions()
                .scanConsistency(QueryScanConsistency.REQUEST_PLUS)
                .parameters(JsonObject.create().put("id", probeId));

            QueryResult r = cluster.query(n1ql, opts);
            long found = r.rowsAsObject().get(0).getLong("c");
            latency = System.currentTimeMillis() - t0;

            if(found == 0) latency = -1; 

            cluster.bucket(bucketNm).scope(scopeNm).collection(collNm).remove(probeId);

        } catch(Exception e){
            System.err.println("RYOW probe error: " + e.getMessage());
        }
        return latency;
    }

    static class Worker implements Callable<Void> {
        final Cluster         cluster;
        final String          n1ql;
        final JsonArray       reviewsArr;
        final int             limit;
        final QueryScanConsistency consistency;
        final Stat            read;
        final AtomicInteger   matchesRef;
        final long            deadline;

        Worker(Cluster cluster, String n1ql, JsonArray reviewsArr, int limit,
               QueryScanConsistency consistency, Stat read, AtomicInteger matchesRef, long deadline){
            this.cluster=cluster; this.n1ql=n1ql; this.reviewsArr=reviewsArr;
            this.limit=limit; this.consistency=consistency; this.read=read;
            this.matchesRef=matchesRef; this.deadline=deadline;
        }

        @Override
        public Void call(){
            while(System.currentTimeMillis() < deadline){
                QueryOptions qOpts = QueryOptions.queryOptions()
                    .scanConsistency(consistency)
                    .parameters(JsonObject.create()
                        .put("reviews", reviewsArr)
                        .put("lim", limit));
                try {
                    long tq = System.currentTimeMillis();
                    QueryResult result = cluster.query(n1ql, qOpts);
                    read.add(System.currentTimeMillis()-tq);
                    matchesRef.set(result.rowsAsObject().size());
                } catch(Exception e){
                    read.err();
                }
            }
            return null;
        }
    }

    public static void main(String[] argv) throws Exception {
        Args A = new Args(argv);

        String  outDir   = A.s("out_dir","./out");
        String  dataset  = new File(A.s("dataset_dir","S")).getName();
        int     threads  = A.i("threads", 1);
        int     duration = A.i("duration", 60);
        String  mode     = A.s("consistency_mode","A");
        String  scen     = A.s("scenario","normal");
        boolean warmup   = A.b("warmup_only", false);

        String host     = A.s("couch_host", "192.168.50.103");
        String user     = A.s("couch_user", "Administrator");
        String pwd      = A.s("couch_pwd",  "rootpwd");
        String bucketNm = A.s("bucket",     "KhaBench");
        String scopeNm  = A.s("scope",      "KhaBench");
        String collNm   = A.s("collection", "FEEDBACK");
        int    limit    = A.i("limit", 20);
        int    ryowN    = A.i("ryow_samples", 30);

        String csv = A.s("reviews_csv","Buen producto,Funciona correctamente,Recomendado,Aceptable,Cumple con lo esperado");
        JsonArray reviewsArr = JsonArray.create();
        for(String s : csv.split(",",-1)){ String t=s.trim(); if(!t.isEmpty()) reviewsArr.add(t); }

        System.out.printf("=== C6 ValidateUpdatedFeedback === dataset=%s thr=%d mode=%s scen=%s%n",
                          dataset, threads, mode, scen);

        Stat        read      = new Stat();
        AtomicInteger matches = new AtomicInteger(0);

        if(warmup){
            writeJSON(outDir,dataset,threads,mode,scen,duration,true,read,0,0,0,0,0,0,0,0,0,0);
            System.out.println("Warm-up only. Fin.");
            return;
        }

        String n1ql =
            "SELECT f.PRODUCT_ID, f.CUSTOMER_ID, f.RATE, f.REVIEW, f\n" +
            "FROM `"+bucketNm+"`.`"+scopeNm+"`.`"+collNm+"` AS f\n" +
            "WHERE ARRAY_CONTAINS($reviews, f.REVIEW)\n" +
            "LIMIT $lim";

        QueryScanConsistency consistency = "B".equalsIgnoreCase(mode)
            ? QueryScanConsistency.REQUEST_PLUS
            : QueryScanConsistency.NOT_BOUNDED;

        SysSampler sampler = new SysSampler();
        Thread samplerThread = new Thread(sampler);
        samplerThread.setDaemon(true);
        samplerThread.start();

        Cluster cluster = Cluster.connect("couchbase://"+host, user, pwd);

        try {
            if("cold".equalsIgnoreCase(scen)){
                flushColdCache(cluster, bucketNm);
            }
            List<Long> ryowLats = new ArrayList<>();
            System.out.println("Midiendo RYOW (" + ryowN + " muestras)...");
            for(int r=0; r<ryowN; r++){
                long lat = measureRyow(cluster, bucketNm, scopeNm, collNm);
                if(lat >= 0) ryowLats.add(lat);
            }

            long deadline = System.currentTimeMillis() + duration * 1000L;
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<Void>> futures = new ArrayList<>();

            for(int t=0; t<threads; t++)
                futures.add(pool.submit(new Worker(cluster, n1ql, reviewsArr, limit,
                                                   consistency, read, matches, deadline)));

            pool.shutdown();
            pool.awaitTermination(duration + 10, TimeUnit.SECONDS);

            sampler.running = false;
            samplerThread.join(2000);

            Collections.sort(ryowLats);
            long ryowP50=0, ryowP95=0, ryowP99=0;
            if(!ryowLats.isEmpty()){
                ryowP50 = ryowLats.get(Math.max(0,(int)Math.ceil(0.50*ryowLats.size())-1));
                ryowP95 = ryowLats.get(Math.max(0,(int)Math.ceil(0.95*ryowLats.size())-1));
                ryowP99 = ryowLats.get(Math.max(0,(int)Math.ceil(0.99*ryowLats.size())-1));
            }

            System.out.println("Iteraciones totales (todos los hilos): " + read.lat.size());
            System.out.println("Matches (última iteración): " + matches.get());
            System.out.printf("RYOW p50=%dms p95=%dms p99=%dms (%d muestras)%n",
                              ryowP50, ryowP95, ryowP99, ryowLats.size());
            System.out.printf("CPU avg=%.1f%% max=%.1f%%  RAM avg=%dMB max=%dMB%n",
                              sampler.avgCpu(), sampler.maxCpu(), sampler.avgMem(), sampler.maxMem());

            writeJSON(outDir, dataset, threads, mode, scen, duration, false,
                      read, matches.get(), (double) duration,
                      sampler.avgCpu(), sampler.maxCpu(), sampler.avgMem(), sampler.maxMem(),
                      ryowP50, ryowP95, ryowP99, ryowLats.size());

        } finally {
            cluster.disconnect();
        }
    }
}