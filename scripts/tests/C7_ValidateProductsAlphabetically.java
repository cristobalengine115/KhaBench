package transactions;

import com.couchbase.client.java.*;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class C7_ValidateProductsAlphabetically {

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
                          int duration, boolean warmup, Stat read, int matches,
                          double elapsed, double avgCpu, double maxCpu, long avgMem, long maxMem,
                          boolean coldFlushOk){
        new File(outDir).mkdirs();
        String path = outDir + "/metrics_raw.json";
        String json =
            "{\n" +
            "  \"meta\": {\"engine\": \"couchbase\", \"tx\": \"C7\", \"dataset\": "+esc(dataset)+", \"threads\": "+threads+",\n" +
            "            \"consistency_mode\": "+esc(mode)+", \"scenario\": "+esc(scen)+", \"duration_s\": "+duration+",\n" +
            "            \"elapsed_s\": "+String.format("%.3f",elapsed)+", \"iterations\": "+read.lat.size()+",\n" +
            "            \"cold_flush_ok\": "+(coldFlushOk?1:0)+", \"warmup_only\": "+(warmup?1:0)+"},\n" +
            "  \"ops\": {\"read\": {\"p50\": "+read.pct(0.50)+", \"p95\": "+read.pct(0.95)+", \"p99\": "+read.pct(0.99)+
                       ", \"p999\": "+read.pct(0.999)+", \"avg_ms\": "+String.format("%.2f",read.avg())+
                       ", \"throughput\": "+String.format("%.4f",read.throughput(elapsed))+
                       ", \"samples\": "+read.lat.size()+", \"errors\": "+read.errors.get()+"}},\n" +
            "  \"result\": {\"matches\": "+matches+"},\n" +
            "  \"system\": {\"cpu_avg_pct\": "+String.format("%.2f",avgCpu)+", \"cpu_max_pct\": "+String.format("%.2f",maxCpu)+
                          ", \"mem_avg_mb\": "+avgMem+", \"mem_max_mb\": "+maxMem+
                          ", \"disk_iops\": 0, \"disk_lat_ms\": 0, \"net_mbps\": 0}\n" +
            "}\n";
        try(FileWriter w=new FileWriter(path)){ w.write(json); }
        catch(Exception e){ System.err.println("No pude escribir "+path+": "+e.getMessage()); }
    }

    static class Worker implements Callable<Void> {
        final Cluster   cluster;
        final String    n1ql;
        final String    startL, endL;
        final int       limit;
        final QueryScanConsistency consistency;
        final Stat      read;
        final AtomicInteger matchesRef;
        final long      deadline;

        Worker(Cluster cluster, String n1ql, String startL, String endL, int limit,
               QueryScanConsistency consistency, Stat read, AtomicInteger matchesRef, long deadline){
            this.cluster=cluster; this.n1ql=n1ql; this.startL=startL; this.endL=endL;
            this.limit=limit; this.consistency=consistency; this.read=read;
            this.matchesRef=matchesRef; this.deadline=deadline;
        }

        @Override
        public Void call(){
            while(System.currentTimeMillis() < deadline){
                QueryOptions qOpts = QueryOptions.queryOptions()
                    .scanConsistency(consistency)
                    .parameters(JsonObject.create()
                        .put("start", startL)
                        .put("end",   endL)
                        .put("lim",   limit));
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
        String  host     = A.s("couch_host", "192.168.50.103");
        String  user     = A.s("couch_user", "Administrator");
        String  pwd      = A.s("couch_pwd",  "rootpwd");
        String  bucketNm = A.s("bucket",     "KhaBench");
        String  scopeNm  = A.s("scope",      "KhaBench");
        String  collNm   = A.s("collection", "PRODUCT");
        String  startL   = A.s("start_letter","M");
        String  endL     = A.s("end_letter",  "Z");
        int     limit    = A.i("limit", 50);
        String  sshUser  = A.s("ssh_user", "pi");

        System.out.printf("=== C7 ValidateProductsAlphabetically === dataset=%s thr=%d mode=%s scen=%s range=%s..%s%n",
                          dataset, threads, mode, scen, startL, endL);

        Stat        read    = new Stat();
        AtomicInteger matches = new AtomicInteger(0);

        if(warmup){
            writeJSON(outDir,dataset,threads,mode,scen,duration,true,read,0,0,0,0,0,0,false);
            System.out.println("Warm-up only. Fin.");
            return;
        }

        SysSampler sampler = new SysSampler();
        Thread samplerThread = new Thread(sampler);
        samplerThread.setDaemon(true);
        samplerThread.start();

        boolean coldFlushOk = false;
        if("cold".equalsIgnoreCase(scen)){
            System.out.println("Cold cache: limpiando nodo...");
            coldFlushOk = dropCaches(host, sshUser);
            Thread.sleep(3000);
        }

        String n1ql =
            "SELECT p.TITLE\n" +
            "FROM `"+bucketNm+"`.`"+scopeNm+"`.`"+collNm+"` AS p\n" +
            "WHERE p.TITLE >= $start AND p.TITLE <= $end\n" +
            "ORDER BY p.TITLE ASC\n" +
            "LIMIT $lim";

        QueryScanConsistency consistency = "B".equalsIgnoreCase(mode)
            ? QueryScanConsistency.REQUEST_PLUS
            : QueryScanConsistency.NOT_BOUNDED;

        Cluster cluster = Cluster.connect("couchbase://"+host, user, pwd);
        long deadline = System.currentTimeMillis() + duration * 1000L;
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        try {
            for(int t=0; t<threads; t++)
                pool.submit(new Worker(cluster, n1ql, startL, endL, limit,
                                       consistency, read, matches, deadline));
            pool.shutdown();
            pool.awaitTermination(duration+10, TimeUnit.SECONDS);
        } finally {
            cluster.disconnect();
        }

        sampler.running = false;
        samplerThread.join(2000);

        System.out.println("Iteraciones totales (todos los hilos): " + read.lat.size());
        System.out.println("Matches (última iteración): " + matches.get());
        System.out.printf("CPU avg=%.1f%% max=%.1f%%  RAM avg=%dMB max=%dMB%n",
                          sampler.avgCpu(), sampler.maxCpu(), sampler.avgMem(), sampler.maxMem());

        writeJSON(outDir, dataset, threads, mode, scen, duration, false,
                  read, matches.get(), (double) duration,
                  sampler.avgCpu(), sampler.maxCpu(), sampler.avgMem(), sampler.maxMem(),
                  coldFlushOk);
    }
}