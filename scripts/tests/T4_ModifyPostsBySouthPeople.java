package transactions;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.AqlQueryOptions;
import com.arangodb.model.TransactionOptions;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class T4_ModifyPostsBySouthPeople {

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
            "  \"meta\": {\"engine\": \"arangodb\", \"tx\": \"T4\", \"dataset\": "+esc(dataset)+", \"threads\": "+threads+",\n" +
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
        final ArangoDatabase db;
        final String postCol, edgeCol, postKey, newContent, mode;
        final boolean requireSouth, dryRun;
        final long probeTimeoutMs, deadline;
        final Stat read, write;
        final Ryow ryow;

        Worker(ArangoDatabase db, String postCol, String edgeCol, String postKey,
               String newContent, String mode, boolean requireSouth, boolean dryRun,
               long probeTimeoutMs, long deadline, Stat read, Stat write, Ryow ryow){
            this.db=db; this.postCol=postCol; this.edgeCol=edgeCol; this.postKey=postKey;
            this.newContent=newContent; this.mode=mode;
            this.requireSouth=requireSouth; this.dryRun=dryRun;
            this.probeTimeoutMs=probeTimeoutMs; this.deadline=deadline;
            this.read=read; this.write=write; this.ryow=ryow;
        }

        @Override
        public Void call(){
            while(System.currentTimeMillis() < deadline){
                try {
                    if(!postExists(db, postCol, postKey, read)) continue;

                    String personKey = findSouthCreatorForPost(db, edgeCol, postCol, postKey, read);
                    if(personKey == null && requireSouth) continue;

                    if(!dryRun){
                        String action = """
                          function (params) {
                            const db = require('@arangodb').db;
                            const errors = require('@arangodb').errors;
                            const col = db._collection(params.postCol);
                            const id  = params.postCol + "/" + String(params.key);
                            const newContent = String(params.newContent ?? "");
                            if (!db._exists(id)) return { updated:false, reason:"post not found" };
                            const maxRetries = 5;
                            for (let attempt = 1; attempt <= maxRetries; attempt++) {
                              try {
                                const doc = db._document(id);
                                const current = (doc.CONTENT ?? "");
                                if (current === newContent) return { updated:false, reason:"same content" };
                                db._update(id, { CONTENT:newContent, LENGTH:newContent.length },
                                           { waitForSync:true, ignoreRevs:true });
                                return { updated:true };
                              } catch (e) {
                                if (e.isArangoError && e.errorNum === errors.ERROR_ARANGO_CONFLICT.code) {
                                  require('internal').sleep(0.05 * attempt);
                                  continue;
                                }
                                return { updated:false, error:true, errorMessage:e.message };
                              }
                            }
                            return { updated:false, reason:"conflict-retries-exhausted" };
                          }
                        """;
                        TransactionOptions opts = new TransactionOptions()
                            .readCollections(postCol, edgeCol)
                            .writeCollections(postCol)
                            .waitForSync("B".equalsIgnoreCase(mode))
                            .params(Map.of("postCol",postCol,"key",postKey,"newContent",newContent));

                        long tw = System.currentTimeMillis();
                        db.transaction(action, Map.class, opts);
                        write.add(System.currentTimeMillis()-tw);

                        long start = System.currentTimeMillis(); boolean ok=false;
                        while(System.currentTimeMillis()-start < probeTimeoutMs){
                            BaseDocument cur = getPost(db, postCol, postKey, read);
                            if(newContent.equals(getAttrString(cur,"CONTENT"))){ ok=true; break; }
                            try{ Thread.sleep(10); }catch(InterruptedException ignore){}
                        }
                        ryow.probe(ok, System.currentTimeMillis()-start);
                    }
                } catch(Exception e){
                    write.err();
                }
            }
            return null;
        }
    }

    public static void main(String[] argv) throws Exception {
        Args A = new Args(argv);

        String  outDir       = A.s("out_dir","./out");
        String  dataset      = new File(A.s("dataset_dir","S")).getName();
        int     threads      = A.i("threads", 1);
        int     duration     = A.i("duration", 60);
        String  mode         = A.s("consistency_mode","A");
        String  scen         = A.s("scenario","normal");
        boolean warmup       = A.b("warmup_only", false);
        String  aHost        = A.s("arango_host","192.168.50.101");
        int     aPort        = A.i("arango_port", 8529);
        String  aUser        = A.s("arango_user","root");
        String  aPwd         = A.s("arango_pwd","rootpwd");
        String  aDbNm        = A.s("arango_db","KhaBench");
        String  postCol      = A.s("post_col","POST");
        String  edgeCol      = A.s("edge_col","POST_HAS_CREATOR_PERSON_SOUTH");
        String  postKey      = A.s("post_key","687197694388");
        String  newContent   = A.s("new_content","Me gustó mucho y lo recomiendo");
        boolean requireSouth = A.b("require_south", false);
        boolean dryRun       = A.b("dry_run", false);
        String  sshUser      = A.s("ssh_user","pi");

        System.out.printf("=== T4 ModifyPostsBySouthPeople === dataset=%s thr=%d mode=%s scen=%s post=%s dry=%s%n",
                          dataset, threads, mode, scen, postKey, dryRun);

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
            System.out.println("Cold cache: limpiando nodo...");
            coldFlushOk = dropCaches(aHost, sshUser);
            Thread.sleep(3000);
        }

        ArangoDB arango = new ArangoDB.Builder()
            .host(aHost, aPort).user(aUser).password(aPwd).build();
        ArangoDatabase db = arango.db(aDbNm);

        Stat read=new Stat(), write=new Stat();
        Ryow ryow=new Ryow();

        long deadline = System.currentTimeMillis() + duration * 1000L;
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        try {
            for(int t=0; t<threads; t++)
                pool.submit(new Worker(db, postCol, edgeCol, postKey, newContent,
                                       mode, requireSouth, dryRun,
                                       500L, deadline, read, write, ryow));
            pool.shutdown();
            pool.awaitTermination(duration+10, TimeUnit.SECONDS);
        } finally {
            arango.shutdown();
        }

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

    private static boolean postExists(ArangoDatabase db, String postCol, String postKey, Stat st){
        long t = System.currentTimeMillis();
        List<Boolean> r = db.query("RETURN DOCUMENT(@id) != null", Boolean.class,
            Map.of("id", postCol+"/"+postKey), new AqlQueryOptions()).asListRemaining();
        st.add(System.currentTimeMillis()-t);
        return !r.isEmpty() && Boolean.TRUE.equals(r.get(0));
    }

    private static String findSouthCreatorForPost(ArangoDatabase db, String edgeCol,
                                                   String postCol, String postKey, Stat st){
        String aql = """
          LET pid = CONCAT(@postCol, "/", @k)
          LET out = FIRST(FOR e IN @@edge FILTER e._from == pid RETURN PARSE_IDENTIFIER(e._to).key)
          RETURN out ? out : FIRST(FOR e IN @@edge FILTER e._to == pid RETURN PARSE_IDENTIFIER(e._from).key)
        """;
        Map<String,Object> bind = new HashMap<>();
        bind.put("postCol", postCol); bind.put("k", postKey); bind.put("@edge", edgeCol);
        long t = System.currentTimeMillis();
        List<String> res = db.query(aql, String.class, bind, new AqlQueryOptions()).asListRemaining();
        st.add(System.currentTimeMillis()-t);
        return res.isEmpty() ? null : res.get(0);
    }

    private static BaseDocument getPost(ArangoDatabase db, String postCol, String postKey, Stat st){
        long t = System.currentTimeMillis();
        List<BaseDocument> rows = db.query(
            "FOR p IN @@col FILTER p._key == @k LIMIT 1 RETURN p",
            BaseDocument.class, Map.of("@col",postCol,"k",postKey), new AqlQueryOptions()).asListRemaining();
        st.add(System.currentTimeMillis()-t);
        return rows.isEmpty() ? new BaseDocument() : rows.get(0);
    }

    private static String getAttrString(BaseDocument d, String attr){
        Object v = d==null ? null : d.getAttribute(attr);
        return v==null ? null : String.valueOf(v);
    }
}