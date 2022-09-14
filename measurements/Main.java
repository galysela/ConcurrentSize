/**
 *  This file is based on https://github.com/yuanhaow/vcaslib/blob/main/artifact/java/src/main/Main.java
 */

/**
 * Java test harness for throughput experiments on concurrent data structures.
 * Copyright (C) 2012 Trevor Brown
 * Copyright (C) 2019 Elias Papavasileiou
 * Copyright (C) 2020 Yuanhao Wei
 * Contact (me [at] tbrown [dot] pro) with any questions or comments.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package measurements;

import measurements.adapters.*;
import measurements.support.*;

import algorithms.vcas.Camera;

import java.io.*;
import java.lang.management.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;


public class Main {
    // variables for the experiment
    protected int nthreads;
    protected int numOfSizeWorkers;
    protected int ntrials;
    protected double nseconds;
    protected String filename;
    protected PercentageRatio workloadRatio;
    protected String alg;
    protected int initSize;
    protected boolean prefill;
    protected Integer setParam;
    protected boolean isSplit;

    // some timing variables
    protected AtomicLong startUserTime = new AtomicLong(0);
    protected AtomicLong startWallTime = new AtomicLong(0);

    public Main(int nthreads, int numOfSizeWorkers, int ntrials, double nseconds, String filename,
                PercentageRatio workloadRatio, String alg, int initSize, boolean prefill, Integer setParam, boolean isSplit) {
        this.nthreads = nthreads;
        this.numOfSizeWorkers = numOfSizeWorkers;
        this.ntrials = ntrials;
        this.nseconds = nseconds;
        this.filename = filename;
        this.workloadRatio = workloadRatio;
        this.alg = alg;
        this.initSize = initSize;
        this.prefill = prefill;
        this.setParam = setParam;
        this.isSplit = isSplit;
    }

    public static final class RandomKeyGenerator {
        final Random rng;
        final int maxKey;

        public RandomKeyGenerator(final Random rng, final int maxKey) {
            this.rng = rng;
            if (maxKey < 0) throw new RuntimeException("maxKey cannot be negative");
            this.maxKey = maxKey;
        }

        public Integer next() {
            return rng.nextNatural(maxKey)+1;
        }
    }

    public final class RandomKeyGeneratorFactory {
        ArrayList<RandomKeyGenerator> getGenerators(Experiment ex, java.util.Random rng) {
            ArrayList<RandomKeyGenerator> arrays = new ArrayList<>(nthreads);
            for (int i = 0; i<nthreads; i++) {
                arrays.add(new RandomKeyGenerator(new Random(rng.nextInt()), ex.maxKey));
            }
            return arrays;
        }
    }

    public abstract static class Worker extends Thread {
        public abstract long getTrueIns();
        public abstract long getFalseIns();
        public abstract long getTrueDel();
        public abstract long getFalseDel();
        public abstract long getTrueFind();
        public abstract long getFalseFind();
        public abstract long getDoneSize();
        public abstract long getMyStartCPUTime();
        public abstract long getMyStartUserTime();
        public abstract long getMyStartWallTime();
        public abstract long getUserTime();
        public abstract long getWallTime();
        public abstract long getCPUTime();
        public abstract long getKeysum();
        public abstract long getInsTime();
        public abstract long getDelTime();
        public abstract long getContainsTime();
    }

    public class TimedWorker<K extends Comparable<? super K>> extends Worker {
        CyclicBarrier start;
        RandomKeyGenerator keyGen;
        AbstractAdapter<K> set;
        long trueDel, falseDel, trueIns, falseIns, trueFind, falseFind, doneSize;
        long keysum; // sum of new keys inserted by this thread minus keys deleted by this thread
        final Experiment ex;
        final PercentageRatio percentageRatio;
        Random rng;

        public final AtomicLong sharedStartUserTime;
        public final AtomicLong sharedStartWallTime;
        public long myStartCPUTime;
        public long myStartUserTime;
        public long myStartWallTime;
        public long cpuTime;
        public long userTime;
        public long wallTime;
        public long insTime;
        public long delTime;
        public long containsTime;

        final int threadID;

        final private boolean isSplit;
        final int numOpRepeatsIfSplit = 100;

        public TimedWorker(final RandomKeyGenerator keyGen,
                           final Experiment ex,
                           final PercentageRatio percentageRatio,
                           final Random rng,
                           final AbstractAdapter<K> set,
                           final CyclicBarrier start,
                           final AtomicLong sharedStart,
                           final AtomicLong sharedStartWallTime,
                           final int threadID,
                           final boolean isSplit) {
            this.keyGen = keyGen;
            this.ex = ex;
            this.percentageRatio = percentageRatio;
            this.rng = rng;
            this.set = set;
            this.start = start;
            this.sharedStartUserTime = sharedStart;
            this.sharedStartWallTime = sharedStartWallTime;
            this.threadID = threadID;
            this.insTime = 0;
            this.delTime = 0;
            this.containsTime = 0;
            this.isSplit = isSplit;
        }

        @SuppressWarnings("unchecked")
        private void executeWorkloadOp(double op) {
            final Integer keyInt = keyGen.next();
            final K key = (K) keyInt;
            if (op < percentageRatio.size + percentageRatio.ins) {
                if (set.insert(key)) {
                    keysum += keyInt;
                    trueIns++;
                } else falseIns++;
            } else if (op < percentageRatio.size + percentageRatio.ins + percentageRatio.del) {
                if (set.remove(key)) {
                    keysum -= keyInt;
                    trueDel++;
                } else falseDel++;
            } else {
                if (set.contains(key)) trueFind++;
                else falseFind++;
            }
        }

        private void executeSizeOp() {
            set.size();
            doneSize++;
        }

        @Override
        @SuppressWarnings("empty-statement")
        public final void run() {
            ThreadID.threadID.set(threadID);
            ThreadMXBean bean = ManagementFactory.getThreadMXBean();
            if (!bean.isCurrentThreadCpuTimeSupported()) {
                System.out.println("ERROR: THREAD CPU TIME UNSUPPORTED");
                System.exit(-1);
            }
            if (!bean.isThreadCpuTimeEnabled()) {
                System.out.println("ERROR: THREAD CPU TIME DISABLED");
                System.exit(-1);
            }
            long id = Thread.currentThread().getId();

            // everyone waits on barrier
            if (start != null) try { start.await(); } catch (Exception e) { e.printStackTrace(); System.exit(-1); }

            // everyone waits until main thread sets experiment state to RUNNING
            while (!ex.isRunning);

            // start timing
            myStartUserTime = bean.getThreadUserTime(id);
            myStartCPUTime = bean.getThreadCpuTime(id);
            myStartWallTime = System.nanoTime();
            sharedStartUserTime.compareAndSet(0, myStartUserTime);
            sharedStartWallTime.compareAndSet(0, myStartWallTime);

            // perform operations while experiment's state is running
            while (ex.isRunning) {
                final double op = Math.abs(100 * (rng.nextNatural() / (double) Integer.MAX_VALUE)); // Multiply by 100 to turn the fraction into percentages
                if (!isSplit) {
                    if (op < percentageRatio.size) {
                        executeSizeOp();
                    } else {
                        executeWorkloadOp(op);
                    }
                } else {
                    if (op < percentageRatio.size) {
                        for (int i = 0; i < numOpRepeatsIfSplit; i++) {
                            executeSizeOp();
                        }
                    } else {
                        long timeBefore = bean.getThreadCpuTime(id);
                        for (int i = 0; i < numOpRepeatsIfSplit; i++) {
                            executeWorkloadOp(op);
                        }
                        long totalTime = bean.getThreadCpuTime(id) - timeBefore;
                        if (op < percentageRatio.size) {
                        } else if (op < percentageRatio.size + percentageRatio.ins) {
                            this.insTime += totalTime;
                        } else if (op < percentageRatio.size + percentageRatio.ins + percentageRatio.del) {
                            this.delTime += totalTime;
                        } else {
                            this.containsTime += totalTime;
                        }
                    }
                }
            }

            // finish timing
            wallTime = System.nanoTime();
            userTime = bean.getThreadUserTime(id);
            cpuTime = bean.getThreadCpuTime(id);
        }

        public long getTrueIns() { return trueIns; }
        public long getFalseIns() { return falseIns; }
        public long getTrueDel() { return trueDel; }
        public long getFalseDel() { return falseDel; }
        public long getTrueFind() { return trueFind; }
        public long getFalseFind() { return falseFind; }
        public long getDoneSize() { return doneSize; }
        public long getMyStartCPUTime() { return myStartCPUTime; }
        public long getMyStartUserTime() { return myStartUserTime; }
        public long getMyStartWallTime() { return myStartWallTime; }
        public long getUserTime() { return userTime; }
        public long getWallTime() { return wallTime; }
        public long getCPUTime() { return cpuTime; }
        public long getKeysum() { return keysum; }
        public long getInsTime() { return insTime; }
        public long getDelTime() { return delTime; }
        public long getContainsTime() { return containsTime; }
    }

    static final class FixedNumberOfKeysWorker<K extends Comparable<? super K>> extends Thread {
        final AbstractAdapter<K> set;
        final RandomKeyGenerator keyGen;
        final int maxKey;
        final int keysNum;
        long keysum;
        final int threadID;

        public FixedNumberOfKeysWorker(
                final AbstractAdapter<K> set,
                final int maxKey,
                final int keysNum,
                final Random rng,
                final int threadID) {
            this.set = set;
            this.maxKey = maxKey;
            this.keysNum = keysNum;
            this.keyGen = new RandomKeyGenerator(rng, maxKey);
            this.threadID = threadID;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            ThreadID.threadID.set(threadID);
            int keysAdded = 0;
            while (keysAdded < keysNum) {
                int key = keyGen.next();
                if (set.insert((K) (Integer) key)) {
                    keysum += key;
                    keysAdded++;
                }
            }
        }

        public long getKeysum() {
            return keysum;
        }
    }

    protected boolean runTrial(
            final PrintStream out,
            final String prefix,
            final SizeKeysumPair pair,
            final java.util.Random rng,
            final AbstractAdapter<Integer> set,
            final Experiment ex,
            final boolean isSplit) {
        // prepare worker threads to run the trial
        startWallTime = new AtomicLong(0);
        startUserTime = new AtomicLong(0);
        CyclicBarrier start = new CyclicBarrier(nthreads+1);
        ArrayList<RandomKeyGenerator> arrays = ex.factory.getGenerators(ex, rng); // key generators supply keys for each thread
        ArrayList<Worker> workers = new ArrayList<>(nthreads); // these are the experiment threads
        for (int i = 0; i<nthreads-numOfSizeWorkers; i++) { // workload threads
            workers.add(new TimedWorker<>(arrays.get(i), ex, ex.workloadRatio, new Random(rng.nextInt()), set, start, startUserTime, startWallTime, i, isSplit));
        }
        for (int i = nthreads-numOfSizeWorkers; i<nthreads; i++) { // size threads
            workers.add(new TimedWorker<>(arrays.get(i), ex, new PercentageRatio(0, 0, 100), new Random(rng.nextInt()), set, start, startUserTime, startWallTime, i, isSplit));
        }

        // perform garbage collection to clean up after the last trial, and record how much GC has happened so far
        System.gc();
        final long gcTimeStart = totalGarbageCollectionTimeMillis();

        // run the trial
        for (int i = 0; i<nthreads; i++) workers.get(i).start();
        try { start.await(); } catch (Exception e) { e.printStackTrace(); System.exit(-1); }
        ex.isRunning = true;
        long localStartTime = System.currentTimeMillis();
        long localEndTime;

        try {
            Thread.sleep((long)(nseconds * 1e3));
        } catch (InterruptedException ex1) {
            ex1.printStackTrace();
            System.exit(-1);
        }

        localEndTime = System.currentTimeMillis();
        ex.isRunning = false;

        // stop all threads and record how much GC has happened so far
        try { for (int i = 0; i<nthreads; i++) workers.get(i).join(); }
        catch (InterruptedException e) { e.printStackTrace(); System.exit(-1); }
        final long gcTimeEnd = totalGarbageCollectionTimeMillis();

        // compute key checksum for all threads (including from prefilling)
        long threadsKeysum = pair.keysum;
        for (int i = 0; i<nthreads; ++i) {
            threadsKeysum += workers.get(i).getKeysum();
        }

        ThreadID.threadID.set(0);

        // check size
        long setSize = getSize(set);
        if (setSize != -1) { // else, size method is not supported, so skip size validation
            long ntrueins = 0, ntruedel = 0;
            for (Worker w : workers) {
                ntrueins += w.getTrueIns();
                ntruedel += w.getTrueDel();
            }
            if (pair.setSize + ntrueins - ntruedel != setSize) {
                System.out.println("ERROR: expected_size=" + (pair.setSize + ntrueins - ntruedel) + " does not match set.size()=" + setSize);
            }
        }

        // check keysum
        if ((long) ex.initSize < 5000000) { // else, set is too large, so skip keysum validation
            long dsKeysum = set.getKeysum();
            if (dsKeysum != threadsKeysum) {
                System.out.println("ERROR: threadsKeysum=" + threadsKeysum + " does not match dsKeysum=" + dsKeysum);
            }
        }

        // produce output
        double elapsed = (localEndTime - localStartTime)/1e3;
        out.print(prefix + ",");
        long nsize = 0, ntrueins = 0, nfalseins = 0, ntruedel = 0, nfalsedel = 0, ntruefind = 0, nfalsefind = 0;
        for (Worker w : workers) {
            ntrueins += w.getTrueIns();
            nfalseins += w.getFalseIns();
            ntruedel += w.getTrueDel();
            nfalsedel += w.getFalseDel();
            ntruefind += w.getTrueFind();
            nfalsefind += w.getFalseFind();
            nsize += w.getDoneSize();
        }

        int finalnnodes = getSize(set);

        long fidOps = ntrueins+ntruedel+ntruefind + nfalseins+nfalsedel+nfalsefind;
        long totalOps = fidOps + nsize;

        double totalThroughput = totalOps/elapsed;
        double fidThroughput = fidOps/elapsed;
        double sizeThroughput = nsize/elapsed;

        out.print((nthreads-numOfSizeWorkers) + "," + numOfSizeWorkers + "," + totalOps + "," +
                Math.round(totalThroughput) + "," + Math.round(fidThroughput) + "," + Math.round(sizeThroughput) + "," +
                ex.initSize + "," + ex.workloadRatio + "," + elapsed + "," +
                ntrueins + "," + nfalseins + "," + ntruedel + "," + nfalsedel + "," +
                ntruefind + "," + nfalsefind + "," + nsize + "," +
                finalnnodes);

        // compute minimum starting times for all threads
        long minStartUserTime = Long.MAX_VALUE;
        long minStartWallTime = Long.MAX_VALUE;
        long minStartCPUTime = Long.MAX_VALUE;
        for (Worker w : workers) {
            minStartUserTime = Math.min(minStartUserTime, w.getMyStartUserTime());
            minStartWallTime = Math.min(minStartWallTime, w.getMyStartWallTime());
            minStartCPUTime = Math.min(minStartCPUTime, w.getMyStartCPUTime());
        }

        // elapsed time per thread
        long totalElapsedUserTime = 0, totalElapsedWallTime = 0, totalElapsedCPUTime = 0;
        long totalElapsedInsTime = 0, totalElapsedDelTime = 0, totalElapsedContainsTime = 0;
        for (Worker w : workers) {
            totalElapsedUserTime += w.getUserTime()-w.getMyStartUserTime();
            totalElapsedWallTime += w.getWallTime()-w.getMyStartWallTime();
            totalElapsedCPUTime += w.getCPUTime()-w.getMyStartCPUTime();
            totalElapsedInsTime += w.getInsTime();
            totalElapsedDelTime += w.getDelTime();
            totalElapsedContainsTime += w.getContainsTime();
        }
        // garbage collection time
        final double gcElapsedTime = (gcTimeEnd-gcTimeStart)/1e3;
        // total time for all threads in trial
        double totalThreadTime = (((totalElapsedCPUTime/1e9)+ 0 /*liveThreadsElapsedCPUTime*/)/nthreads +gcElapsedTime);

        // total elapsed times
        out.print(","+totalElapsedUserTime/1e9);
        out.print(","+totalElapsedWallTime/1e9);
        out.print(","+totalElapsedCPUTime/1e9);
        out.print(","+totalElapsedInsTime/1e9);
        out.print(","+totalElapsedDelTime/1e9);
        out.print(","+totalElapsedContainsTime/1e9);

        out.print(","+gcElapsedTime);
        out.print(","+nseconds);
        out.print(","+totalThreadTime);

        for (Worker w : workers) {
            long ops = w.getTrueIns() + w.getFalseIns() + w.getTrueDel() + w.getFalseDel() + w.getTrueFind() +
                    w.getFalseFind() + w.getDoneSize();
            out.print(","+ops);
        }

        // user start+end times per thread
        for (Worker w : workers) {
            out.print(","+((w.getMyStartUserTime()-minStartUserTime)/1e9)+","+((w.getUserTime()-minStartUserTime)/1e9));
        }

        // wall start+end times per thread
        for (Worker w : workers) {
            out.print(","+((w.getMyStartWallTime()-minStartWallTime)/1e9)+","+((w.getWallTime()-minStartWallTime)/1e9));
        }

        // CPU start+end times per thread
        for (Worker w : workers) {
            out.print(","+((w.getMyStartCPUTime()-minStartCPUTime)/1e9)+","+((w.getCPUTime()-minStartCPUTime)/1e9));
        }

        // ins time per thread
        for (Worker w : workers) {
            out.print(","+(w.getInsTime()/1e9));
        }

        // del time per thread
        for (Worker w : workers) {
            out.print(","+(w.getDelTime()/1e9));
        }

        // contains time per thread
        for (Worker w : workers) {
            out.print(","+(w.getContainsTime()/1e9));
        }

        out.println(); // finished line of output

        return true;
    }

    private int getSize(AbstractAdapter<Integer> set) {
        try {
            return set.size();
        } catch(UnsupportedOperationException e) {
            return -1;
        }
    }

    private long totalGarbageCollectionTimeMillis() {
        final List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans();
        long result = 0;
        for (GarbageCollectorMXBean gcbean : gcbeans) {
            result += gcbean.getCollectionTime();
        }
        return result;
    }

    protected static final class PercentageRatio {
        final int del, ins, size;
        public PercentageRatio(final int ins, final int del, final int size) {
            if (ins < 0 || del < 0 || size < 0 || ins+del+size > 100) throw new RuntimeException("invalid percentageRatio " + ins + "i-" + del + "d-" + size + "size");
            this.del = del;
            this.ins = ins;
            this.size = size;
        }
        @Override
        public String toString() { return "" + ins + "i-" + del + "d-" + (100-ins-del-size) + "f"; }
    }

    public final class Experiment {
        volatile boolean isRunning = false;
        final String alg;
        final Integer param;
        final int initSize;
        final int maxKey;
        final PercentageRatio workloadRatio;
        final RandomKeyGeneratorFactory factory;

        public Experiment(final String alg, final Integer param, final int initSize, final PercentageRatio workloadRatio, final RandomKeyGeneratorFactory factory) {
            this.alg = alg;
            this.param = param;
            this.initSize = initSize;
            this.workloadRatio = workloadRatio;
            this.factory = factory;
            if (workloadRatio.ins == 0 && workloadRatio.del == 0) {
                // deal with an all-search workload by choosing maxKey according to 50% insert, 50% delete, namely, twice the set size
                maxKey = 2 * initSize;
            } else {
                maxKey = (int) Math.round(initSize * ((workloadRatio.ins + workloadRatio.del) / (double) workloadRatio.ins));
            }
        }
        @Override
        public String toString() {
            return alg + (param == null ? "" : ("-" + param)) + "-" + (nthreads-numOfSizeWorkers) + "workloadThreads-" + numOfSizeWorkers + "sizeThreads-" + initSize + "setSize-" + workloadRatio;
        }
    }

    protected static class DualPrintStream {
        private final PrintStream stdout;
        private PrintStream fileout;
        public DualPrintStream(String filename) throws IOException {
            if (filename != null) {
                fileout = new PrintStream(new FileOutputStream(filename));
            }
            stdout = System.out;
        }
        public void print(double x) {
            print(String.valueOf(x));
        }
        public void println(double x) {
            println(String.valueOf(x));
        }
        public void print(String x) {
            stdout.print(x);
            if (fileout != null) fileout.print(x);
        }
        public void println(String x) {
            print(x + "\n");
        }
    }

    public static class SizeKeysumPair {
        public final long setSize;
        public final long keysum;
        public SizeKeysumPair(long setSize, long keysum) {
            this.setSize = setSize;
            this.keysum = keysum;
        }
    }

    @SuppressWarnings("unchecked")
    SizeKeysumPair parallelFillToSteadyState(
            final java.util.Random rand,
            final SetInterface<Integer> set,
            int initSize,
            int maxKey) {
        long keysum = 0;
        int numThreads = Runtime.getRuntime().availableProcessors()-4;
        if(numThreads < 1) numThreads = 1;

        int sizeRemainder = initSize % numThreads;
        int keysNum = 0;

        final FixedNumberOfKeysWorker<Integer>[] workers = new FixedNumberOfKeysWorker[numThreads];
        for (int i=0;i<numThreads;i++) {
            int currentKeysNum = initSize/numThreads + (i<sizeRemainder ? 1 : 0);
            workers[i] = new FixedNumberOfKeysWorker<>((AbstractAdapter<Integer>) set, maxKey, currentKeysNum, new Random(rand.nextInt()), i);
            keysNum += currentKeysNum;
        }
        assert keysNum == initSize;
        for (int i=0;i<numThreads;i++) workers[i].start();
        try { for (int i=0;i<numThreads;i++) workers[i].join(); }
        catch (InterruptedException e) { e.printStackTrace(); System.exit(-1); }

        for (int i=0;i<numThreads;i++) {
            keysum += workers[i].getKeysum();
        }

        return new SizeKeysumPair(initSize, keysum);
    }

    protected ArrayList<Experiment> getExperiments() {
        final ArrayList<Experiment> exp = new ArrayList<>();
        RandomKeyGeneratorFactory keyGen = new RandomKeyGeneratorFactory();
        exp.add(new Experiment(alg, setParam, initSize, workloadRatio, keyGen));
        return exp;
    }

    @SuppressWarnings("unchecked")
    public void run() {
        // create output streams
        PrintStream out = null;
        if (filename == null) {
            out = System.out;
        } else {
            try { out = new PrintStream(filename); }
            catch (Exception e) { e.printStackTrace(); System.exit(-1); }
        }
        DualPrintStream stdout = null;
        try { stdout = new DualPrintStream(filename + "_stdout"); } catch (Exception e) { e.printStackTrace(); System.exit(-1); }

        // print header
        out.print("name"
                + ",trial"
                + ",nWorkloadThreads"
                + ",nSizeThreads"
                + ",totalOps"
                + ",totalThroughput"
                + ",workloadThreadsThroughput"
                + ",sizeThreadsThroughput"
                + ",initSize"
                + ",percentageRatio"
                + ",time"
                + ",ninstrue"
                + ",ninsfalse"
                + ",ndeltrue"
                + ",ndelfalse"
                + ",ncontainstrue"
                + ",ncontainsfalse"
                + ",nsize"
                + ",finalnnodes"
                );
        out.print(",totalelapsedusertime");
        out.print(",totalelapsedwalltime");
        out.print(",totalelapsedcputime");
        out.print(",totalelapsedinstime");
        out.print(",totalelapseddeltime");
        out.print(",totalelapsedcontainstime");
        out.print(",gctime");
        out.print(",nseconds");
        out.print(",effectivetimeperthread");
        for (int i=0;i<nthreads;i++) out.print(",thread"+i+"ops");
        for (int i=0;i<nthreads;i++) out.print(",thread"+i+"userstart"+",thread"+i+"userend");
        for (int i=0;i<nthreads;i++) out.print(",thread"+i+"wallstart"+",thread"+i+"wallend");
        for (int i=0;i<nthreads;i++) out.print(",thread"+i+"cpustart"+",thread"+i+"cpuend");
        for (int i=0;i<nthreads;i++) out.print(",thread"+i+"instime");
        for (int i=0;i<nthreads;i++) out.print(",thread"+i+"deltime");
        for (int i=0;i<nthreads;i++) out.print(",thread"+i+"containstime");
        out.println();

        // retrieve list of experiments to perform
        ArrayList<Experiment> exp = getExperiments();

        // perform the experiment
        Random rng = new Random((int) System.nanoTime()); // produce a seed from current time
        for (Experiment ex : exp) {
            int experimentSeed = rng.nextInt();
            java.util.Random experimentRng = new java.util.Random(experimentSeed);

            // find appropriate factory to produce the set we want for this trial
            // and run the trial
            for (SetFactory<Integer> factory : Factories.factories) if (ex.alg.equals(factory.getName())) {
                stdout.println("Running " + ex);
                for (int trial=0;trial<ntrials;++trial) {
                    stdout.print(".");
                    Camera.camera.timestamp = 0;
                    System.gc();
                    SetInterface<Integer> set = factory.newSet(ex.param);
                    SizeKeysumPair p = new SizeKeysumPair(0, 0);
                    if (prefill) {
                        p = parallelFillToSteadyState(experimentRng, set, ex.initSize, ex.maxKey);
                    }
                    String name = factory.getName();
                    if (name.contains("Batch") && ex.param != null) {
                        name += ex.param;
                    }
                    if (!runTrial(out, name + "," + trial, p, experimentRng, (AbstractAdapter<Integer>) set, ex, isSplit)) System.exit(-1);
                }
                stdout.println("");
            }
        }
    }

    public static void invokeRun(String[] args) {
        if (args.length == 1 && args[0].equals("test")) {
            System.out.println("Running Tests...");
            Tests.runTests();
            return;
        }

        if (args.length < 4) {
            System.out.println("ERROR: Insufficient command-line arguments.");
            System.out.println("Must include: #NUMBER_OF_WORKLOAD_THREADS #NUMBER_OF_SIZE_THREADS #TRIALS SECONDS_PER_TRIAL ALGORITHM");
            System.out.print("Valid algorithms are:");
            for (SetFactory<Integer> f : Factories.factories) System.out.print(" " + f.getName());
            System.out.println();
            System.out.println("Can also include switches after mandatory arguments:");
            System.out.println("\t-prefill  to prefill structures to steady state with random operations");
            System.out.println("\t-file-### to specify an output file to store results in");
            System.out.println("\t-param-## to provide an int parameter that will be passed to the set factory");
            System.out.println("The following switches determine which operations are run (leftover % becomes contains):");
            System.out.println("\t-ins%     to specify what % (0 to 100) of ops should be inserts");
            System.out.println("\t-del%     to specify what % (0 to 100) of ops should be deletes");
            System.out.println("\t-initSizeN    the set will be initialized with N elements");
            System.out.println("\t-split  to split time counting per operation type");
            System.exit(-1);
        }
        int numOfWorkloadWorkers = 0;
        int numOfSizeWorkers = 0;
        int ntrials = 0;
        double nseconds = 0;
        String filename = null;
        String alg = "";
        boolean prefill = false;
        int insPercent = 0;
        int remPercent = 0;
        int initSize = 0;
        Integer setParam = null;
        boolean isSplit = false;

        try {
            numOfWorkloadWorkers = Integer.parseInt(args[0]);
            numOfSizeWorkers = Integer.parseInt(args[1]);
            ntrials = Integer.parseInt(args[2]);
            nseconds = Double.parseDouble(args[3]);
            alg = args[4];
        } catch (Exception ex) {
            System.out.println("ERROR: NUMBER_OF_WORKLOAD_THREADS, NUMBER_OF_SIZE_THREADS, NUMBER_OF_TRIALS, SECONDS_PER_TRIAL must all be numeric");
            System.exit(-1);
        }
        if (numOfWorkloadWorkers < 0 /*|| numOfWorkloadWorkers > THREAD_LIMIT*/) {
            System.out.println("ERROR: Number of workload threads must be >= 0"/* <= " + THREAD_LIMIT + " (or else we'll crash MTL)"*/);
            System.exit(-1);
        }
        if (numOfSizeWorkers < 0 /*|| numOfSizeWorkers > THREAD_LIMIT*/) {
            System.out.println("ERROR: Number of size threads must be >= 0"/* <= " + THREAD_LIMIT + " (or else we'll crash MTL)"*/);
            System.exit(-1);
        }
        if (ntrials <= 0) {
            System.out.println("ERROR: Must run at least 1 trial (recommended to run several and discard the first few)");
            System.exit(-1);
        }
        if (nseconds <= 0) {
            System.out.println("ERROR: Number of seconds per trial s must satisfy 0 < s (should be at least a second, really)");
            System.exit(-1);
        }
        if (alg == null || alg.length() == 0) {
            System.out.println("ERROR: alg cannot be blank or null");
            System.exit(-1);
        }

        int totalOpPercent = 0;
        for (String arg : args) {
            if (arg.startsWith("-")) {
                if (arg.matches("-ins[0-9]+")) {
                    try {
                        insPercent = Integer.parseInt(arg.substring(4));
                        if (insPercent < 0) {
                            System.out.println("ERROR: The insert percentage must be >= 0");
                            System.exit(-1);
                        }
                        totalOpPercent += insPercent;
                    } catch (Exception ex) {
                        System.out.println("ERROR: The insert percentage must be a 32-bit integer.");
                        System.exit(-1);
                    }
                } else if (arg.matches("-del[0-9]+")) {
                    try {
                        remPercent = Integer.parseInt(arg.substring(4));
                        if (remPercent < 0) {
                            System.out.println("ERROR: The delete percentage must be >= 0");
                            System.exit(-1);
                        }
                        totalOpPercent += remPercent;
                    } catch (Exception ex) {
                        System.out.println("ERROR: The delete percentage must be a 32-bit integer.");
                        System.exit(-1);
                    }
                } else if (arg.matches("-initSize[0-9]+")) {
                    try {
                        initSize = Integer.parseInt(arg.substring(9));
                        if (initSize < 1) {
                            System.out.println("ERROR: The initial set size must be > 0");
                            System.exit(-1);
                        }
                    } catch (Exception ex) {
                        System.out.println("ERROR: The initial set size must be a 32-bit integer.");
                        System.exit(-1);
                    }
                } else if (arg.startsWith("-param-")) {
                    try {
                        setParam = Integer.parseInt(arg.substring("-param-".length()));
                    } catch (Exception ex) {
                        System.out.println("ERROR: The set parameter must be a 32-bit integer.");
                        System.exit(-1);
                    }
                } else if (arg.startsWith("-file-")) {
                    filename = arg.substring("-file-".length());
                } else if (arg.matches("-prefill")) {
                    prefill = true;
                } else if (arg.matches("-split")) {
                    isSplit = true;
                } else {
                    System.out.println("ERROR: Unrecognized command-line switch: \"" + arg + "\"");
                    System.exit(-1);
                }
            }
        }

        if (totalOpPercent > 100) {
            System.out.println("ERROR: Total percentage over all operations cannot exceed 100");
            System.exit(-1);
        }

        boolean found = false;
        for (SetFactory<Integer> f : Factories.factories) {
            String name = f.getName();
            if (name.equals(alg)) {
                found = true;
                break;
            }
        }
        if (!found) {
            System.out.println("ERROR: Algorithm \"" + alg + "\" was not recognized.");
            System.out.println("Run this class with no arguments to see a list of valid algorithms.");
            System.exit(-1);
        }

        if (alg.contains("HashTable")) {
            // Pick the requested table size to be as the initial size
            setParam = initSize;
        }

        (new Main(numOfWorkloadWorkers+numOfSizeWorkers, numOfSizeWorkers, ntrials, nseconds, filename,
                new PercentageRatio(insPercent, remPercent, 0),
                alg, initSize, prefill, setParam, isSplit)).run();
    }

    public static void main(String[] args) throws Exception {
        invokeRun(args);
    }
}
