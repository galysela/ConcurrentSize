/**
 * Java test harness for throughput experiments on concurrent data structures.
 * Copyright (C) 2012 Trevor Brown
 * Copyright (C) 2019 Elias Papavasileiou
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

package measurements.support;

import measurements.adapters.*;

public class Tests {
    static volatile boolean shouldRun = false;
    static volatile boolean DEBUG_PRINTS = false;

    private static final int NUM_THREADS;
    static{
        int _numThreads = Runtime.getRuntime().availableProcessors() - 4;
        if (_numThreads < 1) _numThreads = 2;
        NUM_THREADS = _numThreads;
    }

    private static int[] TARGET_SIZES_PER_THREAD = {2, 10, 1000};
    private static double[] MAX_KEY_RATIOS = {1.1, 2, 3};

    static final class FixedNumberOfKeysWorker<K extends Comparable<? super K>> extends Thread {
        final AbstractAdapter<K> set;
        final int keysNum;
        final int minKey;
        final int maxKey;
        final Random rng;
        final int threadID;
        long keysum;

        public FixedNumberOfKeysWorker(
                final AbstractAdapter<K> set,
                final int keysNum,
                final int minKey,
                final int maxKey,
                final Random rng,
                final int threadID) {
            this.set = set;
            this.keysNum = keysNum;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.rng = rng;
            this.threadID = threadID;
        }

        @Override
        public void run() {
            ThreadID.threadID.set(threadID);
            int keysAdded = 0;
            while (keysAdded < keysNum) {
                int key = rng.nextNatural(maxKey-minKey+1) + minKey;
                if (set.insert((K) (Integer) key)) {
                    keysum += key;
                    keysAdded++;
                    assert set.contains((K) (Integer) key);
                }
            }
        }

        public long getKeysum() {
            return keysum;
        }
    }

    static final class AlternatelyInsertRemoveWorker<K extends Comparable<? super K>> extends Thread {
        final AbstractAdapter<K> set;
        final int minKey;
        final int maxKey;
        final Random rng;
        final int threadID;
        boolean withContains;
        long keysum;

        public AlternatelyInsertRemoveWorker(
                final AbstractAdapter<K> set,
                final int minKey,
                final int maxKey,
                final Random rng,
                final int threadID,
                final boolean withContains) {
            this.set = set;
            this.minKey = minKey;
            this.maxKey = maxKey;
            this.rng = rng;
            this.threadID = threadID;
            this.withContains = withContains;
        }

        @Override
        public void run() {
            ThreadID.threadID.set(threadID);
            while (!shouldRun);
            while (shouldRun) {
                int key;

                // insert:
                for (;;) {
                    key = rng.nextNatural(maxKey-minKey+1) + minKey;
                    boolean isInSet = false;
                    if (withContains)
                        isInSet = set.contains((K) (Integer) key);
                    boolean didInsert = set.insert((K) (Integer) key);
                    if (withContains) {
                        assert didInsert != isInSet;
                        assert set.contains((K) (Integer) key);
                    }
                    if (didInsert) {
                        keysum += key;
                        break;
                    }
                }

                // remove:
                for (;;) {
                    key = rng.nextNatural(maxKey-minKey+1) + minKey;
                    boolean isInSet = false;
                    if (withContains)
                        isInSet = set.contains((K) (Integer) key);
                    boolean didRemove = set.remove((K) (Integer) key);
                    if (withContains) {
                        assert didRemove == isInSet;
                        assert !set.contains((K) (Integer) key);
                    }
                    if (didRemove) {
                        keysum -= key;
                        break;
                    }
                }
            }
        }

        public long getKeysum() {
            return keysum;
        }
    }

    static final class EmptyingWorker<K extends Comparable<? super K>> extends Thread {
        final AbstractAdapter<K> set;
        final int keysNum;
        final int maxKey;
        final Random rng;
        final int threadID;
        long keysum;

        public EmptyingWorker(
                final AbstractAdapter<K> set,
                final int keysNum,
                final int maxKey,
                final Random rng,
                final int threadID) {
            this.set = set;
            this.keysNum = keysNum;
            this.maxKey = maxKey;
            this.rng = rng;
            this.threadID = threadID;
        }

        @Override
        public void run() {
            ThreadID.threadID.set(threadID);
            int keysRemoved = 0;
            while (keysRemoved < keysNum) {
                int key = rng.nextNatural(maxKey) + 1;
                if (set.remove((K) (Integer) key)) {
                    keysum += key;
                    keysRemoved++;
                    assert !set.contains((K) (Integer) key);
                }
            }
        }

        public long getKeysum() {
            return keysum;
        }
    }

    static final class IncreasingSizeVerifier<K extends Comparable<? super K>> extends Thread {
        final AbstractAdapter<K> set;
        final int keysNum;
        final int threadID;

        public IncreasingSizeVerifier(
                final AbstractAdapter<K> set,
                final int keysNum,
                final int threadID) {
            this.set = set;
            this.keysNum = keysNum;
            this.threadID = threadID;
        }

        @Override
        public void run() {
            ThreadID.threadID.set(threadID);
            int size = 0;
            int prevSize = 0;
            while (size < keysNum) {
                size = set.size();
                assert size >= prevSize;
                prevSize = size;
            }
        }
    }

    static final class DecreasingSizeVerifier<K extends Comparable<? super K>> extends Thread {
        final AbstractAdapter<K> set;
        final int keysNum;
        final int threadID;

        public DecreasingSizeVerifier(
                final AbstractAdapter<K> set,
                final int keysNum,
                final int threadID) {
            this.set = set;
            this.keysNum = keysNum;
            this.threadID = threadID;
        }

        @Override
        public void run() {
            ThreadID.threadID.set(threadID);
            int size = set.size();
            int prevSize = size;
            while (size > keysNum) {
                size = set.size();
                assert size <= prevSize;
                prevSize = size;
            }
        }
    }

    static final class SizeRangeVerifier<K extends Comparable<? super K>> extends Thread {
        final AbstractAdapter<K> set;
        final int minSize;
        final int maxSize;
        final int threadID;

        public SizeRangeVerifier(
                final AbstractAdapter<K> set,
                final int minSize,
                final int maxSize,
                final int threadID) {
            this.set = set;
            this.minSize = minSize;
            this.maxSize = maxSize;
            this.threadID = threadID;
        }

        @Override
        public void run() {
            ThreadID.threadID.set(threadID);
            while (!shouldRun);
            while (shouldRun) {
                int size = set.size();
                assert size >= minSize;
                assert size <= maxSize;
            }
        }
    }

    static final class InsertOneItemWorker extends Thread {
        final AbstractAdapter<Integer> set;
        final int threadID;

        public InsertOneItemWorker(
                final AbstractAdapter<Integer> set,
                final int threadID) {
            this.set = set;
            this.threadID = threadID;
        }

        @Override
        public void run() {
            ThreadID.threadID.set(threadID);
            while (!shouldRun);
            assert set.insert(threadID);
        }
    }

    static final class ContainsAndSizeWorker extends Thread {
        final AbstractAdapter<Integer> set;
        final int threadID;

        public ContainsAndSizeWorker(
                final AbstractAdapter<Integer> set,
                final int threadID) {
            this.set = set;
            this.threadID = threadID;
        }

        @Override
        public void run() {
            ThreadID.threadID.set(threadID);
            while (!shouldRun);
            int foundKeys = 0;
            for (int i = 0; i < threadID; ++i)
                foundKeys += (set.contains(i) ? 1 : 0);
            int size = set.size();
//            if (foundKeys > size) System.out.println("foundKeys=" + foundKeys + " size=" + size);
            assert foundKeys <= size;
            assert size <= threadID;
        }
    }

    static void insertDeleteOneKey(AbstractAdapter<Integer> set) {
        assert set.insert(10);
        assert set.contains(10);
        assert !set.insert(10);
        assert set.contains(10);
        assert set.remove(10);
        assert !set.remove(10);
        assert !set.contains(10);
        if (DEBUG_PRINTS)
            System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
    }

    static void insertDeleteTwoKeys(AbstractAdapter<Integer> set) {
        assert set.insert(10);
        assert set.contains(10);
        assert set.insert(15);
        assert set.contains(15);
        assert !set.insert(10);
        assert !set.insert(15);
        assert set.remove(10);
        assert !set.contains(10);
        assert set.contains(15);
        assert !set.remove(10);
        assert set.remove(15);
        assert !set.contains(15);
        assert !set.remove(15);
        if (DEBUG_PRINTS)
            System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
    }

    static void insertDeleteSeveralKeys1(AbstractAdapter<Integer> set, boolean isSizeSupported) {
        int keysInset = 0;
        int key1 = 11, key2 = 20, key3 = 30;
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert !set.remove(key1);
        assert !set.contains(key1);
        assert set.insert(key1);
        keysInset++;
        assert set.insert(key3);
        assert set.contains(key3);
        keysInset++;
        assert set.insert(key2);
        assert set.contains(key2);
        keysInset++;
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert set.remove(key1);
        keysInset--;
        assert !set.contains(key1);
        assert !set.contains(key1);
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert !set.contains(key1);
        assert set.contains(key2);
        assert set.contains(key3);
        assert !set.remove(key1);
        assert set.contains(key2);
        assert set.contains(key3);
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert set.remove(key2);
        keysInset--;
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert set.remove(key3);
        keysInset--;
        if (isSizeSupported)
            assert set.size() == keysInset;
        if (DEBUG_PRINTS)
            System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
    }

    static void insertDeleteSeveralKeys2(AbstractAdapter<Integer> set, boolean isSizeSupported) {
        int keysInset = 0;
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert !set.contains(10);
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert set.insert(10);
        keysInset++;
        assert set.insert(15);
        keysInset++;
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert set.remove(15);
        keysInset--;
        assert !set.remove(15);
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert !set.contains(15);
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert set.insert(5);
        keysInset++;
        assert set.remove(5);
        keysInset--;
        if (isSizeSupported)
            assert set.size() == keysInset;
		assert set.insert(20);
        keysInset++;
		assert set.insert(15);
        keysInset++;
		assert set.insert(25);
        keysInset++;
		assert !set.remove(5);
        if (isSizeSupported)
            assert set.size() == keysInset;
		assert set.remove(25);
        keysInset--;
        if (isSizeSupported)
            assert set.size() == keysInset;
        assert set.contains(20);
        assert set.remove(20);
        assert !set.contains(20);
        assert set.contains(15);
        assert set.contains(10);
        assert set.remove(10);
        assert set.contains(15);
        assert !set.contains(10);
        assert set.remove(15);
        assert !set.contains(15);

        if (DEBUG_PRINTS)
            System.out.println(new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
    }

    static long fill(AbstractAdapter<Integer> set, int numKeys, int maxKey, java.util.Random rand, final boolean isSizeSupported, final boolean separateRanges) {
        long keysum = 0;
        IncreasingSizeVerifier increasingSizeVerifier = null;

        final FixedNumberOfKeysWorker[] fillerWorkers = new FixedNumberOfKeysWorker[NUM_THREADS];
        int rangeSize = maxKey / NUM_THREADS;
        assert rangeSize > 0;
        for (int i = 0; i < NUM_THREADS; i++) {
            int minKeyForThread = 1;
            int maxKeyForThread = maxKey;
            if (separateRanges) {
                // create a separate key range for each thread, so that an equal number of elements are filled in each range and no range gets full, so that inserts in following tests may succeed
                minKeyForThread = 1 + i * rangeSize;
                maxKeyForThread = (i + 1) * rangeSize;
            }
            fillerWorkers[i] = new FixedNumberOfKeysWorker((AbstractAdapter) set, numKeys / NUM_THREADS, minKeyForThread, maxKeyForThread, new Random(rand.nextInt()), i);
        }

        if (isSizeSupported) {
            increasingSizeVerifier = new IncreasingSizeVerifier((AbstractAdapter) set, numKeys, NUM_THREADS);

            increasingSizeVerifier.start();
        }

        for (int i = 0; i < NUM_THREADS; i++) fillerWorkers[i].start();

        if (isSizeSupported) {
            try {
                increasingSizeVerifier.join();
            }
            catch (InterruptedException e) { e.printStackTrace(); System.exit(-1); }
        }

        try {
            for (int i = 0; i < NUM_THREADS; i++) fillerWorkers[i].join();
        }
        catch (InterruptedException e) { e.printStackTrace(); System.exit(-1); }

        for (int i = 0; i < NUM_THREADS; i++) {
            keysum += fillerWorkers[i].getKeysum();
        }

        if (isSizeSupported) {
            assert numKeys == set.size();
        }

        if (DEBUG_PRINTS)
            System.out.println("  " + new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");
        return keysum;
    }

    static long empty(AbstractAdapter<Integer> set, int numKeysToRemove, int initialSize, int maxKey, java.util.Random rand, final boolean isSizeSupported) {
        long keysum = 0;
        int targetSize = initialSize - numKeysToRemove;
        DecreasingSizeVerifier decreasingSizeVerifier = null;

        final EmptyingWorker[] emptyingWorkers = new EmptyingWorker[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            emptyingWorkers[i] = new EmptyingWorker((AbstractAdapter) set, numKeysToRemove / NUM_THREADS, maxKey, new Random(rand.nextInt()), i);
        }

        if (isSizeSupported) {
            decreasingSizeVerifier = new DecreasingSizeVerifier((AbstractAdapter) set, targetSize, NUM_THREADS);

            decreasingSizeVerifier.start();
        }

        for (int i = 0; i < NUM_THREADS; i++) emptyingWorkers[i].start();

        if (isSizeSupported) {
            try {
                decreasingSizeVerifier.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        try {
            for (int i = 0; i < NUM_THREADS; i++) emptyingWorkers[i].join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            keysum += emptyingWorkers[i].getKeysum();
        }

        if (isSizeSupported) {
            assert targetSize == set.size();
        }

        if (DEBUG_PRINTS)
            System.out.println("  " + new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");

        return keysum;
    }

    static long insertRemoveAlternately(AbstractAdapter<Integer> set, int initialSize, int maxKey, java.util.Random rand, final boolean isSizeSupported, final boolean separateRanges) {
        long keysum = 0;
        SizeRangeVerifier sizeRangeVerifier = null;

        final AlternatelyInsertRemoveWorker[] alternatelyInsertRemoveWorkers = new AlternatelyInsertRemoveWorker[NUM_THREADS];
        int rangeSize = maxKey / NUM_THREADS;
        assert rangeSize > 0;
        for (int i = 0; i < NUM_THREADS; i++) {
            int minKeyForThread = 1;
            int maxKeyForThread = maxKey;
            if (separateRanges) {
                // create a separate key range for each thread, so that it can anticipate the result of contains following an insert or remove.
                minKeyForThread = 1 + i * rangeSize;
                maxKeyForThread = (i + 1) * rangeSize;
            }
            alternatelyInsertRemoveWorkers[i] = new AlternatelyInsertRemoveWorker((AbstractAdapter) set, minKeyForThread, maxKeyForThread, new Random(rand.nextInt()), i, separateRanges);
        }
        if (isSizeSupported) {
            sizeRangeVerifier = new SizeRangeVerifier((AbstractAdapter) set, initialSize, initialSize + NUM_THREADS, NUM_THREADS);
        }

        if (isSizeSupported) {
            sizeRangeVerifier.start();
        }

        for (int i = 0; i < NUM_THREADS; i++) alternatelyInsertRemoveWorkers[i].start();

        shouldRun = true;

        final int milliseconds = 500;
        try {
            Thread.sleep((long)(milliseconds));
        } catch (InterruptedException ex1) {
            ex1.printStackTrace();
            System.exit(-1);
        }
        shouldRun = false;

        if (isSizeSupported) {
            try {
                sizeRangeVerifier.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        try {
            for (int i = 0; i < NUM_THREADS; i++) alternatelyInsertRemoveWorkers[i].join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            keysum += alternatelyInsertRemoveWorkers[i].getKeysum();
        }

        if (isSizeSupported) {
            assert set.size() == initialSize;
        }

        if (DEBUG_PRINTS)
            System.out.println("  " + new Object(){}.getClass().getEnclosingMethod().getName() + ": OK");

        return keysum;
    }

    // This is the example in our paper's introduction, which exposes that java.util.concurrent.ConcurrentSkipListMap is not linearizable
    static void sizeConsistentWithContains(AbstractAdapter<Integer> set) {
        final InsertOneItemWorker[] insertOneItemWorker = new InsertOneItemWorker[NUM_THREADS-1];
        for (int i = 0; i < NUM_THREADS - 1; i++) {
            insertOneItemWorker[i] = new InsertOneItemWorker((AbstractAdapter) set, i);
        }
        ContainsAndSizeWorker containsAndSizeWorker = new ContainsAndSizeWorker((AbstractAdapter) set, NUM_THREADS - 1);

        containsAndSizeWorker.start();
        for (int i = 0; i < NUM_THREADS - 1; i++) insertOneItemWorker[i].start();

        shouldRun = true;

        try {
            containsAndSizeWorker.join();
            for (int i = 0; i < NUM_THREADS - 1; i++) insertOneItemWorker[i].join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        shouldRun = false;

        for (int i = 0; i < NUM_THREADS - 1; i++) assert set.remove(i);
        assert set.size() == 0;
    }

    private static void runTests(AbstractAdapter<Integer> set) {
        // Check if set supports size
        boolean isSizeSupported = false;
        try {
            set.size();
            isSizeSupported = true;
        } catch (UnsupportedOperationException e) {
            System.out.println("[Not testing size - size not supported]");
        }

        Random rng = new Random((int) System.nanoTime()); // produce a seed from current time
        int experimentSeed = rng.nextInt();
        java.util.Random experimentRng = new java.util.Random(experimentSeed);

        insertDeleteOneKey(set);
        insertDeleteTwoKeys(set);
        insertDeleteSeveralKeys1(set, isSizeSupported);
        insertDeleteSeveralKeys2(set, isSizeSupported);

        if (isSizeSupported) {
            assert set.size() == 0;
            for (int i = 0; i < 100; ++i)
                sizeConsistentWithContains(set);
        }

        for (int i = 0; i < TARGET_SIZES_PER_THREAD.length; ++i) {
            for (int j = 0; j < MAX_KEY_RATIOS.length; ++j) {
                int targetTotalSize = TARGET_SIZES_PER_THREAD[i] * NUM_THREADS;

                int maxKey = (int) (targetTotalSize * MAX_KEY_RATIOS[j]);
                if (maxKey < targetTotalSize + 1) {
                    maxKey = targetTotalSize + 1;
                    // Otherwise, it will be impossible to insert when the set is of size targetTotalSize after filling.
                }

                if (DEBUG_PRINTS)
                    System.out.println("Testing with targetTotalSize=" + targetTotalSize + " and maxKey=" + maxKey + ":");

                assert fill(set, targetTotalSize, maxKey, experimentRng, isSizeSupported, false) +
                        insertRemoveAlternately(set, targetTotalSize, maxKey, experimentRng, isSizeSupported, false) ==
                        empty(set, targetTotalSize, targetTotalSize, maxKey, experimentRng, isSizeSupported);
                if (isSizeSupported)
                    assert set.size() == 0;

                maxKey = (int) (targetTotalSize * MAX_KEY_RATIOS[j]);
                if (maxKey < targetTotalSize + NUM_THREADS) {
                    maxKey = targetTotalSize + NUM_THREADS;
                    // Otherwise, it will be impossible to insert in the full sub-ranges after filling.
                }

                if (DEBUG_PRINTS)
                    System.out.println("Testing threads on separate ranges with targetTotalSize=" + targetTotalSize + " and maxKey=" + maxKey + ":");

                assert fill(set, targetTotalSize, maxKey, experimentRng, isSizeSupported, true) +
                        insertRemoveAlternately(set, targetTotalSize, maxKey, experimentRng, isSizeSupported, true) ==
                        empty(set, targetTotalSize, targetTotalSize, maxKey, experimentRng, isSizeSupported);
                if (isSizeSupported)
                    assert set.size() == 0;
            }
        }
    }

    public static void runTests() {
        int[] setParam = {2, 16, 64};
        ThreadID.threadID.set(ThreadID.MAX_THREADS-1);
        System.out.println("Set DEBUG_PRINTS=true for verbose test prints.");

        System.out.println("[Running tests with up to " + (NUM_THREADS + 1) + " worker threads.]");
        System.out.println();

        // Run tests
        for (SetFactory<Integer> set : Factories.factories) {
            if (set.getName().contains("Batch")) {
                for (int i = 0; i < setParam.length; i++) {
                    System.out.println("[*] Testing " + set.getName() + " Batch Size " + setParam[i] + " ...");
                    AbstractAdapter<Integer> setAdapter = (AbstractAdapter<Integer>) set.newSet(setParam[i]);
                    runTests(setAdapter);
                    System.out.println();
                }
            }
            else if (set.getName().contains("HashTable")) {
                for (int targetSizePerThread : TARGET_SIZES_PER_THREAD) {
                    int tableSize = targetSizePerThread * NUM_THREADS;
                    System.out.println("[*] Testing " + set.getName() + " Table Size " + tableSize + " ...");
                    AbstractAdapter<Integer> setAdapter = (AbstractAdapter<Integer>) set.newSet(tableSize);
                    runTests(setAdapter);
                    System.out.println();
                }
            }
            else {
                System.out.println("[*] Testing " + set.getName() + " ...");
                AbstractAdapter<Integer> setAdapter = (AbstractAdapter<Integer>) set.newSet(null);
                runTests(setAdapter);
                System.out.println();
            }          
        }        
    }
}
