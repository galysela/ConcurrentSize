package measurements.support;

import measurements.adapters.*;
import java.util.ArrayList;

public class Factories {
    // central list of factory classes for all supported data structures
    public static final ArrayList<SetFactory<Integer>> factories =
            new ArrayList<SetFactory<Integer>>();
    static {
        factories.add(new ConcurrentSkipListMapFactory<Integer>());
        factories.add(new SizeConcurrentSkipListMapFactory<Integer>());

        factories.add(new LockFreeBSTFactory<Integer>());
        factories.add(new SizeBSTFactory<Integer>());

        factories.add(new HashTableFactory<Integer>());
        factories.add(new SizeHashTableFactory<Integer>());

        factories.add(new IteratorSkipListFactory<Integer>());

        factories.add(new VcasBatchBSTGCFactory<Integer>());
    }

    // factory classes for each supported data structure

    protected static class LockFreeBSTFactory<K extends Comparable<? super K>> extends SetFactory<K> {
        public SetInterface<K> newSet(final Integer param) {
            return new BSTAdapter<K>();
        }
        public String getName() { return "BST"; }
    }

    protected static class VcasBatchBSTGCFactory<K extends Comparable<? super K>> extends SetFactory<K> {
        Object param;
        public SetInterface<K> newSet(final Integer param) {
            this.param = param;
            return param == null ? new VcasBatchBSTGCAdapter<K>()
                    : new VcasBatchBSTGCAdapter<K>(param);
        }
        public String getName() { return "VcasBatchBSTGC"; }
    }

    protected static class ConcurrentSkipListMapFactory<K extends Comparable<? super K>> extends SetFactory<K> {
        public SetInterface<K> newSet(final Integer param) {
            return new SkipListAdapter<K>();
        }
        public String getName() { return "SkipList"; }
    }

    protected static class SizeConcurrentSkipListMapFactory<K extends Comparable<? super K>> extends SetFactory<K> {
        public SetInterface<K> newSet(final Integer param) {
            return new SizeSkipListAdapter<K>();
        }
        public String getName() { return "SizeSkipList"; }
    }

    protected static class SizeBSTFactory<K extends Comparable<? super K>> extends SetFactory<K> {
        public SetInterface<K> newSet(final Integer param) {
            return new SizeBSTAdapter<K>();
        }
        public String getName() { return "SizeBST"; }
    }

    protected static class HashTableFactory<K extends Comparable<? super K>> extends SetFactory<K> {
        public SetInterface<K> newSet(final Integer param) {
            if (param == null) throw new NullPointerException();
            return new HashTableAdapter<K>(param);
        }
        public String getName() { return "HashTable"; }
    }

    protected static class SizeHashTableFactory<K extends Comparable<? super K>> extends SetFactory<K> {
        public SetInterface<K> newSet(final Integer param) {
            if (param == null) throw new NullPointerException();
            return new SizeHashTableAdapter<K>(param);
        }
        public String getName() { return "SizeHashTable"; }
    }

    protected static class IteratorSkipListFactory<K extends Comparable<? super K>> extends SetFactory<K> {
        public SetInterface<K> newSet(final Integer param) {
            return new IteratorSkipListAdapter<K>();
        }
        public String getName() { return "IteratorSkipList"; }
    }
}