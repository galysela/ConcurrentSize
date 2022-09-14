package measurements.adapters;

import algorithms.vcas.VcasBatchBSTMapGC;
import measurements.support.SetInterface;

public class VcasBatchBSTGCAdapter<K extends Comparable<? super K>> extends AbstractAdapter<K> implements SetInterface<K> {
    VcasBatchBSTMapGC<K,K> tree;

    public VcasBatchBSTGCAdapter(int k) {
        tree = new VcasBatchBSTMapGC<K,K>(k);
    }

    public VcasBatchBSTGCAdapter() {
        tree = new VcasBatchBSTMapGC<K,K>();
    }

    @Override
    public boolean contains(K key) {
        return tree.containsKey(key);
    }

    @Override
    public boolean insert(K key) {
        return tree.putIfAbsent(key, key) == null;
    }

    @Override
    public boolean remove(K key) {
        return tree.remove(key) != null;
    }

    @Override
    public int size() {
//        return tree.rangeScan((K) (Integer) Integer.MIN_VALUE, (K) (Integer) Integer.MAX_VALUE).length;
        return tree.snapshotSize();
    }

    @Override
    public long getKeysum() {
        return tree.getSumOfKeys();
    }
}
