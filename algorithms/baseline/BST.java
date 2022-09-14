package algorithms.baseline;

/**
 *  This file is a variant of https://bitbucket.org/trbot86/implementations/src/master/java/src/algorithms/published/LockFreeBSTMap.java
 */

/**
 *  This is an implementation of the non-blocking, concurrent binary search tree of
 *  Faith Ellen, Panagiota Fatourou, Eric Ruppert and Franck van Breugel.
 *
 *  Copyright (C) 2011  Trevor Brown, Joanna Helga
 *  Contact Trevor Brown (tabrown@cs.toronto.edu) with any questions or comments.
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public class BST<K extends Comparable<? super K>, V> {
    //--------------------------------------------------------------------------------
    // Class: Node, LeafNode, InternalNode
    //--------------------------------------------------------------------------------
    protected static abstract class Node<E extends Comparable<? super E>, V> {
        final E key;

        Node(final E key) {
            this.key = key;
        }
    }

    protected final static class LeafNode<E extends Comparable<? super E>, V> extends Node<E,V> {
        final V value;

        LeafNode(final E key, final V value) {
            super(key);
            this.value = value;
        }
    }

    protected final static class InternalNode<E extends Comparable<? super E>, V> extends Node<E,V> {
        volatile Node<E,V> left;
        volatile Node<E,V> right;
        volatile Info<E,V> info;

        InternalNode(final E key, final LeafNode<E,V> left, final LeafNode<E,V> right) {
            super(key);
            this.left = left;
            this.right = right;
            this.info = null;
        }
    }

    //--------------------------------------------------------------------------------
    // Class: Info, DInfo, IInfo, Mark, Clean
    // May 25th: trying to make CAS to update field static
    // instead of using <state, Info>, we extends Info to all 4 states
    // to see a state of a node, see what kind of Info class it has
    //--------------------------------------------------------------------------------
    protected static abstract class Info<E extends Comparable<? super E>, V> {
    }

    protected final static class DInfo<E extends Comparable<? super E>, V> extends Info<E,V> {
        final InternalNode<E,V> p;
        final LeafNode<E,V> l;
        final InternalNode<E,V> gp;
        final Info<E,V> pinfo;

        DInfo(final LeafNode<E,V> leaf, final InternalNode<E,V> parent, final InternalNode<E,V> grandparent, final Info<E,V> pinfo) {
            this.p = parent;
            this.l = leaf;
            this.gp = grandparent;
            this.pinfo = pinfo;
        }
    }

    protected final static class IInfo<E extends Comparable<? super E>, V> extends Info<E,V> {
        final InternalNode<E,V> p;
        final LeafNode<E,V> l;
        final Node<E,V> lReplacingNode;

        IInfo(final LeafNode<E,V> leaf, final InternalNode<E,V> parent, final Node<E,V> lReplacingNode){
            this.p = parent;
            this.l = leaf;
            this.lReplacingNode = lReplacingNode;
        }
    }

    protected final static class Mark<E extends Comparable<? super E>, V> extends Info<E,V> {
        final DInfo<E,V> dinfo;

        Mark(final DInfo<E,V> dinfo) {
            this.dinfo = dinfo;
        }
    }

    protected final static class Clean<E extends Comparable<? super E>, V> extends Info<E,V> {}

//--------------------------------------------------------------------------------
// DICTIONARY
//--------------------------------------------------------------------------------
    private static final AtomicReferenceFieldUpdater<InternalNode, Node> leftUpdater = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Node.class, "left");
    private static final AtomicReferenceFieldUpdater<InternalNode, Node> rightUpdater = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Node.class, "right");
    private static final AtomicReferenceFieldUpdater<InternalNode, Info> infoUpdater = AtomicReferenceFieldUpdater.newUpdater(InternalNode.class, Info.class, "info");

    final InternalNode<K,V> root;

    public BST() {
        // to avoid handling special case when <= 2 nodes,
        // create 2 dummy nodes, both contain key null
        // All real keys inside BST are required to be non-null
        root = new InternalNode<K,V>(null, new LeafNode<K,V>(null, null), new LeafNode<K,V>(null, null));
    }

//--------------------------------------------------------------------------------
// PUBLIC METHODS:
// - find   : boolean
// - insert : boolean
// - delete : boolean
//--------------------------------------------------------------------------------

    /** PRECONDITION: key CANNOT BE NULL **/
    public final boolean containsKey(final K key) {
        return get(key) != null;
    }

    /** PRECONDITION: key CANNOT BE NULL **/
    public final V get(final K key) {
        if (key == null) throw new NullPointerException();
        Node<K,V> l = root.left;
        while (l.getClass() == InternalNode.class) {
            l = (l.key == null || key.compareTo(l.key) < 0) ? ((InternalNode<K,V>)l).left : ((InternalNode<K,V>)l).right;
        }
        return (l.key != null && key.compareTo(l.key) == 0) ? ((LeafNode<K,V>)l).value : null;
    }

    // Insert key to dictionary, returns the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: key, value CANNOT BE NULL **/
    public final V putIfAbsent(final K key, final V value){
        if (key == null || value == null) throw new NullPointerException();
        InternalNode<K,V> newInternal;
        LeafNode<K,V> newSibling, newNode;

        /** SEARCH VARIABLES **/
        InternalNode<K,V> p;
        Info<K,V> pinfo;
        Node<K,V> l;
        /** END SEARCH VARIABLES **/

        newNode = new LeafNode<K,V>(key, value);

        while (true) {

            /** SEARCH **/
            p = root;
            l = p.left;
            while (l.getClass() == InternalNode.class) {
                p = (InternalNode<K,V>)l;
                l = (p.key == null || key.compareTo(p.key) < 0) ? p.left : p.right;
            }
            pinfo = p.info;                             // read pinfo once instead of every iteration
            if (l != p.left && l != p.right) continue;  // then confirm the child link to l is valid
                                                        // (just as if we'd read p's info field before the reference to l)
            /** END SEARCH **/

            LeafNode<K,V> foundLeaf = (LeafNode<K,V>)l;

            if (key.equals(foundLeaf.key)) {
                return foundLeaf.value; // key already in the tree, no duplicate allowed
            } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                help(pinfo);
            } else {
                newSibling = new LeafNode<K,V>(foundLeaf.key, foundLeaf.value);
                if (foundLeaf.key == null || key.compareTo(foundLeaf.key) < 0)  // newinternal = max(ret.foundLeaf.key, key);
                    newInternal = new InternalNode<K,V>(foundLeaf.key, newNode, newSibling);
                else
                    newInternal = new InternalNode<K,V>(key, newSibling, newNode);

                final IInfo<K,V> newPInfo = new IInfo<K,V>(foundLeaf, p, newInternal);

                // try to IFlag parent
                if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
                    helpInsert(newPInfo);
                    return null;
                } else {
                    // if fails, help the current operation
                    // [CHECK]
                    // need to get the latest p.info since CAS doesnt return current value
                    help(p.info);
                }
            }
        }
    }

    // Insert key to dictionary, return the previous value associated with the specified key,
    // or null if there was no mapping for the key
    /** PRECONDITION: key, value CANNOT BE NULL **/
    public final V put(final K key, final V value) {
        if (key == null || value == null) throw new NullPointerException();
        InternalNode<K, V> newInternal;
        LeafNode<K, V> newSibling, newNode;
        IInfo<K, V> newPInfo;
        V result;

        /** SEARCH VARIABLES **/
        InternalNode<K, V> p;
        Info<K, V> pinfo;
        Node<K, V> l;
        /** END SEARCH VARIABLES **/
        newNode = new LeafNode<K, V>(key, value);

        while (true) {

            /** SEARCH **/
            p = root;
            l = p.left;
            while (l.getClass() == InternalNode.class) {
                p = (InternalNode<K,V>)l;
                l = (p.key == null || key.compareTo(p.key) < 0) ? p.left : p.right;
            }
            pinfo = p.info;                             // read pinfo once instead of every iteration
            if (l != p.left && l != p.right) continue;  // then confirm the child link to l is valid
                                                        // (just as if we'd read p's info field before the reference to l)
            /** END SEARCH **/

            if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                help(pinfo);
            } else {
                LeafNode<K,V> foundLeaf = (LeafNode<K,V>)l;

                if (key.equals(foundLeaf.key)) {
                    // key already in the tree, try to replace the old node with new node
                    newPInfo = new IInfo<K, V>(foundLeaf, p, newNode);
                    result = foundLeaf.value;
                } else {
                    // key is not in the tree, try to replace a leaf with a small subtree
                    newSibling = new LeafNode<K, V>(foundLeaf.key, foundLeaf.value);
                    if (foundLeaf.key == null || key.compareTo(foundLeaf.key) < 0) // newinternal = max(ret.foundLeaf.key, key);
                    {
                        newInternal = new InternalNode<K, V>(foundLeaf.key, newNode, newSibling);
                    } else {
                        newInternal = new InternalNode<K, V>(key, newSibling, newNode);
                    }

                    newPInfo = new IInfo<K, V>(foundLeaf, p, newInternal);
                    result = null;
                }

                // try to IFlag parent
                if (infoUpdater.compareAndSet(p, pinfo, newPInfo)) {
                    helpInsert(newPInfo);
                    return result;
                } else {
                    // if fails, help the current operation
                    // need to get the latest p.info since CAS doesnt return current value
                    help(p.info);
                }
            }
        }
    }

    // Delete key from dictionary, return the associated value when successful, null otherwise
    /** PRECONDITION: key CANNOT BE NULL **/
    public final V remove(final K key){
        if (key == null) throw new NullPointerException();

        /** SEARCH VARIABLES **/
        InternalNode<K,V> gp;
        Info<K,V> gpinfo;
        InternalNode<K,V> p;
        Info<K,V> pinfo;
        Node<K,V> l;
        /** END SEARCH VARIABLES **/
        
        while (true) {

            /** SEARCH **/
            gp = null;
            gpinfo = null;
            p = root;
            pinfo = p.info;
            l = p.left;
            while (l.getClass() == InternalNode.class) {
                gp = p;
                p = (InternalNode<K,V>)l;
                l = (p.key == null || key.compareTo(p.key) < 0) ? p.left : p.right;
            }
            // note: gp can be null here, because clearly the root.left.left == null
            //       when the tree is empty. however, in this case, l.key will be null,
            //       and the function will return null, so this does not pose a problem.
            if (gp != null) {
                gpinfo = gp.info;                               // - read gpinfo once instead of every iteration
                if (p != gp.left && p != gp.right) continue;    //   then confirm the child link to p is valid
                pinfo = p.info;                                 //   (just as if we'd read gp's info field before the reference to p)
                if (l != p.left && l != p.right) continue;      // - do the same for pinfo and l
            }
            /** END SEARCH **/
            
            if (!key.equals(l.key)) return null;
            if (!(gpinfo == null || gpinfo.getClass() == Clean.class)) {
                help(gpinfo);
            } else if (!(pinfo == null || pinfo.getClass() == Clean.class)) {
                help(pinfo);
            } else {
                LeafNode<K,V> foundLeaf = (LeafNode<K,V>)l;
                // try to DFlag grandparent
                final DInfo<K,V> newGPInfo = new DInfo<K,V>(foundLeaf, p, gp, pinfo);

                if (infoUpdater.compareAndSet(gp, gpinfo, newGPInfo)) {
                    if (helpDelete(newGPInfo)) return foundLeaf.value;
                } else {
                    // if fails, help grandparent with its latest info value
                    help(gp.info);
                }
            }
        }
    }

//--------------------------------------------------------------------------------
// PRIVATE METHODS
// - helpInsert
// - helpDelete
//--------------------------------------------------------------------------------

    private void helpInsert(final IInfo<K,V> info){
        (info.p.left == info.l ? leftUpdater : rightUpdater).compareAndSet(info.p, info.l, info.lReplacingNode);
        infoUpdater.compareAndSet(info.p, info, new Clean());
    }

    private boolean helpDelete(final DInfo<K,V> info){
        final boolean result;

        result = infoUpdater.compareAndSet(info.p, info.pinfo, new Mark<K,V>(info));
        final Info<K,V> currentPInfo = info.p.info;
        if (result || (currentPInfo.getClass() == Mark.class && ((Mark<K,V>) currentPInfo).dinfo == info)) {
            // CAS succeeded or somebody else already helped
            helpMarked(info);
            return true;
        } else {
            help(currentPInfo);
            infoUpdater.compareAndSet(info.gp, info, new Clean());
            return false;
        }
    }

    private void help(final Info<K,V> info) {
        if (info.getClass() == IInfo.class)     helpInsert((IInfo<K,V>) info);
        else if(info.getClass() == DInfo.class) helpDelete((DInfo<K,V>) info);
        else if(info.getClass() == Mark.class)  helpMarked(((Mark<K,V>)info).dinfo);
    }

    private void helpMarked(final DInfo<K,V> info) {
        final Node<K,V> other = (info.p.right == info.l) ? info.p.left : info.p.right;
        (info.gp.left == info.p ? leftUpdater : rightUpdater).compareAndSet(info.gp, info.p, other);
        infoUpdater.compareAndSet(info.gp, info, new Clean());
    }

    /**
     *
     * DEBUG CODE (FOR TESTBED)
     *
     */

    public long getSumOfKeys() {
        return getSumOfKeys(root);
    }

    private long getSumOfKeys(Node node) {
        long sum = 0;
        if (node.getClass() == LeafNode.class)
            sum += node.key != null ? (int) (Integer) node.key : 0;
        else
            sum += getSumOfKeys(((InternalNode<K,V>)node).left) + getSumOfKeys(((InternalNode<K,V>)node).right);
        return sum;
    }
}