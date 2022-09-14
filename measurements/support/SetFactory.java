
package measurements.support;

public abstract class SetFactory<K> {
    public abstract SetInterface<K> newSet(final Integer param);
    public abstract String getName();
}