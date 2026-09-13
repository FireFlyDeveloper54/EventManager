package dev.hotaru.event;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * Captures and preserves generic type information at runtime using super type tokens.
 *
 * <p>Example usage:
 * <pre>{@code
 * TypeToken<List<String>> token = new TypeToken<List<String>>() {};
 * Type type = token.getType();
 * }</pre>
 *
 * @param <T> the represented type
 */
public abstract class TypeToken<T> {

    private final Type type;

    protected TypeToken() {
        Type superClass = getClass().getGenericSuperclass();
        if (!(superClass instanceof ParameterizedType)) {
            throw new IllegalArgumentException("TypeToken must be constructed with a generic type parameter");
        }
        this.type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    private TypeToken(Type type) {
        this.type = Objects.requireNonNull(type, "type");
    }

    /**
     * Returns the represented generic type.
     */
    public Type getType() {
        return type;
    }

    /**
     * Creates a TypeToken wrapping the specified existing Type.
     */
    public static TypeToken<?> of(Type type) {
        return new TypeToken<Object>(type) {};
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TypeToken<?>)) return false;
        TypeToken<?> that = (TypeToken<?>) o;
        return Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(type);
    }

    @Override
    public String toString() {
        return "TypeToken[" + type + "]";
    }
}
