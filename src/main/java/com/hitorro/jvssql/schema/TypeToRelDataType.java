/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.schema;

import com.hitorro.jsontypesystem.Field;
import com.hitorro.jsontypesystem.Type;
import com.hitorro.util.typesystem.TypeFieldDataType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Adapts a JVS {@link Type} into a Calcite {@link RelDataType} row type. Each
 * top-level {@link Field} on the JVS type becomes a column on the row.
 *
 * <p>Nested / MLS / dynamic-only fields are represented as {@code ANY} at
 * schema level — the strict-with-escape validator handles dotted-path
 * resolution and {@code DYNAMIC('path')} lookups. Refinements to sub-column
 * typing (e.g. {@code content.mls[0].text} as VARCHAR) are handled at row
 * projection time via a custom SqlOperator, not baked into the schema.</p>
 */
public final class TypeToRelDataType {

    private TypeToRelDataType() {}

    /** Row type for a whole JVS Type: one column per top-level field. */
    public static RelDataType asRowType(Type type, RelDataTypeFactory factory) {
        RelDataTypeFactory.Builder b = factory.builder();
        // Type#getFields is package-private; iterate the way the projection code does.
        Map<String, Field> fields = fieldsOf(type);
        for (Map.Entry<String, Field> e : fields.entrySet()) {
            b.add(e.getKey(), fieldType(e.getValue(), factory));
        }
        return b.build();
    }

    /** Type of a single top-level column (nullable by default — JVS makes no NOT NULL guarantees). */
    public static RelDataType fieldType(Field f, RelDataTypeFactory factory) {
        Type fieldType = f.getType();
        SqlTypeName sql = mapPrimitive(fieldType == null ? null : fieldType.getPrimitiveType());
        RelDataType rdt = factory.createSqlType(sql);
        return factory.createTypeWithNullability(rdt, true);
    }

    /**
     * JVS primitive-type → Calcite SqlTypeName. Non-primitives (object / MLS /
     * arrays / user-defined) collapse to {@code ANY} at schema level; per-cell
     * type resolution happens at expression time.
     */
    public static SqlTypeName mapPrimitive(TypeFieldDataType t) {
        if (t == null) return SqlTypeName.ANY;
        switch (t) {
            case Long:          return SqlTypeName.BIGINT;
            case Int:           return SqlTypeName.INTEGER;
            case Short:         return SqlTypeName.SMALLINT;
            case Byte:          return SqlTypeName.TINYINT;
            case Double:        return SqlTypeName.DOUBLE;
            case Float:         return SqlTypeName.FLOAT;
            case String:        return SqlTypeName.VARCHAR;
            case Date:          return SqlTypeName.TIMESTAMP;
            case Boolean:       return SqlTypeName.BOOLEAN;
            case HTSerializable:
            default:            return SqlTypeName.ANY;
        }
    }

    /**
     * The public {@link Type#visit} walk. We collect top-level fields (path.size()==1)
     * and return {@code false} from {@code enterField} to skip descent into sub-fields.
     * A follow-up can add a {@code fieldNames()} accessor to Type to skip the visitor.
     */
    private static Map<String, Field> fieldsOf(Type type) {
        Map<String, Field> out = new LinkedHashMap<>();
        type.visit(new com.hitorro.jsontypesystem.TypeVisitor<>() {
            @Override public void enterType(Type t, com.hitorro.util.json.keys.propaccess.Propaccess path) {}
            @Override public void leaveType(Type t, com.hitorro.util.json.keys.propaccess.Propaccess path) {}
            @Override public boolean enterField(Field f, com.hitorro.util.json.keys.propaccess.Propaccess path) {
                if (path.length() == 1) out.put(f.getName(), f);
                return false;
            }
            @Override public void leaveField(Field f, com.hitorro.util.json.keys.propaccess.Propaccess path) {}
            @Override public void enterGroup(Field f, com.hitorro.jsontypesystem.Group g, com.hitorro.util.json.keys.propaccess.Propaccess path) {}
            @Override public void leaveGroup(Field f, com.hitorro.jsontypesystem.Group g, com.hitorro.util.json.keys.propaccess.Propaccess path) {}
        }, ALWAYS_TRUE, new com.hitorro.util.json.keys.propaccess.Propaccess(""));
        return out;
    }

    private static final java.util.function.Predicate<com.hitorro.jsontypesystem.BaseT> ALWAYS_TRUE = x -> true;
}
