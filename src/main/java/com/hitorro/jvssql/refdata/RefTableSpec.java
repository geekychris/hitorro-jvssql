/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.refdata;

import com.hitorro.jvssql.config.RefreshPolicy;
import com.hitorro.util.basefile.fs.BaseFile;

/**
 * Bookkeeping for a registered reference table: the source file, its refresh
 * policy, and the table name (for logging). Kept alongside {@code JvsTable}
 * so {@code JvsSqlEngine.refreshReferenceTable(name)} and the scheduled
 * refresher have everything they need.
 */
public final class RefTableSpec {

    private final String name;
    private final BaseFile source;
    private final RefreshPolicy policy;

    public RefTableSpec(String name, BaseFile source, RefreshPolicy policy) {
        this.name = name;
        this.source = source;
        this.policy = policy;
    }

    public String name() { return name; }
    public BaseFile source() { return source; }
    public RefreshPolicy policy() { return policy; }
}
