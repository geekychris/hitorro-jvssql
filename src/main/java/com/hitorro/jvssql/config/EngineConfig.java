/*
 * Copyright (c) 2006-2025 Chris Collins
 */
package com.hitorro.jvssql.config;

import com.hitorro.util.basefile.fs.BaseFile;

/**
 * Engine-wide runtime configuration. Spill directory, memory budget, and other
 * defaults that operators consult (ExternalMergeSort in particular).
 */
public final class EngineConfig {

    private final BaseFile spillDir;
    private final int memoryBudgetMB;
    private final boolean strictTypes;

    private EngineConfig(Builder b) {
        this.spillDir = b.spillDir;
        this.memoryBudgetMB = b.memoryBudgetMB;
        this.strictTypes = b.strictTypes;
    }

    /** Directory used for external-sort spill runs and any other on-disk scratch. */
    public BaseFile spillDir() { return spillDir; }

    /** Soft memory cap for in-memory buffers (per operator). */
    public int memoryBudgetMB() { return memoryBudgetMB; }

    /** If true (default), unknown bare identifiers in SQL are validation errors
     * unless wrapped in {@code DYNAMIC('path')}. If false, unknown identifiers
     * are silently treated as dynamic lookups (permissive mode). */
    public boolean strictTypes() { return strictTypes; }

    public static Builder builder() { return new Builder(); }

    public static EngineConfig defaults() { return builder().build(); }

    public static final class Builder {
        private BaseFile spillDir;
        private int memoryBudgetMB = 512;
        private boolean strictTypes = true;

        public Builder spillDir(BaseFile dir) { this.spillDir = dir; return this; }
        public Builder memoryBudgetMB(int mb) { this.memoryBudgetMB = mb; return this; }
        public Builder strictTypes(boolean strict) { this.strictTypes = strict; return this; }
        public EngineConfig build() { return new EngineConfig(this); }
    }
}
