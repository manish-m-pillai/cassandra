/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction.writers;

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.ILifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public class MaxSSTableSizeWriter extends CompactionAwareWriter
{
    private final long maxSSTableSize;
    private final int level;
    private final long estimatedSSTables;

    public MaxSSTableSizeWriter(ColumnFamilyStore cfs,
                                Directories directories,
                                ILifecycleTransaction txn,
                                Set<SSTableReader> nonExpiredSSTables,
                                long maxSSTableSize,
                                int level)
    {
        this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, level, false);
    }

    public MaxSSTableSizeWriter(ColumnFamilyStore cfs,
                                Directories directories,
                                ILifecycleTransaction txn,
                                Set<SSTableReader> nonExpiredSSTables,
                                long maxSSTableSize,
                                int level,
                                boolean keepOriginals)
    {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
        this.level = level;
        this.maxSSTableSize = maxSSTableSize;

        long totalSize = getTotalWriteSize(nonExpiredSSTables, estimatedTotalKeys, cfs, txn.opType());
        estimatedSSTables = Math.max(1, totalSize / maxSSTableSize);
    }

    /**
     * Gets the estimated total amount of data to write during compaction
     */
    private static long getTotalWriteSize(Iterable<SSTableReader> nonExpiredSSTables, long estimatedTotalKeys, ColumnFamilyStore cfs, OperationType compactionType)
    {
        long estimatedKeysBeforeCompaction = 0;
        for (SSTableReader sstable : nonExpiredSSTables)
            estimatedKeysBeforeCompaction += sstable.estimatedKeys();
        estimatedKeysBeforeCompaction = Math.max(1, estimatedKeysBeforeCompaction);
        double estimatedCompactionRatio = (double) estimatedTotalKeys / estimatedKeysBeforeCompaction;

        return Math.round(estimatedCompactionRatio * cfs.getExpectedCompactedFileSize(nonExpiredSSTables, compactionType));
    }

    @Override
    protected boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key)
    {
        return sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten() > maxSSTableSize;
    }

    protected int sstableLevel()
    {
        return level;
    }

    protected long sstableKeyCount()
    {
        return estimatedTotalKeys / estimatedSSTables;
    }

    @Override
    protected long getExpectedWriteSize()
    {
        return Math.min(maxSSTableSize, super.getExpectedWriteSize());
    }
}
