/*
 * Copyright (C) 2013  The Async HBase Authors.  All rights reserved.
 * This file is part of Async HBase.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *   - Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   - Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *   - Neither the name of the StumbleUpon nor the names of its contributors
 *     may be used to endorse or promote products derived from this software
 *     without specific prior written permission.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.hbase.async;

import org.jboss.netty.buffer.ChannelBuffer;

import org.hbase.async.generated.FilterPB;

/**
 * Filters based on a limit and offset of number of columns. I.e. Pagination.
 * @since 1.5
 */
public final class ColumnPaginationByteOffsetFilter extends ScanFilter {

    private static final byte[] NAME = Bytes.ISO88591("org.apache.hadoop.hbase"
            + ".filter.ColumnPaginationByteOffsetFilter");

    private int limit = 0;
    private int offset = -1;
    private byte[] columnOffset = null;

    /**
     * Constructor to define filter of numerical limit and offset.
     * @param limit The maximum number of columns to return.
     * @param offset The integer offset where to start pagination.
     */
    public ColumnPaginationByteOffsetFilter(final int limit, final int offset) {
        this.limit = limit;
        this.offset = offset;
    }

    /**
     * Constructor to define filter of numerical limit and column qualifier bytes offset.
     * @param limit The maximum number of columns to return.
     * @param columnOffset The bytes offset where to start pagination.
     */
    public ColumnPaginationByteOffsetFilter(final int limit, final byte[] columnOffset) {
        this.limit = limit;
        this.columnOffset = columnOffset;
    }

    @Override
    byte[] serialize() {
        final FilterPB.ColumnPaginationFilter.Builder filter =
                FilterPB.ColumnPaginationFilter.newBuilder();

        filter.setLimit(limit);
        if (offset > -1)
            filter.setOffset(offset);

        if (columnOffset != null)
            filter.setColumnOffset(Bytes.wrap(columnOffset));

        return filter.build().toByteArray();
    }

    @Override
    byte[] name() {
        return NAME;
    }

    @Override
    int predictSerializedSize() {
        return 1 + NAME.length + 4 + 4
                + 3 + (columnOffset == null ? 0 : columnOffset.length);
    }

    @Override
    void serializeOld(final ChannelBuffer buf) {
        buf.writeByte((byte) NAME.length); // 1
        buf.writeBytes(NAME); // NAME.length

        // Limit
        buf.writeInt(this.limit); // 4

        // Integer Offset
        buf.writeInt(this.offset); // 4

        // Column Offset
        if (this.columnOffset != null)
            HBaseRpc.writeByteArray(buf, this.columnOffset); // 3 + columnOffset.length
    }

    public String toString() {
        return "ColumnPaginationFilter(limit=" + limit
                + ", offset=" + offset
                + ", columnOffset=" + Bytes.pretty(this.columnOffset) + ")";
    }

}