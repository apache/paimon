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

package org.apache.paimon.gateway.handler;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.DefaultFullHttpResponse;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpVersion;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.LastHttpContent;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.gateway.reader.ExcelWriteStrategy;
import org.apache.paimon.gateway.reader.WriteStrategy;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;

import org.apache.paimon.shade.guava30.com.google.common.base.Splitter;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.shaded.netty4.io.netty.channel.Channel;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelConfig;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpContent;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.timeout.IdleStateEvent;
import org.apache.flink.shaded.netty4.io.netty.util.AttributeKey;
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.gateway.utils.HttpServerResponseUtil.response;

/** An HTTP handler for loading data into a catalog, handling various HTTP events and requests. */
@ChannelHandler.Sharable
public class LoadHttpHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final Logger LOG = LoggerFactory.getLogger(LoadHttpHandler.class);
    private static final AttributeKey<HttpRequest> REQUEST_KEY =
            AttributeKey.valueOf("HttpRequest");
    private final Catalog catalog;

    public LoadHttpHandler(Catalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        LOG.info(ctx.channel().remoteAddress() + "请求连接上来了");
        super.handlerAdded(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        ctx.channel().close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error("exceptionCaught : {}", cause.getMessage(), cause);
        ctx.channel().close();
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) {
        Channel ch = ctx.channel();
        ChannelConfig config = ch.config();

        if (!ch.isWritable()) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(
                        "{} is not writable, over high water level : {}",
                        ch,
                        config.getWriteBufferHighWaterMark());
            }

            config.setAutoRead(false);
        } else {
            if (LOG.isWarnEnabled()) {
                LOG.warn(
                        "{} is writable, to low water : {}",
                        ch,
                        config.getWriteBufferLowWaterMark());
            }
            config.setAutoRead(true);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            ctx.channel().close();
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
        if (msg instanceof HttpRequest) {
            HttpRequest request = (HttpRequest) msg;
            ctx.channel().attr(REQUEST_KEY).set(request);
        } else if (msg instanceof HttpContent) {
            HttpRequest request = ctx.channel().attr(REQUEST_KEY).get();
            String columnSeparator = request.headers().get("column_separator");
            Optional<String[]> names = extractDbAndTable(request.uri());
            HttpContent content = (HttpContent) msg;
            ByteBuf buf = content.content();
            String receivedContent = buf.toString(CharsetUtil.UTF_8);

            String dbName = names.get()[0];
            String tableName = names.get()[1];
            Identifier tableIdentifier = Identifier.create(dbName, tableName);

            WriteStrategy writeStrategy = new ExcelWriteStrategy();
            // Check if table exists before trying to get or create it
            Schema schema = writeStrategy.retrieveSchema();
            if (!catalog.tableExists(tableIdentifier)) {
                if (!catalog.databaseExists(dbName)) {
                    catalog.createDatabase(dbName, false);
                }
                if (!catalog.tableExists(tableIdentifier)) {
                    catalog.createTable(tableIdentifier, schema, false);
                }
            }
            Table table = catalog.getTable(tableIdentifier);
            // 1. Create a WriteBuilder (Serializable)
            BatchWriteBuilder writeBuilder = table.newBatchWriteBuilder().withOverwrite();

            // 2. Write records in distributed tasks
            BatchTableWrite write = writeBuilder.newWrite();
            writeStrategy.writer(write, receivedContent, columnSeparator);

            List<CommitMessage> messages = write.prepareCommit();

            // 3. Collect all CommitMessages to a global node and commit
            BatchTableCommit commit = writeBuilder.newCommit();
            commit.commit(messages);

            response(
                    ctx,
                    "{\n"
                            + "    \"Status\": \"Success\",\n"
                            + "    \"NumberTotalRows\": 1000000,\n"
                            + "    \"NumberLoadedRows\": 1000000,\n"
                            + "    \"NumberFilteredRows\": 1,\n"
                            + "    \"NumberUnselectedRows\": 0,\n"
                            + "    \"LoadBytes\": 40888898,\n"
                            + "    \"LoadTimeMs\": 2144,\n"
                            + "    \"BeginTxnTimeMs\": 1,\n"
                            + "    \"StreamLoadPutTimeMs\": 2,\n"
                            + "    \"ReadDataTimeMs\": 325,\n"
                            + "    \"WriteDataTimeMs\": 1933,\n"
                            + "    \"CommitAndPublishTimeMs\": 106\n"
                            + "}");
        }
        if (msg instanceof LastHttpContent) {
            // 所有的数据块都已经接收完毕
            ctx.writeAndFlush(new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
            // 这里可以清理资源，比如关闭文件输出流等
        }
    }

    private static Optional<String[]> extractDbAndTable(String path) {
        List<String> parts = Splitter.on('/').omitEmptyStrings().splitToList(path);

        if (parts.size() >= 4) {
            return Optional.of(new String[] {parts.get(1), parts.get(2)});
        } else {
            return Optional.empty();
        }
    }
}
