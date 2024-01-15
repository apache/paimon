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

package org.apache.paimon.server;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableCommit;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;

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
import org.apache.flink.shaded.netty4.io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** sds. */
@ChannelHandler.Sharable
public class LoadHttpHandler extends SimpleChannelInboundHandler<HttpObject> {

    private static final Logger LOG = LoggerFactory.getLogger(LoadHttpHandler.class);

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
        LOG.info(ctx.channel().remoteAddress()+"请求连接上来了");
        super.handlerAdded(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
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
            // 处理HttpRequest的开始部分（例如检查URI和方法）
            LOG.info("Received HttpRequest: {}", request.uri());
        } else if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;
            ByteBuf buf = content.content();
            String receivedContent = buf.toString(CharsetUtil.UTF_8);
            String[] split = receivedContent.split(",");
            Options options = new Options();
            options.set(
                    "warehouse", "file:///Users/zhuoyuchen/Documents/GitHub/incubator-paimon/data");
            CatalogContext context = CatalogContext.create(options);
            Catalog catalog = CatalogFactory.createCatalog(context);
            Identifier tableIdentifier = Identifier.create("my_db", "my_table");
            if (!catalog.tableExists(tableIdentifier)) {
                Schema schema =
                        Schema.newBuilder()
                                .column("address", DataTypes.STRING())
                                .column("data", DataTypes.STRING())
                                .build();
                catalog.createDatabase("my_db", false);
                catalog.createTable(tableIdentifier, schema, false);
            }

            // 1. Create a WriteBuilder (Serializable)
            Table table = GetTable.getTable();
            StreamWriteBuilder writeBuilder = table.newStreamWriteBuilder();

            // 2. Write records in distributed tasks
            StreamTableWrite write = writeBuilder.newWrite();
            // commitIdentifier like Flink checkpointId
            long commitIdentifier = 0;

            while (true) {
                GenericRow record1 =
                        GenericRow.of(
                                BinaryString.fromString("address"),
                                BinaryString.fromString(split[0]));
                GenericRow record2 =
                        GenericRow.of(
                                BinaryString.fromString("data"), BinaryString.fromString(split[1]));
                write.write(record1);
                write.write(record2);
                List<CommitMessage> messages = write.prepareCommit(false, commitIdentifier);
                commitIdentifier++;

                // 3. Collect all CommitMessages to a global node and commit
                StreamTableCommit commit = writeBuilder.newCommit();
                commit.commit(commitIdentifier, messages);

                // 4. When failure occurs and you're not sure if the commit process is successful,
                //    you can use `filterAndCommit` to retry the commit process.
                //    Succeeded commits will be automatically skipped.
                /*
                Map<Long, List<CommitMessage>> commitIdentifiersAndMessages = new HashMap<>();
                commitIdentifiersAndMessages.put(commitIdentifier, messages);
                commit.filterAndCommit(commitIdentifiersAndMessages);
                */

                Thread.sleep(1000);
            }

            //            // 打印接收到的数据
            //            LOG.info("Received content chunk: {}", receivedContent);
            //
            //            // 这里可以处理数据，例如写入文件或数据库
            //
            //            if (content instanceof LastHttpContent) {
            //                // 处理请求的结束部分
            //                // 发送响应
            //                //sendJsonContent(ctx, HttpResponseStatus.OK, "Data processed");
            //                LOG.info("Finished processing request.");
            //            }
        }
    }
}
