package com.sopovs.moradanen.tarantool;

import com.sopovs.moradanen.tarantool.core.TarantoolException;
import com.sopovs.moradanen.tarantool.core.Util;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.msgpack.core.MessageFormat.FIXMAP;

public class TarantoolAsyncClient implements Closeable {

    private final AtomicInteger counter = new AtomicInteger();
    private final AsynchronousSocketChannel channel;

    public TarantoolAsyncClient(String host) {
        this(host, 3301);
    }


    public TarantoolAsyncClient(String host, int port) {
        try {
            //TODO all this async too
            channel = AsynchronousSocketChannel.open();
            channel.connect(new InetSocketAddress(host, port)).get();
            Integer read = channel.read(ByteBuffer.allocate(128)).get();
            if (read != 128) {
                throw new TarantoolException("foo");
            }
        } catch (Exception e) {
            throw new TarantoolException(e);
        }
    }


    public Future<Void> ping() {
        try {
            CompletableFuture<Void> future = new CompletableFuture<>();
            MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
            packer.packMapHeader(2);

            packer.packInt(Util.KEY_CODE);
            packer.packInt(Util.CODE_PING);
            packer.packInt(Util.KEY_SYNC);
            packer.packInt(counter.incrementAndGet());
            packer.flush();
            byte[] bytes = packer.toByteArray();

            ByteBuffer sizeBuffer = ByteBuffer.allocate(5);
            sizeBuffer.put(MessagePack.Code.UINT32);
            sizeBuffer.putInt(bytes.length);
            if (0 != channel.write(sizeBuffer).get()) {
                throw new TarantoolException("Cannot write size");
            }

            channel.write(ByteBuffer.wrap(bytes), null, new CompletionHandler<Integer, Void>() {
                @Override
                public void completed(Integer result, Void attachment) {
                    ByteBuffer sizeBuffer = ByteBuffer.allocate(5);
                    channel.read(sizeBuffer, null, new CompletionHandler<Integer, Void>() {
                        @Override
                        public void completed(Integer result, Void attachment) {
                            if(result != 5){
                                channel.read(sizeBuffer, null, this);
                            }
                            try {
                                System.out.println(result);
                                sizeBuffer.flip();
                                sizeBuffer.get();
                                int size =  sizeBuffer.getInt();
                                ByteBuffer buffer = ByteBuffer.allocate(size);

                                channel.read(buffer, null, new CompletionHandler<Integer, Void>() {
                                    @Override
                                    public void completed(Integer result, Void attachment) {
                                        try {
                                            buffer.flip();
                                            MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(buffer);
//                                            if (unpacker.getNextFormat() == FIXMAP) {

                                                int headerSize = unpacker.unpackMapHeader();
                                                for (int i = 0; i < headerSize; i++) {
                                                    unpacker.unpackByte();
                                                    unpacker.unpackInt();
                                                }
                                                int bodySize = unpacker.unpackMapHeader();
//                                            } else {
//                                                while (unpacker.hasNext()) {
//                                                    System.out.println(unpacker.getNextFormat());
//                                                    unpacker.skipValue();
//                                                }
//                                            }
                                            future.complete(null);
                                        } catch (Throwable e) {
                                            future.completeExceptionally(e);
                                        }

                                    }

                                    @Override
                                    public void failed(Throwable e, Void attachment) {
                                        future.completeExceptionally(e);
                                    }
                                });


                            } catch (Exception e) {
                                future.completeExceptionally(e);
                            }
                        }

                        @Override
                        public void failed(Throwable e, Void attachment) {
                            future.completeExceptionally(e);
                        }
                    });
                }

                @Override
                public void failed(Throwable exc, Void attachment) {
                    exc.printStackTrace();
                }
            });
            return future;
        } catch (Exception e) {
            throw new TarantoolException(e);
        }


    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new TarantoolException(e);
        }
    }


}
