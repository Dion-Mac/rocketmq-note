/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.CommitLog.PutMessageContext;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * TODO 内存映射相关，非常重要
 * MappedFile是RocketMQ内存映射文件的具体实现
 *
 * 在MappedFile设计中，只有提交了的数据(写入到 MappedByteBuffer 或 FileChannel 中的数据)才是安全的数据。
 * 为什么没刷盘之前也认为是安全数据呢，这就和 MappedByteBuffer 和 FileChannel 的写入机制有关了，
 * 无论是 MappedByteBuffer 还是 FileChannel 在写入数据时，实际上只是将数据写入 PageCache，
 * 而操作系统会自动的将脏页刷盘，这层 PageCache 就是我们应用和物理存储之间的夹层，
 * 当我们将数据写入 PageCache 后，即便我们的应用崩溃了，但是只要系统不崩溃，最终也会将数据刷入磁盘。
 * 所以，RocketMQ 以写入 PageCache 作为数据安全可读的判断标准。
 */
public class MappedFile extends ReferenceResource {

    /**
     * 操作系统每页大小 默认4k 页缓存
     */
    public static final int OS_PAGE_SIZE = 1024 * 4;

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 当前JVM实例中 MappedFile用到的虚拟内存大小
     */
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    /**
     * 当前JVM实例中 MappedFile的个数
     */
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);

    /**
     * 当前文件写指针，从0开始
     */
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);

    /**
     * 当前文件提交指针
     * 如果开启transientStorePoolEnable,则数据会存储在TransientStorePool中
     * 然后提交到内存映射ByteBuffer中，再刷写到磁盘
     */
    protected final AtomicInteger committedPosition = new AtomicInteger(0);

    /**
     * 当前文件刷盘指针，该指针之前的数据持久化到磁盘中
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);

    /**
     * 文件大小
     */
    protected int fileSize;

    /**
     * 文件通道
     */
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     */
    protected ByteBuffer writeBuffer = null;

    /**
     * 堆外内存池 该内存池内的内存会提供内存锁定机制，transientStorePoolEnable为true时启用
     */
    protected TransientStorePool transientStorePool = null;

    /**
     * 文件名称
     */
    private String fileName;

    /**
     * 该文件的初始偏移量
     */
    private long fileFromOffset;

    /**
     * 物理文件
     */
    private File file;

    /**
     * 物理文件对应的内存映射buffer
     */
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 文件最后一次内容写入的时间
     */
    private volatile long storeTimestamp = 0;

    /**
     * 是不是MappedFileQueue队列中的第一个文件
     */
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    /**
     * Java官方没有提供公共的方法来进行 MappedByteBuffer 的回收，所以不得不通过反射来进行回收，
     * 这也是 MappedByteBuffer 比较坑的一点
     * @param buffer
     */
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";
        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        /**
         * 初始化fileFromOffset, 文件名代表该文件的起始偏移量
         */
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        ensureDirOK(this.file.getParent());

        try {
            /**
             * 笔记
             * 下面两句代码的意思是
             * 通过RandomAccessFile创建读写文件通道   fileChannel
             * 并将文件内容使用NIO的内存映射机制buffer将文件映射到内存中  mappedByteBuffer 使用的就是操作系统的页缓存 TODO 如果broker奔溃了，系统正常，页缓存的数据会丢失吗？网上有的说会有的说不会，研究确认一下  已經确认过，不会
             * 这样做代表了什么呢？
             */
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            // 为什么 mappedByteBuffer 是一个 DirectByteBuffer 对象？ 又是一个技术点了？
            /**
             * 由于FileChannelImpl和DirectByteBuffer不在同一个包中，所以有权限访问问题，
             * 所以通过AccessController类的doPrivileged方法，获取DirectByteBuffer的构造器，然后再进行实例化
             */
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);

            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
            PutMessageContext putMessageContext) {
        assert messageExt != null;
        assert cb != null;
        //获取文件的写入指针位置,即获取mappedFile当前写指针
        int currentPos = this.wrotePosition.get();
        /**
         * 如果指针位置大于文件大小则直接返回
         * 如果currentPos大于或等于文件大小fileSize,则表明文件已经写满
         */
        if (currentPos < this.fileSize) {
            /**
             * 如果writeBuffer不为空，说明使用了TransientStorePool, 则使用writeBuffer作为写入时使用的buffer
             * 通过writeBuffer.slice()创建一个与MappedFile共享的内存区，并设置position为当前指针
             */
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBrokerInner) { // 单条写入，通过回调方法写入
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBatch) { // 批量写入
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());// 记录当前文件写指针
            this.storeTimestamp = result.getStoreTimestamp();// 记录文件最后一次写入时间
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     *
     * flush和commit一样也可以传入一个刷盘页数，当脏页数量达到要求时，会进行刷盘操作
     *
     * 分析一下wrotePosition，committedPosition，flushedPosition的关系，
     * 当有新的数据要写入时，先会写入内存，然后writePosition代表的就是内存写入的末尾，
     * commit过程只有transientStorePoolEnable为true时才有意义，代表的是从writeBuffer拷贝到FileChannel时，拷贝数据的末尾，
     * 而flushedPosition则代表将内存数据刷盘到物理磁盘的末尾。
     *
     * 综上所述，可以得到一个关于这三个position之间的关系：
     *
     * transientStorePoolEnable: flushedPosition<=committedPosition<=wrotePosition
     * MappedByteBuffer only: flushedPosition<=wrotePosition
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    /**
                     * 如果使用writeBuffer存储的话则调用fileChannel的force将内存中的数据持久化到磁盘，
                     * 刷盘结束后，flushedPosition会等于committedPosition，
                     * 否则调用mappedByteBuffer的force，最后flushedPosition会等于writePosition。
                     */
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * 这个方法只有开启了TransientStorePool才需要正常执行到，没有开启的话，没有提交这个过程，数据就直接写到了mappedByteBuffer
     * commit线程将数据从堆外内存提交到内存映射buffer
     * @param commitLeastPages  本次提交的最小的页数，如果待提交数据不满commitLeastPages, 则不执行本次提交操作，待下次提交。
     * @return committedPosition
     */
    public int commit(final int commitLeastPages) {
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            //无需commit data到file channel, 只需把wrotePosition当作committedPosition
            return this.wrotePosition.get();
        }
        /**
         * 判断是否执行commit操作，如果文件已满返回true
         */
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0();
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel. 文件写完，归还writeBuffer，如果开启了transientStorePool的话
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    /**
     * 其作用就是将MappedFile writeBuffer中的数据提交到文件通道fileChannel中
     *
     * 和flush操作的区别在什么地方？
     */
    protected void commit0() {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - lastCommittedPosition > 0) {
            try {
                /**
                 * 先创建writeBuffer的共享缓存区，与原先的byteBuffer共享内存但维护一套独立的
                 */
                ByteBuffer byteBuffer = writeBuffer.slice();//创建共享
                /**
                 * 将新创建的position回退到上一次提交的位置
                 */
                byteBuffer.position(lastCommittedPosition);
                /**
                 * 设置limit为wrotePosition，即当前最大有效数据指针
                 */
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                /**
                 * 把committedPosition到wrotePosition的数据复制到fileChannel中
                 */
                this.fileChannel.write(byteBuffer);
                /**
                 * 更新committedPosition指针为wrotePosition
                 */
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 如果flushLeastPages大于0， 则比较wrotePosition（当前writeBuffer的写指针）与上一次刷盘的指针（flushedPosition)的差值，
     * 除以OS_PAGE_SIZE得到当前脏页的数量，如果大于flushLeastPages则返回true
     *
     * 在flushLeastPages小于等于0的情况下，只要存在脏页就刷盘
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
//        int flush = this.committedPosition.get();
        int commit = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        /**
         * 如果commitLeastPages大于0， 则比较wrotePosition（当前writeBuffer的写指针）与上一次提交的指针（committedPosition)的差值，
         * 除以OS_PAGE_SIZE得到当前脏页的数量，如果大于commitLeastPages则返回true
         *
         * 在commitLeastPages小于等于0的情况下，只要存在脏页就提交
         */
//        if (commitLeastPages > 0) {
//            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
//        }
        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

//        return write > flush;
        return write > commit;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 查找pos到当前最大可读之间的数据
     * 这里为什么这么写，要研究一下 TODO
     * @param pos
     * @return
     */
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();// FIXME 这里为什么创建一个新的？
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        /**
         * 使用父类ReferenceResource的shutdown方法
         */
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeElapsedTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     *
     * 获取当前文件最大的可读指针。如果 writeBuffer 为空，则直接返回当前的写指针wrotePosition;
     * 如果 writeBuffer 不为空，则返回上一次提交的指针committedPosition。
     */
    public int getReadPosition() {
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
