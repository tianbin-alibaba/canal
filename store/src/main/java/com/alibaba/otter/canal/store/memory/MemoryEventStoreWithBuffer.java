package com.alibaba.otter.canal.store.memory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.protocol.position.Position;
import com.alibaba.otter.canal.protocol.position.PositionRange;
import com.alibaba.otter.canal.store.AbstractCanalStoreScavenge;
import com.alibaba.otter.canal.store.CanalEventStore;
import com.alibaba.otter.canal.store.CanalStoreException;
import com.alibaba.otter.canal.store.CanalStoreScavenge;
import com.alibaba.otter.canal.store.helper.CanalEventUtils;
import com.alibaba.otter.canal.store.model.BatchMode;
import com.alibaba.otter.canal.store.model.Event;
import com.alibaba.otter.canal.store.model.Events;

/**
 * 基于内存buffer构建内存memory store
 * 
 * <pre>
 * 变更记录：
 * 1. 新增BatchMode类型，支持按内存大小获取批次数据，内存大小更加可控.
 *   a. put操作，会首先根据bufferSize进行控制，然后再进行bufferSize * bufferMemUnit进行控制. 因存储的内容是以Event，如果纯依赖于memsize进行控制，会导致RingBuffer出现动态伸缩
 * </pre>
 * 
 * @author jianghang 2012-6-20 上午09:46:31
 * @version 1.0.0
 */
public class MemoryEventStoreWithBuffer extends AbstractCanalStoreScavenge implements CanalEventStore<Event>, CanalStoreScavenge {

    private static final long INIT_SEQUENCE = -1;
    private int               bufferSize    = 16 * 1024;
    private int               bufferMemUnit = 1024;                                      // memsize的单位，默认为1kb大小
    private int               indexMask;
    private Event[]           entries;

    // 记录下put/get/ack操作的三个下标
    private AtomicLong        putSequence   = new AtomicLong(INIT_SEQUENCE);             // 代表当前put操作最后一次写操作发生的位置
    private AtomicLong        getSequence   = new AtomicLong(INIT_SEQUENCE);             // 代表当前get操作读取的最后一条的位置
    private AtomicLong        ackSequence   = new AtomicLong(INIT_SEQUENCE);             // 代表当前ack操作的最后一条的位置

    // 记录下put/get/ack操作的三个memsize大小
    private AtomicLong        putMemSize    = new AtomicLong(0);
    private AtomicLong        getMemSize    = new AtomicLong(0);
    private AtomicLong        ackMemSize    = new AtomicLong(0);

    // 记录下put/get/ack操作的三个execTime
    private AtomicLong        putExecTime   = new AtomicLong(System.currentTimeMillis());
    private AtomicLong        getExecTime   = new AtomicLong(System.currentTimeMillis());
    private AtomicLong        ackExecTime   = new AtomicLong(System.currentTimeMillis());

    // 记录下put/get/ack操作的三个table rows
    private AtomicLong        putTableRows  = new AtomicLong(0);
    private AtomicLong        getTableRows  = new AtomicLong(0);
    private AtomicLong        ackTableRows  = new AtomicLong(0);

    // 阻塞put/get操作控制信号
    private ReentrantLock     lock          = new ReentrantLock();
    private Condition         notFull       = lock.newCondition();
    private Condition         notEmpty      = lock.newCondition();

    private BatchMode         batchMode     = BatchMode.ITEMSIZE;                        // 默认为内存大小模式
    private boolean           ddlIsolation  = false;
    private boolean           raw           = true;                                      // 针对entry是否开启raw模式

    public MemoryEventStoreWithBuffer(){

    }

    public MemoryEventStoreWithBuffer(BatchMode batchMode){
        this.batchMode = batchMode;
    }

    public void start() throws CanalStoreException {
        super.start();
        if (Integer.bitCount(bufferSize) != 1) {
            throw new IllegalArgumentException("bufferSize must be a power of 2");
        }

        indexMask = bufferSize - 1;
        entries = new Event[bufferSize];
    }

    public void stop() throws CanalStoreException {
        super.stop();

        cleanAll();
    }

    public void put(List<Event> data) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return;
        }

        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkFreeSlotAt(putSequence.get() + data.size())) { // 检查是否有空位
                    notFull.await(); // wait until not full
                }
            } catch (InterruptedException ie) {
                notFull.signal(); // propagate to non-interrupted thread
                throw ie;
            }
            doPut(data);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean put(List<Event> data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }

        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                if (checkFreeSlotAt(putSequence.get() + data.size())) {
                    doPut(data);
                    return true;
                }
                if (nanos <= 0) {
                    return false;
                }

                try {
                    nanos = notFull.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notFull.signal(); // propagate to non-interrupted thread
                    throw ie;
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean tryPut(List<Event> data) throws CanalStoreException {
        if (data == null || data.isEmpty()) {
            return true;
        }

        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (!checkFreeSlotAt(putSequence.get() + data.size())) {
                return false;
            } else {
                doPut(data);
                return true;
            }
        } finally {
            lock.unlock();
        }
    }

    public void put(Event data) throws InterruptedException, CanalStoreException {
        put(Arrays.asList(data));
    }

    public boolean put(Event data, long timeout, TimeUnit unit) throws InterruptedException, CanalStoreException {
        return put(Arrays.asList(data), timeout, unit);
    }

    public boolean tryPut(Event data) throws CanalStoreException {
        return tryPut(Arrays.asList(data));
    }

    /**
     * 执行具体的put操作
     */
    private void doPut(List<Event> data) {

        //1 将新插入的event数据赋值到Event[]数组的正确位置上
        //1.1 获得putSequence的当前值current，和插入数据后的putSequence结束值end
        long current = putSequence.get();
        long end = current + data.size();

        // 先写数据，再更新对应的cursor,并发度高的情况，putSequence会被get请求可见，拿出了ringbuffer中的老的Entry值
        //1.2 循环需要插入的数据，从current位置开始，到end位置结束
        for (long next = current + 1; next <= end; next++) {

            //1.3 通过getIndex方法对next变量转换成正确的位置，设置到Event[]数组中
            //需要转换的原因在于，这里的Event[]数组是环形队列的底层实现，其大小为bufferSize值，默认为16384。
            //运行一段时间后，接收到的binlog数量肯定会超过16384，每接受到一个event，putSequence+1，因此最终必然超过这个值。
            //而next变量是比当前putSequence值要大的，因此必须进行转换，否则会数组越界，转换工作就是在getIndex方法中进行的。
            entries[getIndex(next)] = data.get((int) (next - current - 1));
        }
        //2 直接设置putSequence为end值，相当于完成event记录数的累加
        putSequence.set(end);

        // 记录一下gets memsize信息，方便快速检索
        //3 累加新插入的event的大小到putMemSize上
        if (batchMode.isMemSize()) {
            //用于记录本次插入的event记录的大小
            long size = 0;
            //循环每一个event
            for (Event event : data) {
                //通过calculateSize方法计算每个event的大小，并累加到size变量上
                size += calculateSize(event);
            }
            //将size变量的值，添加到当前putMemSize
            putMemSize.getAndAdd(size);
        }
        profiling(data, OP.PUT);
        // tell other threads that store is not empty
        // 4 调用notEmpty.signal()方法，通知队列中有数据了，如果之前有client获取数据处于阻塞状态，将会被唤醒
        notEmpty.signal();
    }

    public Events<Event> get(Position start, int batchSize) throws InterruptedException, CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            try {
                while (!checkUnGetSlotAt((LogPosition) start, batchSize))
                    notEmpty.await();
            } catch (InterruptedException ie) {
                notEmpty.signal(); // propagate to non-interrupted thread
                throw ie;
            }

            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    public Events<Event> get(Position start, int batchSize, long timeout, TimeUnit unit) throws InterruptedException,
                                                                                        CanalStoreException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            for (;;) {
                if (checkUnGetSlotAt((LogPosition) start, batchSize)) {
                    return doGet(start, batchSize);
                }

                if (nanos <= 0) {
                    // 如果时间到了，有多少取多少
                    return doGet(start, batchSize);
                }

                try {
                    nanos = notEmpty.awaitNanos(nanos);
                } catch (InterruptedException ie) {
                    notEmpty.signal(); // propagate to non-interrupted thread
                    throw ie;
                }

            }
        } finally {
            lock.unlock();
        }
    }

    public Events<Event> tryGet(Position start, int batchSize) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return doGet(start, batchSize);
        } finally {
            lock.unlock();
        }
    }

    private Events<Event> doGet(Position start, int batchSize) throws CanalStoreException {
        LogPosition startPosition = (LogPosition) start;

        //1 确定从哪个位置开始获取数据
        //获得当前的get位置
        long current = getSequence.get();
        //获得当前的put位置
        long maxAbleSequence = putSequence.get();
        //要获取的第一个Event的位置，一开始等于当前get位置
        long next = current;

       //要获取的最后一个event的位置，一开始也是当前get位置，每获取一个event，end值加1，最大为current+batchSize
        //因为可能进行ddl隔离，因此可能没有获取到batchSize个event就返回了，此时end值就会小于current+batchSize
        long end = current;
        // 如果startPosition为null，说明是第一次，默认+1处理
        if (startPosition == null || !startPosition.getPostion().isIncluded()) { // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
            next = next + 1;
        }

        if (current >= maxAbleSequence) {
            return new Events<Event>();
        }
       //2 如果有数据，根据batchMode是ITEMSIZE或MEMSIZE选择不同的处理方式
        Events<Event> result = new Events<Event>();
        //维护要返回的Event列表
        List<Event> entrys = result.getEvents();
        long memsize = 0;
        //2.1 如果batchMode是ITEMSIZE
        if (batchMode.isItemSize()) {
            end = (next + batchSize - 1) < maxAbleSequence ? (next + batchSize - 1) : maxAbleSequence;
            // 提取数据并返回
            //2.1.1 循环从开始位置(next)到结束位置(end)，每次循环next+1
            for (; next <= end; next++) {
                //2.1.2 获取指定位置上的事件
                Event event = entries[getIndex(next)];
                //2.1.3 果是当前事件是DDL事件，且开启了ddl隔离，本次事件处理完后，即结束循环(if语句最后是一行是break)
                if (ddlIsolation && isDdl(event.getEventType())) {
                    // 如果是ddl隔离，直接返回
                    // 2.1.4 因为ddl事件需要单独返回，因此需要判断entrys中是否应添加了其他事件
                    if (entrys.size() == 0) { //如果entrys中尚未添加任何其他event
                        entrys.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    entrys.add(event);
                }
            }
            //2.2 如果batchMode是MEMSIZE
        } else {

            //2.2.1 计算本次要获取的event占用最大字节数
            long maxMemSize = batchSize * bufferMemUnit;
            //2.2.2 memsize从0开始，当memsize小于maxMemSize且next未超过maxAbleSequence时，可以进行循环
            for (; memsize <= maxMemSize && next <= maxAbleSequence; next++) {
                // 永远保证可以取出第一条的记录，避免死锁
                //2.2.3 获取指定位置上的Event
                Event event = entries[getIndex(next)];
                //2.2.4 果是当前事件是DDL事件，且开启了ddl隔离，本次事件处理完后，即结束循环(if语句最后是一行是break)
                if (ddlIsolation && isDdl(event.getEventType())) {
                    // 如果是ddl隔离，直接返回
                    if (entrys.size() == 0) {
                        entrys.add(event);// 如果没有DML事件，加入当前的DDL事件
                        end = next; // 更新end为当前
                    } else {
                        // 如果之前已经有DML事件，直接返回了，因为不包含当前next这记录，需要回退一个位置
                        end = next - 1; // next-1一定大于current，不需要判断
                    }
                    break;
                } else {
                    //如果没有开启DDL隔离，直接将事件加入到entrys中
                    entrys.add(event);
                    //并将当前添加的event占用字节数累加到memsize变量上
                    memsize += calculateSize(event);
                    end = next;// 记录end位点
                }
            }

        }

        //3 构造PositionRange，表示本次获取的Event的开始和结束位置
        PositionRange<LogPosition> range = new PositionRange<LogPosition>();
        result.setPositionRange(range);

        //3.1 把entrys列表中的第一个event的位置，当做PositionRange的开始位置
        range.setStart(CanalEventUtils.createPosition(entrys.get(0)));
        range.setEnd(CanalEventUtils.createPosition(entrys.get(result.getEvents().size() - 1)));
        range.setEndSeq(end);
        // 记录一下是否存在可以被ack的点
        for (int i = entrys.size() - 1; i >= 0; i--) {
            Event event = entrys.get(i);
            // GTID模式,ack的位点必须是事务结尾,因为下一次订阅的时候mysql会发送这个gtid之后的next,如果在事务头就记录了会丢这最后一个事务
            //4.1.1 如果是事务开始/事务结束/或者dll事件，
            if ((CanalEntry.EntryType.TRANSACTIONBEGIN == event.getEntryType() && StringUtils.isEmpty(event.getGtid()))
                || CanalEntry.EntryType.TRANSACTIONEND == event.getEntryType() || isDdl(event.getEventType())) {
                // 将事务头/尾设置可被为ack的点
                range.setAck(CanalEventUtils.createPosition(event));
                break;
            }
            //4.1.3 如果没有这三种类型事件，意味着没有可被ack的点
        }

         //5 累加getMemSize值，getMemSize值
        //5.1 通过AtomLong的compareAndSet尝试增加getSequence值
        if (getSequence.compareAndSet(current, end)) {
            getMemSize.addAndGet(memsize);
            notFull.signal();
            profiling(result.getEvents(), OP.GET);
            return result;
        } else {
            return new Events<Event>();
        }
    }
    //获取第一条数据的position，如果没有数据返回为null
    public LogPosition getFirstPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long firstSeqeuence = ackSequence.get();
            //1 没有ack过数据，且队列中有数据
            if (firstSeqeuence == INIT_SEQUENCE && firstSeqeuence < putSequence.get()) {
                //没有ack过数据，那么ack为初始值-1，又因为队列中有数据，因此ack+1,即返回队列中第一条数据的位置
                Event event = entries[getIndex(firstSeqeuence + 1)]; // 最后一次ack为-1，需要移动到下一条,included
                                                                     // = false
                return CanalEventUtils.createPosition(event, false);
                //2 已经ack过数据，但是未追上put操作
            } else if (firstSeqeuence > INIT_SEQUENCE && firstSeqeuence < putSequence.get()) {
                // ack未追上put操作
                //返回最后一次ack的位置数据 + 1
                Event event = entries[getIndex(firstSeqeuence)]; // 最后一次ack的位置数据,需要移动到下一条,included
                // = false
                return CanalEventUtils.createPosition(event, false);
                //3 已经ack过数据，且已经追上put操作，说明队列中所有数据都被消费完了
            } else if (firstSeqeuence > INIT_SEQUENCE && firstSeqeuence == putSequence.get()) {
                // 已经追上，store中没有数据
                // 最后一次ack的位置数据，和last为同一条
                Event event = entries[getIndex(firstSeqeuence)]; // 最后一次ack的位置数据，和last为同一条，included
                                                                 // = false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 没有任何数据
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public LogPosition getLatestPosition() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            long latestSequence = putSequence.get();
            if (latestSequence > INIT_SEQUENCE && latestSequence != ackSequence.get()) {
                Event event = entries[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，最后一条未消费的数据
                return CanalEventUtils.createPosition(event, true);
            } else if (latestSequence > INIT_SEQUENCE && latestSequence == ackSequence.get()) {
                // ack已经追上了put操作
                Event event = entries[(int) putSequence.get() & indexMask]; // 最后一次写入的数据，included
                                                                            // =
                                                                            // false
                return CanalEventUtils.createPosition(event, false);
            } else {
                // 没有任何数据
                return null;
            }
        } finally {
            lock.unlock();
        }
    }

    public void ack(Position position) throws CanalStoreException {
        cleanUntil(position, -1L);
    }

    public void ack(Position position, Long seqId) throws CanalStoreException {
        cleanUntil(position, seqId);
    }

    @Override
    public void cleanUntil(Position position) throws CanalStoreException {
        cleanUntil(position, -1L);
    }
    // postion表示要ack的配置
    public void cleanUntil(Position position, Long seqId) throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            //获得当前ack值
            long sequence = ackSequence.get();
            //获得当前get值
            long maxSequence = getSequence.get();

            boolean hasMatch = false;
            long memsize = 0;
            // ack没有list，但有已存在的foreach，还是节省一下list的开销
            long localExecTime = 0L;
            int deltaRows = 0;
            if (seqId > 0) {
                maxSequence = seqId;
            }

            //迭代所有未被ack的event，从中找出与需要ack的position相同位置的event，清空这个event之前的所有数据。
            //一旦找到这个event，循环结束。
            for (long next = sequence + 1; next <= maxSequence; next++) {
                //获得要ack的event
                Event event = entries[getIndex(next)];
                if (localExecTime == 0 && event.getExecuteTime() > 0) {
                    localExecTime = event.getExecuteTime();
                }
                deltaRows += event.getRowsCount();
                //计算当前要ack的event占用字节数
                memsize += calculateSize(event);
                // 找到对应的position，更新ack seq
                if ((seqId < 0 || next == seqId) && CanalEventUtils.checkPosition(event, (LogPosition) position)) {
                    // 找到对应的position，更新ack seq
                    hasMatch = true;
                     //如果batchMode是MEMSIZE
                    if (batchMode.isMemSize()) {
                        //累加ackMemSize
                        ackMemSize.addAndGet(memsize);
                        // 尝试清空buffer中的内存，将ack之前的内存全部释放掉
                        for (long index = sequence + 1; index < next; index++) {
                            entries[getIndex(index)] = null;// 设置为null
                        }

                        // 考虑getFirstPosition/getLastPosition会获取最后一次ack的position信息
                        // ack清理的时候只处理entry=null，释放内存
                        Event lastEvent = entries[getIndex(next)];
                        lastEvent.setEntry(null);
                        lastEvent.setRawEntry(null);
                    }

                    //累加ack值
                    //官方注释说，采用compareAndSet，是为了避免并发ack。我觉得根本不会并发ack，因为都加锁了
                    if (ackSequence.compareAndSet(sequence, next)) {// 避免并发ack
                        //如果之前存在put操作因为队列满了而被阻塞，通知其队列有了新空间
                        notFull.signal();
                        ackTableRows.addAndGet(deltaRows);
                        if (localExecTime > 0) {
                            ackExecTime.lazySet(localExecTime);
                        }
                        return;
                    }
                }
            }
            if (!hasMatch) {// 找不到对应需要ack的position
                throw new CanalStoreException("no match ack position" + position.toString());
            }
        } finally {
            lock.unlock();
        }
    }

    public void rollback() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            getSequence.set(ackSequence.get());
            getMemSize.set(ackMemSize.get());
        } finally {
            lock.unlock();
        }
    }

    public void cleanAll() throws CanalStoreException {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            putSequence.set(INIT_SEQUENCE);
            getSequence.set(INIT_SEQUENCE);
            ackSequence.set(INIT_SEQUENCE);

            putMemSize.set(0);
            getMemSize.set(0);
            ackMemSize.set(0);
            entries = null;
            // for (int i = 0; i < entries.length; i++) {
            // entries[i] = null;
            // }
        } finally {
            lock.unlock();
        }
    }

    // =================== helper method =================

    private long getMinimumGetOrAck() {
        long get = getSequence.get();
        long ack = ackSequence.get();
        return ack <= get ? ack : get;
    }

    /**
     * 查询是否有空位
     */
    private boolean checkFreeSlotAt(final long sequence) {

        //1、检查是否足够的slot。注意方法参数传入的sequence值是：当前putSequence值 + 新插入的event的记录数。
        //按照前面的说明，其减去bufferSize不能大于ack位置，或者换一种说法，减去bufferSize不能大于ack位置。
        //1.1 首先用sequence值减去bufferSize
        final long wrapPoint = sequence - bufferSize;
        //1.2 获取get位置ack位置的较小值，事实上，ack位置总是应该小于等于get位置，因此这里总是应该返回的是ack位置。
        final long minPoint = getMinimumGetOrAck();
        //1.3 将1.1 与1.2步得到的值进行比较，如果前者大，说明二者差值已经超过了bufferSize，不能插入数据，返回false
        if (wrapPoint > minPoint) { // 刚好追上一轮
            return false;
        } else {
            // 在bufferSize模式上，再增加memSize控制
            //2 如果batchMode是MEMSIZE，继续检查是否超出了内存限制。
            if (batchMode.isMemSize()) {
                //2.1 使用putMemSize值减去ackMemSize值，得到当前保存的event事件占用的总内存
                final long memsize = putMemSize.get() - ackMemSize.get();
                //2.2 如果没有超出bufferSize * bufferMemUnit内存限制，返回true，否则返回false
                if (memsize < bufferSize * bufferMemUnit) {
                    return true;
                } else {
                    return false;
                }
            } else {
                //3 如果batchMode不是MEMSIZE，说明只限制记录数，则直接返回true
                return true;
            }
        }
    }

    /**
     * 检查是否存在需要get的数据,并且数量>=batchSize
     */
    private boolean checkUnGetSlotAt(LogPosition startPosition, int batchSize) {
        //1 如果batchMode为ITEMSIZE
        if (batchMode.isItemSize()) {
            long current = getSequence.get();
            long maxAbleSequence = putSequence.get();
            long next = current;
            //1.1 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录。
            if (startPosition == null || !startPosition.getPostion().isIncluded()) { // 第一次订阅之后，需要包含一下start位置，防止丢失第一条记录
                next = next + 1;// 少一条数据
            }

            //1.2 理论上只需要满足条件：putSequence - getSequence >= batchSize
            //1.2.1 先通过current < maxAbleSequence进行一下简单判断，如果不满足，可以直接返回false了
            //1.2.2 如果1.2.1满足，再通过putSequence - getSequence >= batchSize判断是否有足够的数据
            if (current < maxAbleSequence && next + batchSize - 1 <= maxAbleSequence) {
                return true;
            } else {
                return false;
            }
        } else {
            //2 如果batchMode为MEMSIZE
            // 处理内存大小判断
            long currentSize = getMemSize.get();
            long maxAbleSize = putMemSize.get();
            //2.1 需要满足条件 putMemSize-getMemSize >= batchSize * bufferMemUnit
            if (maxAbleSize - currentSize >= batchSize * bufferMemUnit) {
                return true;
            } else {
                return false;
            }
        }
    }

    private long calculateSize(Event event) {
        // 直接返回binlog中的事件大小
        return event.getRawLength();
    }

    private int getIndex(long sequcnce) {
        return (int) sequcnce & indexMask;
    }

    private boolean isDdl(EventType type) {
        return type == EventType.ALTER || type == EventType.CREATE || type == EventType.ERASE
               || type == EventType.RENAME || type == EventType.TRUNCATE || type == EventType.CINDEX
               || type == EventType.DINDEX;
    }

    private void profiling(List<Event> events, OP op) {
        long localExecTime = 0L;
        int deltaRows = 0;
        if (events != null && !events.isEmpty()) {
            for (Event e : events) {
                if (localExecTime == 0 && e.getExecuteTime() > 0) {
                    localExecTime = e.getExecuteTime();
                }
                deltaRows += e.getRowsCount();
            }
        }
        switch (op) {
            case PUT:
                putTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    putExecTime.lazySet(localExecTime);
                }
                break;
            case GET:
                getTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    getExecTime.lazySet(localExecTime);
                }
                break;
            case ACK:
                ackTableRows.addAndGet(deltaRows);
                if (localExecTime > 0) {
                    ackExecTime.lazySet(localExecTime);
                }
                break;
            default:
                break;
        }
    }

    private enum OP {
        PUT, GET, ACK
    }

    // ================ setter / getter ==================
    public int getBufferSize() {
        return this.bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void setBufferMemUnit(int bufferMemUnit) {
        this.bufferMemUnit = bufferMemUnit;
    }

    public void setBatchMode(BatchMode batchMode) {
        this.batchMode = batchMode;
    }

    public void setDdlIsolation(boolean ddlIsolation) {
        this.ddlIsolation = ddlIsolation;
    }

    public boolean isRaw() {
        return raw;
    }

    public void setRaw(boolean raw) {
        this.raw = raw;
    }

    public AtomicLong getPutSequence() {
        return putSequence;
    }

    public AtomicLong getAckSequence() {
        return ackSequence;
    }

    public AtomicLong getPutMemSize() {
        return putMemSize;
    }

    public AtomicLong getAckMemSize() {
        return ackMemSize;
    }

    public BatchMode getBatchMode() {
        return batchMode;
    }

    public AtomicLong getPutExecTime() {
        return putExecTime;
    }

    public AtomicLong getGetExecTime() {
        return getExecTime;
    }

    public AtomicLong getAckExecTime() {
        return ackExecTime;
    }

    public AtomicLong getPutTableRows() {
        return putTableRows;
    }

    public AtomicLong getGetTableRows() {
        return getTableRows;
    }

    public AtomicLong getAckTableRows() {
        return ackTableRows;
    }

}
