> spark的缓存机制保证了需要访问重复数据的应用（如迭代型算法和交互式应用）可以运行的更快。

完整的存储级别介绍如下所示：

| Storage Level| Meaning| 
| ------------- |:-------------:|
| MEMORY_ONLY      | 将RDD作为非序列化的Java对象存储在jvm中。如果RDD不能被内存装下，一些分区将不会被缓存，并且在需要的时候被重新计算。这是系统默认的存储级别。 |
| MEMORY_AND_DISK      | 将RDD作为非序列化的Java对象存储在jvm中。如果RDD不能被与内存装下，超出的分区将被保存在硬盘上，并且在需要时被读取。|
| MEMORY_ONLY_SER | 将RDD作为序列化的Java对象存储（每个分区一个byte数组）。这种方式比非序列化方式更节省空间，特别是用到快速的序列化工具时，但是会更耗费cpu资源—密集的读操作。      |
|MEMORY_AND_DISK_SER | 和MEMORY_ONLY_SER类似，但不是在每次需要时重复计算这些不适合存储到内存中的分区，而是将这些分区存储到磁盘中。      |
| DISK_ONLY| 仅仅将RDD分区存储到磁盘中      |
| MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc.| 和上面的存储级别类似，但是复制每个分区到集群的两个节点上面     |

 ## 如何使用
我们可以利用不同的存储级别存储每一个被持久化的RDD。可以存储在内存中，也可以序列化后存储在磁盘上等方式。Spark也会自动持久化一些shuffle操作（如reduceByKey）中的中间数据，即使用户没有调用persist方法。这样的好处是避免了在shuffle出错情况下，需要重复计算整个输入。

系统将要计算 RDD partition 的时候就去判断 partition 要不要被 cache。如果要被 cache 的话，先将 partition 计算出来，然后 cache 到内存。

我们也可以通过persist()或者cache()方法持久化一个rdd，但只有当action操作时才会触发cache的真正执行，下面看看两者的区别：
```
def cache(): this.type = persist()

def persist(): this.type = persist(StorageLevel.MEMORY_ONLY)

def persist(newLevel: StorageLevel): this.type = {
    if (isLocallyCheckpointed) {  //该RDD之前被checkpoint过，说明RDD已经被缓存过。
       //我们只需要直接覆盖原来的存储级别即可
      persist(LocalRDDCheckpointData.transformStorageLevel(newLevel), allowOverride = true)
    } else {
      persist(newLevel, allowOverride = false)
    }
  }

private def persist(newLevel: StorageLevel, allowOverride: Boolean): this.type = {
    // 原来的存储级别不为NONE；新存储级别！=原来的存储界别；不允许覆盖
    if (storageLevel != StorageLevel.NONE && newLevel != storageLevel && !allowOverride) {
      throw new UnsupportedOperationException(  
        "Cannot change storage level of an RDD after it was already assigned a level")
    }
    if (storageLevel == StorageLevel.NONE) {  // 第一次调用persist
      sc.cleaner.foreach(_.registerRDDForCleanup(this))  // 通过sc来清理注册
      sc.persistRDD(this) //缓存RDD
    }
    storageLevel = newLevel //跟新存储级别
    this
  }
```
可以直观的看到cache直接调用了无参的persist()，该方法即默认使用了StorageLevel.MEMORY_ONLY级别的存储，另外两个重载的方法细节可看代码中的注释。

## 什么时候会用到缓存的RDD
当真正需要计算某个分区的数据时，将会触发RDD的iterator方法执行，该方法会返回一个迭代器，迭代器可遍历分区所有数据。
```
final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      getOrCompute(split, context)
    } else {
      computeOrReadCheckpoint(split, context)
    }
  }
```
执行的第一步便是检查当前RDD的存储级别，若不为NONE则之前肯定对RDD执行过persist操作，继续跟进getOrCompute方法
```
private[spark] def getOrCompute(partition: Partition, context: TaskContext): Iterator[T] = {
    val blockId = RDDBlockId(id, partition.index)
    var readCachedBlock = true
    // This method is called on executors, so we need call SparkEnv.get instead of sc.env.
    SparkEnv.get.blockManager.getOrElseUpdate(blockId, storageLevel, elementClassTag, () => {
      readCachedBlock = false
      computeOrReadCheckpoint(partition, context)
    }) match {
       ...
    }
  }
```
通过rddid和partitionid唯一标示一个block，由blockManager的方法getOrElseUpdate获取对应的block，若未获取到则执行computeOrReadCheckpoint来获取，未获取到的原因可能是数据丢失或者该rdd被persist了但是是第一次计算，跟进方法getOrElseUpdate：
```
 def getOrElseUpdate[T](
      blockId: BlockId,
      level: StorageLevel,
      classTag: ClassTag[T],
      makeIterator: () => Iterator[T]): Either[BlockResult, Iterator[T]] = {
    // Attempt to read the block from local or remote storage. If it's present, then we don't need
    // to go through the local-get-or-put path.
    get[T](blockId)(classTag) match {
      case Some(block) =>
        return Left(block)
      case _ =>
        // Need to compute the block.
    }
    // Initially we hold no locks on this block.
    doPutIterator(blockId, makeIterator, level, classTag, keepReadLock = true) match {
          ...
    }
  }
```
getOrElseUpdate方法中会尝试从本地或者远程存储介质中获取数据，若为获取到则会通过computeOrReadCheckpoint来获取数据，该方法也在存储级别为NONE时调用，跟进方法computeOrReadCheckpoint：
```
private[spark] def computeOrReadCheckpoint(split: Partition, context: TaskContext): Iterator[T] =
  {
    if (isCheckpointedAndMaterialized) {
      firstParent[T].iterator(split, context)
    } else {
      compute(split, context)
    }
  }
```
若当前RDD被checkpoint过，则直接调用其父RDD checkpointRDD的iterator方法来获取数据，最后实在取不到数据就只有通过RDD的compute计算出来了。

## 获取 cached partitions 的存储位置
partition 被 cache 后所在节点上的 blockManager 会通知 driver 上的 blockMangerMasterActor 说某 rdd 的 partition 已经被我 cache 了，这个信息会存储在 blockMangerMasterActor 的 blockLocations: HashMap中。等到 task 执行需要 cached rdd 的时候，会调用 blockManagerMaster 的 getLocations(blockId) 去询问某 partition 的存储位置，这个询问信息会发到 driver 那里，driver 查询 blockLocations 获得位置信息并将信息送回。

读取其他节点上的 cached partition：task 得到 cached partition 的位置信息后，将 GetBlock(blockId) 的请求通过 connectionManager 发送到目标节点。目标节点收到请求后从本地 blockManager 那里的 memoryStore 读取 cached partition，最后发送回来。

