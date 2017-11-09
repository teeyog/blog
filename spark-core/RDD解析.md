> RDD（Resilient Distributed Dataset）：弹性分布式数据集。

##特性
- A list of partitions (可分片)
- A function for computing each split (compute func)
- A list of dependencies on other RDDs (依赖)
- A Partitioner for key-value RDDs (分片器，决定一条数据属于某分片)
- A list of preferred locations to compute each split on (e.g. block locations for an HDFS file) (位置优先)

 ## Partition 
1. RDD能并行计算的原因就是Partition，一个RDD可有多个partition，每个partition一个task任务，每个partition代表了该RDD一部分数据，分区内部并不会存储具体的数据，访问数据时是通过partition的迭代器，iterator 可遍历到所有数据。
2. partition的个数需要视情况而定，RDD 可以通过创建操作或者转换操作得到，转换操作中，分区的个数会根据转换操作对应多个 RDD 之间的依赖关系确定，窄依赖子 RDD 由父 RDD 分区个数决定，Shuffle 依赖由子 RDD 分区器决定，从集合中创建RDD时默认个数为defaultParallelism，当该值没有设定时：
    - 本地模式： conf.getInt("spark.default.parallelism", totalCores) // CPU cores
    -  Mesos：  conf.getInt("spark.default.parallelism", 8)  // 8
    - Standalone&Yarn： conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))
3. 特质Partition只有一个返回index的方法，很多具体的 RDD 也会有自己实现的 partition。
```
  trait Partition extends Serializable { 
    def index: Int
    override def hashCode(): Int = index
    override def equals(other: Any): Boolean = super.equals(other)
  }
```

## compute func
每个具体的RDD都得实现compute 方法，该方法接受的参数之一是一个Partition 对象，目的是计算该分区中的数据。
我们通过map方法来看具体的实现：
```
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }
```
调用map时都会new一个MapPartitionsRDD实例，并且接收一个方法作为参数，该方法接收一个迭代器（后面会细讲），对该RDD的map操作函数f将作用于这个迭代器的每一条数据。在MapPartitionsRDD中是通过compute方法来计算对应分区的数据：
```
override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))
```
这里将调用该RDD(MapPartitionsRDD)内的第一个父 RDD 的 iterator 方法，该方的目的是拉取父 RDD 对应分区内的数据。iterator方法会返回一个迭代器，对应的是父RDD计算完成的数据，该迭代器将作为 f 方法的一个参数，该f 方法就是上面提到的创建MapPartitionsRDD实例时传入的方法。

其实RDD的compute方法也类似。接下来我们看看iterator方法究竟都做了什么事：
```
final def iterator(split: Partition, context: TaskContext): Iterator[T] = {
    if (storageLevel != StorageLevel.NONE) {
      getOrCompute(split, context)
    } else {
      computeOrReadCheckpoint(split, context)
    }
  }
```
RDD的iterator方法即遍历对应分区的数据，先判断改RDD的存储级别若不为NONE，则说明该数据已经存在于缓存中，RDD 经过持久化操作并经历了一次计算过程 ，可直接将数据返回。
```
private[spark] def getOrCompute(partition: Partition, context: TaskContext): Iterator[T] = {
    val blockId = RDDBlockId(id, partition.index)
    var readCachedBlock = true
    // This method is called on executors, so we need call SparkEnv.get instead of sc.env.
    SparkEnv.get.blockManager.getOrElseUpdate(blockId, storageLevel, elementClassTag, () => {
      readCachedBlock = false
      computeOrReadCheckpoint(partition, context)
    }) match {
      case Left(blockResult) =>
        if (readCachedBlock) {
          val existingMetrics = context.taskMetrics().inputMetrics
          existingMetrics.incBytesRead(blockResult.bytes)
          new InterruptibleIterator[T](context, blockResult.data.asInstanceOf[Iterator[T]]) {
            override def next(): T = {
              existingMetrics.incRecordsRead(1)
              delegate.next()
            }
          }
        } else {
          new InterruptibleIterator(context, blockResult.data.asInstanceOf[Iterator[T]])
        }
       ...
    }
  }
```
通过RDD_id和partition_index唯一表示一个block，先从缓存中取数据，也有可能取不到数据
 - 数据丢失
 - RDD 经过持久化操作，但是是当前分区数据是第一次被计算，因此会出现拉取得到数据为 None

取不到的时候则调用computeOrReadCheckpoint来获取并加入缓存。
当RDD的存储级别若为NONE，则需要直接通过computeOrReadCheckpoint方法来计算。
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
该方法会先检查当前RDD是否被checkpoint，若是则直接从依赖的checkpointRDD中获取迭代对象，若不是则需要通过compute方法计算。

## dependency
RDD的容错机制就是通过dependency实现的，在外部成为血统（Lineage）关系，在源码里面实为dependency，抽象类Dependency只有一个返回对应RDD的方法。
```
abstract class Dependency[T] extends Serializable {
  def rdd: RDD[T]
}
```
每个RDD都有一个返回其所依赖的dependences:Seq[Dependency[_]] 的dependencies方法，Dependency里面存的就是父RDD，递归RDD+遍历这个dependences将可得到整个DAG。

依赖分为两种，分别是窄依赖（Narrow Dependency）和 Shuffle 依赖（Shuffle Dependency，也称即宽依赖）。在窄依赖中，父RDD的一个分区至多被一个子RDD的一个分区所依赖，分区数据不可被拆分：

![](http://upload-images.jianshu.io/upload_images/3597066-5423426a34098ae9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

在宽依赖中，父RDD的一个分区被子RDD的多个分区所依赖，分区数据被拆分：

![](http://upload-images.jianshu.io/upload_images/3597066-f990347c08e60253.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

一次转换操作可同时包含窄依赖和宽依赖：

![](http://upload-images.jianshu.io/upload_images/3597066-beb34a8a2703622f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

窄依赖的抽象类为NarrowDependency，对应实现分别是 OneToOneDependency （一对一依赖）类和RangeDependency （范围依赖）类。

一对一依赖表示子 RDD 分区的编号与父 RDD 分区的编号完全一致的情况，若两个 RDD 之间存在着一对一依赖，则子 RDD 的分区个数、分区内记录的个数都将继承自父 RDD。

范围依赖是依赖关系中的一个特例，只被用于表示 UnionRDD 与父 RDD 之间的依赖关系。

宽依赖的对应实现为 ShuffleDependency 类，宽依赖支持两种 Shuffle Manager，即 HashShuffleManager 和 SortShuffleManager

## Partitioner 
partitioner就是决定一条数据应该数据哪个分区的分区器，但只有 k, v 类型的 RDD 才能有 partitioner，因为都是由其 k 来决定的。
特质 Partitioner提供了一个返回分区index的方法，通过传入k及指定的分区个数：
```
trait Partitioner { 
  def partition(key: Any, numPartitions: Int): Int
}
```
Spark 内置了两种分区器，分别是哈希分区器（Hash Partitioner）和范围分区器（Range Partitioner）。
#### Hash Partitioner
我们来看HashPartitioner的定义，主要是getPartition方法，当key为null时直接返回null
```
class HashPartitioner(partitions: Int) extends Partitioner {
  def numPartitions: Int = partitions
  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => Utils.nonNegativeMod(key.hashCode, numPartitions)
  }
  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }
  override def hashCode: Int = numPartitions
}
```
不为null时将调用Utils的nonNegativeMod方法，即将key的hashcode和mod取余，若结果为正，则返回该结果；若结果为负，返回结果加上 mod。
```
 def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }
```
#### RangePartitioner
在key的hashcode分布不均的情况下会到导致通过HashPartitioner分出来的分区数据倾斜不均匀，这是就需要用到RangePartitioner分区器，该分区器运行速度相对HashPartitioner较慢，原理复杂。

HashPartitioner会将一个范围的key直接映射到一个partition，也就是一个partition的key一定比另一个partition的key都大或者都小，而怎么具体划分这个范围的边界成为关键，既要保证分布均匀又要减少遍历次数。具体实现可见 [Spark分区器HashPartitioner和RangePartitioner代码详解](https://www.iteblog.com/archives/1522.html)

## preferred locations
每个具体的RDD实例都需要实现自己的getPreferredLocations方法，RDD位置优先即返回partition的存储位置，该位置和spark的任务调度有关，尽量将计算移到该partition对应的地方。
以从Hadoop中读取数据生成RDD为例，preferredLocations返回每一个数据块所在的机器名或者IP地址，如果每一个数据块是多份存储的（HDFS副本数），那么就会返回多个机器地址。
