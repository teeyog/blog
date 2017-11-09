## 概述
Spark Application只有遇到action操作时才会真正的提交任务并进行计算，DAGScheduler 会根据各个RDD之间的依赖关系形成一个DAG，并根据ShuffleDependency来进行stage的划分，stage包含多个tasks，个数由该stage的finalRDD决定，stage里面的task完全相同，DAGScheduler 完成stage的划分后基于每个Stage生成TaskSet，并提交给TaskScheduler，TaskScheduler负责具体的task的调度，在Worker节点上启动task。
![](http://upload-images.jianshu.io/upload_images/3597066-fc535e47d140e361.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## Job的提交
以count为例，直接看源码都有哪些步骤：
```
def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum
    DAGScheduler#runJob
        DAGScheduler#runJob
            DAGScheduler#runJob
                DAGScheduler#dagScheduler.runJob
                    DAGScheduler#submitJob
                        eventProcessLoop.post(JobSubmitted(**))
```
eventProcessLoop是一个DAGSchedulerEventProcessLoop(this)对象，可以把DAGSchedulerEventProcessLoop理解成DAGScheduler的对外的功能接口。它对外隐藏了自己内部实现的细节。无论是内部还是外部消息，DAGScheduler可以共用同一消息处理代码，逻辑清晰，处理方式统一。
eventProcessLoop接收各种消息并进行处理，处理的逻辑在其doOnReceive方法中：
```
 private def doOnReceive(event: DAGSchedulerEvent): Unit = event match {
    case JobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties) =>
      dagScheduler.handleJobSubmitted(jobId, rdd, func, partitions, callSite, listener, properties)

    case MapStageSubmitted(jobId, dependency, callSite, listener, properties) =>
      dagScheduler.handleMapStageSubmitted(jobId, dependency, callSite, listener, properties)

    ......
}
```
当提交的是JobSubmitted，便会通过 dagScheduler.handleJobSubmitted处理此事件。

## Stage的划分
在handleJobSubmitted方法中第一件事情就是通过finalRDD向前追溯对Stage的划分。
  ```
private[scheduler] def handleJobSubmitted(jobId: Int,
      finalRDD: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      callSite: CallSite,
      listener: JobListener,
      properties: Properties) {
    var finalStage: ResultStage = null
    try { 
   //Stage划分过程是从最后一个Stage开始往前执行的，最后一个Stage的类型是ResultStage
      finalStage = newResultStage(finalRDD, func, partitions, jobId, callSite)
    } catch {
      case e: Exception =>
        logWarning("Creating new stage failed due to exception - job: " + jobId, e)
        listener.jobFailed(e)
        return
    }
    //为此job生成一个ActiveJob对象
    val job = new ActiveJob(jobId, finalStage, callSite, listener, properties)
    clearCacheLocs()
    logInfo("Got job %s (%s) with %d output partitions".format(
      job.jobId, callSite.shortForm, partitions.length))
    logInfo("Final stage: " + finalStage + " (" + finalStage.name + ")")
    logInfo("Parents of final stage: " + finalStage.parents)
    logInfo("Missing parents: " + getMissingParentStages(finalStage))

    val jobSubmissionTime = clock.getTimeMillis()
    jobIdToActiveJob(jobId) = job //记录该job处于active状态
    activeJobs += job 
    finalStage.setActiveJob(job)
    val stageIds = jobIdToStageIds(jobId).toArray
    val stageInfos = stageIds.flatMap(id => stageIdToStage.get(id).map(_.latestInfo))
    listenerBus.post( //向LiveListenerBus发送Job提交事件
      SparkListenerJobStart(job.jobId, jobSubmissionTime, stageInfos, properties))
    submitStage(finalStage) //提交Stage

    submitWaitingStages()
  }
```
跟进newResultStage方法：
```
private def newResultStage(
      rdd: RDD[_],
      func: (TaskContext, Iterator[_]) => _,
      partitions: Array[Int],
      jobId: Int,
      callSite: CallSite): ResultStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, jobId) //获取stage的parentstage
    val stage = new ResultStage(id, rdd, func, partitions, parentStages, jobId, callSite)
    stageIdToStage(id) = stage //将Stage和stage_id关联
    updateJobIdStageIdMaps(jobId, stage) //跟新job所包含的stage
    stage
  }
```
直接实例化一个ResultStage，但需要parentStages作为参数，我们看看getParentStagesAndId做了什么：
```
private def getParentStagesAndId(rdd: RDD[_], firstJobId: Int): (List[Stage], Int) = {
    val parentStages = getParentStages(rdd, firstJobId)
    val id = nextStageId.getAndIncrement()
    (parentStages, id)
  }
```
获取parentStages，并返回一个与stage关联的唯一id，由于是递归的向前生成stage，所以最先生成的stage是最前面的stage，越往前的stageId就越小，即父Stage的id最小。继续跟进getParentStages：
```
private def getParentStages(rdd: RDD[_], firstJobId: Int): List[Stage] = {
    val parents = new HashSet[Stage] // 当前Stage的所有parent Stage
    val visited = new HashSet[RDD[_]] // 已经访问过的RDD
    // We are manually maintaining a stack here to prevent StackOverflowError
    // caused by recursively visiting
    val waitingForVisit = new Stack[RDD[_]] //等待访问的RDD
    def visit(r: RDD[_]) {
      if (!visited(r)) { //若未访问过
        visited += r  //标记已被访问
        // Kind of ugly: need to register RDDs with the cache here since
        // we can't do it in its constructor because # of partitions is unknown
        for (dep <- r.dependencies) { //遍历其所有依赖
          dep match {
            case shufDep: ShuffleDependency[_, _, _] => //若为宽依赖，则生成新的Stage，shuffleMapstage
              parents += getShuffleMapStage(shufDep, firstJobId)
            case _ => //若为窄依赖（归为当前Stage），压入栈，继续向前循环，直到遇到宽依赖或者无依赖
              waitingForVisit.push(dep.rdd)
          }
        }
      }
    }
    waitingForVisit.push(rdd) //将当前rdd压入栈
    while (waitingForVisit.nonEmpty) { //等待访问的rdd不为空时继续访问
      visit(waitingForVisit.pop())
    }
    parents.toList
  }
```
通过给定的RDD返回其依赖的Stage集合。通过RDD每一个依赖进行遍历，遇到窄依赖就继续往前遍历，遇到ShuffleDependency便通过getShuffleMapStage返回一个ShuffleMapStage对象添加到父Stage列表中。可见，这里的parentStage是Stage直接依赖的父stages（parentStage也有自己的parentStage），而不是整个DAG的所有stages。继续跟进getShuffleMapStage的实现：
```
private def getShuffleMapStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    shuffleToMapStage.get(shuffleDep.shuffleId) match {
      case Some(stage) => stage //若已经在shuffleToMapStage存在直接返回Stage
      case None => //不存在需要生成新的Stage
        //为当前shuffle的父shuffle都生成一个ShuffleMapStage
       getAncestorShuffleDependencies(shuffleDep.rdd).foreach { dep =>
          if (!shuffleToMapStage.contains(dep.shuffleId)) {
            shuffleToMapStage(dep.shuffleId) = newOrUsedShuffleStage(dep, firstJobId) //跟新shuffleToMapStage映射
          }
        }
        // 为当前shuffle生成新的Stage
        val stage = newOrUsedShuffleStage(shuffleDep, firstJobId)
        shuffleToMapStage(shuffleDep.shuffleId) = stage
        stage
    }
  }
```
先从shuffleToMapStage根据shuffleid获取Stage，若未获取到再去计算，第一次都肯定为None，我们先看getAncestorShuffleDependencies干了什么：
```
 private def getAncestorShuffleDependencies(rdd: RDD[_]): Stack[ShuffleDependency[_, _, _]] = {
    val parents = new Stack[ShuffleDependency[_, _, _]] // 当前shuffleDependency所有的祖先ShuffleDependency（不是直接ShuffleDependency）
    val visited = new HashSet[RDD[_]] // 已经被访问过的RDD
    // 等待被访问的RDD
    val waitingForVisit = new Stack[RDD[_]]
    def visit(r: RDD[_]) {
      if (!visited(r)) { //未被访问过
        visited += r //标记已被访问
        for (dep <- r.dependencies) { //遍历直接依赖
          dep match {
            case shufDep: ShuffleDependency[_, _, _] => 
              if (!shuffleToMapStage.contains(shufDep.shuffleId)) { // 若为shuffleDependency并且还没有映射，则添加到parents 
                parents.push(shufDep)
              }
            case _ =>
          }
          waitingForVisit.push(dep.rdd)  //即使是shuffleDependency的rdd也要继续遍历
        }
      }
    }

    waitingForVisit.push(rdd)
    while (waitingForVisit.nonEmpty) {
      visit(waitingForVisit.pop())
    }
    parents
  }
```
貌似和getParentStages方法很像，区别是这里获取的所有祖先ShuffleDependency，而不是直接父ShuffleDependency。

为当前shuffle的父shuffle都生成一个ShuffleMapStage后再通过newOrUsedShuffleStage获取当前依赖的shuffleStage，再和shuffleid关联起来，看newOrUsedShuffleStage的实现：
```
private def newOrUsedShuffleStage(
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int): ShuffleMapStage = {
    val rdd = shuffleDep.rdd //依赖对应的rdd
    val numTasks = rdd.partitions.length //分区个数
    val stage = newShuffleMapStage(rdd, numTasks, shuffleDep, firstJobId, rdd.creationSite) //返回当前rdd的shufflestage
    if (mapOutputTracker.containsShuffle(shuffleDep.shuffleId)) {
    //如果当前shuffle已经在MapOutputTracker中注册过，也就是Stage已经被计算过，从MapOutputTracker中获取计算结果
      val serLocs = mapOutputTracker.getSerializedMapOutputStatuses(shuffleDep.shuffleId)
      val locs = MapOutputTracker.deserializeMapStatuses(serLocs)
      (0 until locs.length).foreach { i => // 更新Shuffle的Shuffle Write路径
        if (locs(i) ne null) {
          // locs(i) will be null if missing
          stage.addOutputLoc(i, locs(i))
        }
      }
    } else { //还没有被注册计算过
      // Kind of ugly: need to register RDDs with the cache and map output tracker here
      // since we can't do it in the RDD constructor because # of partitions is unknown
      logInfo("Registering RDD " + rdd.id + " (" + rdd.getCreationSite + ")")
      mapOutputTracker.registerShuffle(shuffleDep.shuffleId, rdd.partitions.length)  //注册
    }
    stage
  }
```
继续看newShuffleMapStage：
```
private def newShuffleMapStage(
      rdd: RDD[_],
      numTasks: Int,
      shuffleDep: ShuffleDependency[_, _, _],
      firstJobId: Int,
      callSite: CallSite): ShuffleMapStage = {
    val (parentStages: List[Stage], id: Int) = getParentStagesAndId(rdd, firstJobId) //获取parentstages即stageid
    val stage: ShuffleMapStage = new ShuffleMapStage(id, rdd, numTasks, parentStages,
      firstJobId, callSite, shuffleDep) //实例化一个shuffleStage对象

    stageIdToStage(id) = stage //Stage和id关联
    updateJobIdStageIdMaps(firstJobId, stage) //跟新job所有的Stage
    stage
  }
```
怎么和newResultStage极其的相似？是的没错，这里会生成ShuffleStage，getParentStagesAndId里面的实现就是一个递归调用。

由finalRDD往前追溯递归生成Stage，最前面的ShuffleStage先生成，最终生成ResultStage，至此，DAGScheduler对Stage的划分已经完成。
