# 在Spark应用中实现速率控制
## 背景
首先, 我们必须回答这样一个问题, 即为什么我们需要在Spark应用中的速率控制? 众所周知, Spark是用来处理海量数据的引擎. 我们使用Spark的目的是希望快速处理大量数据, 对Spark应用进行速率的限制是不是一种本末倒置的行为?

理论上来说, 诚然如此. 但在实际业务需求中, 我们经常遇到这样的情形: 我们从HIVE/HDFS等来源读取到的数据的属性是不完整的, 必须从一些其它接口获取数据的额外属性. 比如一些日志文件记录, 原始数据中仅仅记录了它在TOS上的地址. 又比如一些数据的属性, 必须从算法开发的接口中根据数据的原始属性计算得到. 这些外在的数据来源所能支持的速率并非无限. 我们必须在请求端有所限制, 否则会引起数据错误率提高, 服务响应减慢, 服务宕机等一系列问题.

为了解决这样的问题, 通过公司内部提供的工具链, 有一些比较简单直观的方法. 比如将数据传到mysql后用其他语言和框架实现流控. 或者通过faas作为中间处理节点以kafka/bmq串接服务. 以请求接口补充某题在题库中是否重复的数据信息为例:
- 通过Hive2Kafka/Bmq程序将原始数据输入Bmq Topic A
- 通过Faas消息队列消费者消费原始数据, 请求接口, 将数据结果输出到Bmq Topic B
- 通过Kafka2Hive程序将结果数据dump到Hive上, 进行分析处理

我们目前有相当多的流程使用的是这样的方法(比如页搜回扫服务). 可以看到这样的方法流程链路是相当长的. 涉及到三端两个Bmq Topic(如果是mysql, 也需要起码两个dump任务和表以及相应的各种建表审批流程). 如果每个接口都要这样对接一遍, 实在是令人头大. 而且, 使用faas的方法虽然方便一些(不用自己实现流控, 消费逻辑等等), 但它将批处理的逻辑变成了流处理的逻辑. 从结果上来看, Kafka/bmq2Hive程序dump回数据到hive上时, 我们是不知道数据何时处理完毕的. 而且Kafka/bmq2Hive程序dump数据的最小时间分区是小时, 这意味着即使我们通过一些手段能校验一批数据的完整性, 我们也至少需要T+1H的响应时间来处理数据. 当然, 我们可以通过Flink/Spark Structured Streaming把我们整个处理逻辑转换成流的, 但是我们的数据本来就是批的, 能不能不要改动数据的本身的批的属性呢?

## 原理与方法
### Spark并发颗粒度原理
要控制Spark处理数据的速率, 我们首先必须对Spark处理数据的并行度原理有所理解. 提交一个Spark Job时, Spark根据Rdd之间的宽窄依赖关系将一个Job划分为几个Stage, 又通过Rdd的partition将Stage分解成多个Task, 分配到executor slot上. 通俗地理解, executor core 个数 * executor instance 个数, 即为Task的并行数量. 这个值可以通过spark.sparkContext.defaultParallelism取到. 那么我们要增加或减少并发度, 就可以通过调整executor总共使用的core数量和Rdd的partition数量来达成目的.

但是, 考虑到我们在这里使用Spark进行的本质上是IO处理, 申请相当于并发量的核数是否显得有些浪费呢? 事实上, 我们可以通过调用forEachPartition方法, 一次处理整个数据Partition. 这样, 就给我们通过线程池, 使用单个executor core批量处理多条记录提供了条件.
### 并发实现
```scala
import java.util.concurrent.Executors

import org.apache.spark.sql.{Dataset, Encoder}

import scala.concurrent.duration.Duration.Inf
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

object ThreadedConcurrentContext {
  implicit lazy val ec: ExecutionContextExecutorService = {
    ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(1000))
  }

  implicit class RateLimitedConcurrentDataset[A](ds: Dataset[A]) {
    def concurrentMap[B: Encoder](func: A => B): Dataset[B] = {
      ds.mapPartitions(it => {
        val futures = it.map(r => Future {func(r)})
        Await.result(Future.sequence(futures), Inf)
      })
    }
  }
}
```
上面的代码建立了一个1000个线程的线程池, 然后在dataset的mapPartitions方法里调用Future, 并发地使用线程池处理一个Partition里的所有数据. 注意这里的ExecutionContextExecutorService前面的Lazy修饰, 通过object + lazy val可以保证这个线程池在executor端的维持单例. ([这里](http://www.russellspitzer.com/2017/02/27/Concurrency-In-Spark/)有一个awaitSliding改进版本, 每次只await一部分future, 节省内存资源)

OK, 目前我们已经实现了并发, 那么如何实现速率的限制呢? 一个简单的想法是, 通过失败时sleep+retry, 控制完成单个请求所需要的最长时间, 以一种比较粗糙的自适应的方式打满目标的qps上限.
### Retry实现
```scala
import scala.util._

def retry[T](n: Int, after: Int)(fn: => T): Try[T] = {
  Try {
    fn
  } match {
    case Failure(_) if n > 1 =>
      Thread.sleep(after)
      retry(n - 1, after)(fn)
    case fn => fn
  }
}
```
这种方法的弊端是会打满接口的负载. 有时这个接口并不是专为了我们的应用而设计, 又或者是这个接口在负载高时会出现各种各样的问题, 这个方法是不能使用的.

最靠谱的方法还是直接限制我们请求的总QPS.
### QPS Limit实现
参考Faas的限流实现, 将每个consumer分配一个速率为QPS/n的令牌桶. 我们为每个Executor分配一个速率为QPS/Executor个数的令牌桶.
```scala
import com.google.common.util.concurrent

import scala.concurrent.{ExecutionContext, Future}

object RateLimiter {
  def limit[A, B](rate: Double)(fn: A => B)(implicit executionContext: ExecutionContext): A => Future[B] = {
    val limiter = {
      println(s"Limiter of rate $rate created on executors")
      concurrent.RateLimiter.create(rate)
    }
    a: A =>
      Future {
        limiter.acquire()
        fn(a)
      }
  }
}
```
这里直接使用了Google guava包里实现的令牌桶.
```scala
import java.util.concurrent.Executors

import RateLimiter.limit
import org.apache.spark.sql.{Dataset, Encoder}

import scala.concurrent.duration.Duration.Inf
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}

object RateLimitedConcurrentContext {
  implicit lazy val ec: ExecutionContextExecutorService = {
    println("Thread pool created on executors")
    ExecutionContext.fromExecutorService(Executors.newWorkStealingPool(1000))
  }

  implicit class RateLimitedConcurrentDataset[A](ds: Dataset[A]) {
    def getExecutorNumber: Int = ds.sparkSession.sparkContext.statusTracker.getExecutorInfos.length - 1

    def rateLimitedConcurrentMap[B: Encoder](func: A => B, qpsLimit: Int, repartition: Boolean = true): Dataset[B] = {
      val parallelism = ds.sparkSession.sparkContext.defaultParallelism
      val qpsLimitPerExecutor = qpsLimit.toFloat / getExecutorNumber
      val repartitionedDs = if (repartition) {
        val targetPartitionNumber = parallelism
        ds.repartition(targetPartitionNumber)
      } else ds
      case object rateLimitedFunc {
        lazy val function: A => Future[B] = limit(qpsLimitPerExecutor)(func)
      }
      val broadcastFunc = ds.sparkSession.sparkContext.broadcast(rateLimitedFunc)
      repartitionedDs.mapPartitions(it => {
        val limitedFunc = broadcastFunc.value.function
        val futures = it.map(limitedFunc)
        Await.result(Future.sequence(futures), Inf)
      })
    }
  }

}
```
使用时
```scala
import RateLimitedSparkContext.rateLimitedConcurrentDataset

df.rateLimitedConcurrentMap(func, qps=1000)
```
这里的实现有几个tricky的地方:
- 线程池ec和令牌桶limter, 需要在Executor端实现单例. 对scala来说, 单例模式可以简单地用object实现, 但如何使得ec和limiter在Executor上初始化呢? (这两者是不能在Driver端初始化, 然后通过serilization传输到Executor端的, 就如同serialize一个数据库连接一样, 这样做毫无意义). 可以看到ec作为全局object, 可以简单的在object中使用lazy修饰达到这一目的. 而limiter的初始化更加复杂一些, 因为它关系到我们规定的qps参数, 是不能在全局范围初始化, 而只能在函数里初始化的. 这里把它包装在case object中通过broadcast广播出去, 然后调用它的lazy val member去达到初始化limiter的目的.
- 由于我们的数据是完整的批而不是kafka topic上的流, 因此一开始最好就要把数据合理地repartition, 避免由于数据倾斜造成个别executor上task执行缓慢. 代码里直接取了sparkContext.defaultParallelism作为partition数量. 注意, 如果你开启了动态资源分配(spark.dynamicAllocation.enabled值为true, 我们的dorado环境里这个值默认为true). 同时partition数量又大于defaultParallelism, 那么你的executor会被动态分配到一个高于你的初始executor数量的数目上, 这意味着我们计算的qps per executor失效了, 整体qps会高于预期. 建议关闭动态资源分配, 或者维持你的partition数量小于等于defaultParallelism.