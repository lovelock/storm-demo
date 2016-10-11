# Storm应用演示

一个简单的关于Apache Storm的使用的分享，其中涉及到了以下话题：

1. Storm的特点
    大规模、分布式、流处理系统
    
2. Storm和Hadoop的差异
    前者是流处理，后者是批处理
    
3. 安装JDK/Maven/Storm, 参考[在本地单机部署Hadoop/Storm运行环境](http://unixera.com/java/deploy-pseudo-distributed-mode-hadoop/)

4. 关键概念介绍

    - Topology
        完成一项作业的业务逻辑的集合
    - Spout
        数据流的输入端
    - Bolt
        输入端之后处理数据的各个逻辑
    - Tuple
        数据在Storm中流动所使用的格式
    - 两种运行模式
    
        - 本地模式（个人感觉不太实用）
        - 生产环境

5. 演示一个实际的Storm应用

    - LogStash从前端机拉取日志 （运维负责，略）
    - LogStash把日志推到Kafka（运维负责，略）
    - 使用KafkaSpout把数据输入到Storm
    - 编写多个Bolt处理Tuple，并将其发射到下一个Bolt
    - 将处理完成的数据写入到HDFS中
    - 当前版本（0.10.0）的Storm无法使用分块，将新版本（2.0.0-SNAPSHOT）支持的功能backport到当前版本
    
6. 应用参数调优

7. FAQ
