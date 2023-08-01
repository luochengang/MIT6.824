# MIT6.824
## 已完成

1) 2023-7-29 20:20 MIT6.824 Lab2(raft)和Lab3(kvraft)的源码阅读

1) Pre-Vote
raft博士学位论文 4.2.3 Disruptive servers
raft博士学位论文 9.6 Preventing disruptions when a server rejoins the cluster
2) lease mechanism for read-only queries(Lease Read)
raft博士学位论文 6.4 Processing read-only queries more efﬁciently
https://blog.csdn.net/weixin_43705457/article/details/120572291
https://cn.pingcap.com/blog/lease-read
3) ReadIndex
raft博士学位论文 6.4 Processing read-only queries more efﬁciently
https://cn.pingcap.com/blog/lease-read ReadIndex Read
4)Raft 的优化
Batch特性是值得做的，其它的不值得。
Batch: leader可以积累了多个requests后，再batch发给follower
https://cn.pingcap.com/blog/optimizing-raft-in-tikv

## 未完成

1) membership change
2) leader transfer

https://github.com/baidu/braft/blob/master/docs/cn/raft_protocol.md

https://github.com/baidu/braft

## 其它

### 测试linux命令

nohup go test > /root/script/out10 &

### snapshot流动方向

![](C:\Users\luoch\software\github\MIT6.824\snapshot流动方向.jpg)