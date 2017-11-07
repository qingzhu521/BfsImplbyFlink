这个文档包括以下用flink实现bfs的版本<br>
bfs需要数据是点，这个点所有的边，这个点现在所在的层级。

1. 使用gelly vertex center实现的bfs <br>
	代码文件-----BFS.java

2. 使用gelly GAS api 实现的bfs <br>
	代码文件-----GASBfs.java

#####################################################

以下是基本api实现的bfs
我们总是需要规定以下几个数据集合
点集合，每个点关联的属性，边集合，我们有以下几种存储方式。对数据进行整理

3. 使用flink基本api模拟gelly vertex center实现(**特点是点和边分开存储，边以边表的形式存储**）的bfs<br>
代码文件-----T2EdgeSetCoGroup.java<br>
数据存储：<br>
DataSet<Tuple2<Integer, Integer>> edges<br>
DataSet<Tuple2<Integer, Integer>> vertexWithLevel 存储点和这点所在level<br>
主要过程：<br>
workset最初是（0,1）起点为0，起点的level是1。(line 89)<br>
在iteration中的操作是 workset中每个点u去找这点所有的边v（通过cogroup寻找）。生成 (v, (level of u) + 1)这样的message集合。(line 93-113)<br>
message集合和原始solution集合cogroup知道哪些数据需要更改生成delta集合（line 115-139）<br>
iteration close（line 141）去更新原始的solution set。<br>
主要过程 两个cogroup。line 93 - 113 和line 115 - 139。 针对flink cogroup的开销见文档
https://docs.google.com/document/d/1pNr7rLWDjmv2cZlkd1kjwbWZtQZoJAd9UgsmMp-b1R0/edit。 flink主要在序列化上花费太多时间。

4. 使用flink基本api实现的bfs（**特点是一个点和这个点所有的边存储在一起，点和属性另外存储**）<br>
代码文件-----T2AdjSetBfs.java<br>
数据存储：<br>
DataSet<Tuple2<Integer, Integer[]>> verteices 这个点和这点所有的邻接点
DataSet<Tuple2<Integer, Integer>> vertexWithLevel 存储点和这点所在level<br>
主要过程：<br>
workset最初是（0,1）起点为0，起点的level是1。(line 53)<br>
在iteration中的操作是 workset中每个点u去找这点所有的边v（通过join寻找）。生成 (v, (level of u) + 1)这样的message集合。（line 58-68）和3中不同的地方时在上述3.中边不一定存储在一起，而这里边一定存储在一起。<br>
message集合和原始solution集合cogroup知道哪些数据需要更改生成delta集合（line 74-85）<br>
iteration close（line 88）更新原始solution set

5. 使用flink基本api实现的bfs（**特点是一个点和这点所有的边以及额外的属性都放在一条数据中存储**）<br>
代码文件-----Tuple3BfsCoGraph.java<br>
数据存储：<br>
DataSet<Tuple3<Integer, Integer[], Integer>> verteices 分别表示点，这点所有的边，这个点当前的属性<br>
主要过程：<br>
workset是通过filter算子过滤出来的，起点是0，起点level是1。（line 61-74）<br>
在iteration中的操作是 workset直接由点u flatmap生成（v,（level of u) + 1) 这样的message的集合（line 78-90）<br>
delta是通过 message和solutionset cogroup得出（line 91-112）尤其值得注意的是这里使用withForwardFieldSecond。让数据可以不经过解序列化进入下一阶段这一步非常提速。<br>
iteration close（line 114）更新原始solution set<br>

<a href="https://docs.google.com/spreadsheets/d/1xDOspfTyHqvdbwztA1B-tFgdCL-2_DqzVgbm1rR3s5U/edit?usp=sharing" title="Title">比较文件</a>在这里
