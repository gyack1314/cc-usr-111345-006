# 业务资料约定

cost为整数分，time为整数时刻；行id是物理稳定序键。windows.start/end为相对当前行或peer的位置偏移，负值表示PRECEDING，0表示CURRENT，正值表示FOLLOWING；其实际值域方向需按order解释。

每行id在该表内唯一，文件顺序不代表业务事件顺序。清单schema=1，各表登记文件名、字节长度、SHA256、行数及完整字段集合，未知版本或内容变化应重新登记。业务身份、金额、版本及测量数值均为本地资料，不代表任何现实机构的经营数据。

- rows：id、tenant、time、cost。
- windows：id、partition、order、unit、start、end、exclude。

normal与boundary是相互独立的资料包，不能直接拼接重复id。引用字段按本项目domain.py的REFERENCES登记。读取检查只检查完整性、身份及这些直接引用，不替代仓库查询语义、快照可见性或递归完备性判断。

# 窗口执行语义

窗口算子（window_data.executor）按以下约定求值，与慢解释器（window_data.reference）逐行核对：

- 分区：partition列值完全相等的行同区，null与null相等；分区列按asc、nulls first参与排序。
- 排序：order各列按各自direction与独立nulls位置比较；业务键相同（null视同相等）的行互为peer；物理行顺序只以行id字典序打破并列，不拆散peer。
- 帧：start/end为相对偏移（负=PRECEDING，0=CURRENT，正=FOLLOWING），start不大于end。
  - ROWS：偏移按物理行计，CURRENT ROW仅当前行。
  - GROUPS：偏移按完整peer组计，CURRENT ROW指当前行所在的整个peer组。
  - RANGE：仅单个整数排序键（null除外，布尔与非整数拒绝），偏移为非负整数的绝对值、符号定方向；升序时值域为[cur+start, cur+end]，降序时为[cur-end, cur-start]，边界含端点；null排序键不做加减，只与null行成帧（整个null peer区间），非null行的帧不含null行。
- 排除：EXCLUDE CURRENT ROW只去当前行；EXCLUDE GROUP去掉当前行整个peer组；EXCLUDE TIES去掉其他peer但保留当前行；均在帧内生效，与帧单位无关。
- 聚合：COUNT计非null值，空帧为0；SUM/AVG/MIN/MAX在无非null值时为null（空帧或全null帧都不变成0）；SUM/AVG只接受整数值。
- 排名：row_number为物理行位次（从1计）；rank为1+前方行数（并列留空位）；dense_rank为1+前方peer组数；均与聚合帧无关。
- lead/lag：按物理行偏移取目标行的值，保留null、不跳过缺测行，越界返回函数定义中明确给出的default。
- 输出：结果按行id字典序回到原行id；输入分片顺序变化不改变结果。
