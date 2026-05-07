一、方案背景
● 在 AI + 数据湖场景下，企业通常需要将多模态数据（文本、图片、音视频）统一入湖，通过 AI 模型生成向量嵌入，然后支持语义检索、以图搜图、RAG 知识库等应用。Apache Paimon 是一种数据湖存储格式，通过 Global Index 框架和可插拔的索引引擎，支持向量索引、BTree 标量索引、Bitmap 索引、全文索引等多种索引类型，使用户能够在湖上使用不同的查询条件高效检索数据。
● Paimon 作为存储框架，在部分场景下查询体验上还有优化的空间。客户的向量查询一般需要通过 Spark / Flink 提交批处理作业，从作业调度、资源申请到执行完成，等待时间通常在分钟级，无法满足在线服务的延迟要求；查询结束后计算资源即释放，索引数据无法常驻内存，每次查询都需要重新加载索引文件，冷启动开销大。另一方面，Elasticsearch 本身拥有强大的分布式检索能力，但在大规模向量索引构建场景下面临瓶颈——向量索引构建是 CPU 密集型任务，在线上 ES 集群中执行会抢占在线查询资源，影响已有搜索业务的稳定性；ES 集群的计算资源是固定的，面对数亿级向量的批量构建需求，无法像大数据集群那样弹性扩展。
● 基于 Paimon 的 SPI 可插拔机制，可以将 ES 的各类型索引引擎直接嵌入数据湖框架，用户无须搬迁数据即可获得 ES 级别的分布式向量检索能力。该方案让 Paimon 负责统一存储和分布式构建调度，ES 负责在线分布式检索和联合查询，两者优势互补——用户无须搬迁数据，即可获得 ES 级别的分布式向量检索和标量联合查询能力。
● ES 提供的能力规划如下：
阶段	能力	说明	状态
Phase 1	DiskBBQ 向量索引	基于ES9.3内嵌的ucene中的DiskBBQ向量算法，实现拉取最近邻向量数据，减少磁盘IO的查询能力。
● 支持向量索引ES侧的查询能力
● 支持paimon java客户端的查询能力
● 支持paimon flink、spark场景的查询能力
● 不支持paimon python、rust客户端的查询能力	✅ 当前版本
Phase 2	Native ES 向量索引	基于 ES Native Lib（Lumina 引擎），提供更高精度的向量检索
● 支持向量索引ES侧的查询能力
● 支持paimon java客户端的查询能力
● 支持paimon flink、spark场景的查询能力
● 支持paimon python、rust客户端的查询能力	🚧 规划中
Phase 3	标量索引联合查询能力	基于 Lucene 的标量索引能力，支持数值范围查询、关键词过滤等
● 在之前基础上，支持向量+其他条件联合查询的能力，联合索引查询由于paimon串行两阶段查询	🚧 规划中

二、整体架构
2.1 端到端架构总览


2.2 Paimon索引构建架构
ES 向量索引通过 Paimon 的 SPI 插件机制接入构建链路。Flink 或 Spark 作为计算引擎负责调度和分片，Paimon 框架从 OSS 读取数据文件并提供统一的写入接口，ES Lib 包接收向量数据后调用底层算法引擎（Lucene DiskBBQ / Native ES）构建索引，最终将索引文件写回 OSS。

构建流程要点：
● Flink / Spark 负责分布式调度，将全表数据按行范围自动分片，每个分片独立并行构建。
● Paimon 框架 负责从 OSS 读取数据文件，通过统一的 SPI 接口将向量数据逐条传入 ES Lib。
● ES Lib 包 是 ES 团队提供的 Java 库，封装了索引构建的完整逻辑。它在本地临时目录中调用底层算法引擎构建索引，构建完成后将索引文件写入 OSS。
● 算法引擎 当前支持 Lucene DiskBBQ（IVF + 二进制量化），后续将支持 Native ES（HNSW / QGraph）。
● 构建完成后，Paimon 将索引文件信息注册到快照中，原子性生效。
2.3 Paimon索引查询架构
查询时，Paimon 框架从快照中定位索引文件，通过 SPI 接口调用 ES Lib 的搜索能力。ES Lib 内部按需从 OSS 加载索引数据，调用算法引擎执行向量近邻搜索，返回行号和分数。框架再根据行号回查数据文件，返回最终结果。

查询流程要点：
● Paimon 框架 负责查询规划（定位索引分片）、标量预过滤（可选）、结果合并和数据回查。
● ES Lib 包 封装了索引文件的加载和搜索逻辑。首次查询时按需从 OSS 加载索引数据并缓存，后续查询直接命中缓存。
● 算法引擎 执行实际的向量近邻搜索：DiskBBQ 通过 IVF 聚类中心匹配 + 二进制量化精排；Native ES 通过 HNSW 图遍历搜索。
● 查询结果为行号集合 + 相似度分数，框架根据行号映射到数据文件，只读取命中的行，避免全表扫描。

2.4 ES 集群导入Paimon索引与查询架构
除了通过 Paimon 框架查询外，ES 索引文件还可以零拷贝导入 ES 集群，利用 ES 的分布式查询能力提供在线服务。

导入要点：
● 利用 OSS Server-Side CopyObject 将 Paimon 索引文件重命名为 ES 段格式，纯元数据操作，毫秒级完成。
● ES 通过 OpenStore（Alluxio）透明读取 OSS 上的索引文件，热数据自动缓存到本地。
● 多 Shard 并行搜索，Coordinator 合并全局 TopK 返回。
三、技术方案优势
3.1 ES 引擎优势给Paimon的能力提升
优势维度	具体说明
标量+向量混合检索	ES 在每个 Shard 内同步执行标量过滤和向量检索，向量搜索可根据标量条件进行剪枝优化，相比 Paimon 原生的两阶段串行方案（先 BTree 过滤 → 再向量搜索），理论上有显著性能收益
多 Shard 并行查询	ES 天然支持分布式多 Shard 并行检索，每个 Shard 独立执行搜索后由 Coordinator 合并全局 TopK，充分利用集群算力
IVF + BBQ 高效量化	DiskBBQ 采用 IVF 聚类 + Binary Quantization 编码，在保持高召回率的同时大幅压缩索引体积，降低内存和磁盘 I/O 开销
成熟的向量引擎维护	ES 团队对向量库内部逻辑维护更成熟，后续的查询性能优化、数据结构优化可以持续迭代更新
零拷贝索引导入	基于 OSS Server-Side CopyObject，Paimon 构建的索引可毫秒级导入 ES 集群，全程零数据传输，不受文件大小影响
OpenStore 透明直读	ES 通过 Alluxio/OpenStore 透明读取 OSS 上的索引文件，自动本地缓存加速，无需额外数据搬迁
3.2 与纯 ES 方案对比
维度	纯 ES 方案	Paimon + ES 联合方案
数据存储	ES 独立存储，数据孤岛	数据湖统一存储，一份数据多引擎共用
数据入湖	需要额外 ETL 同步	Flink 实时流式入湖，秒级延迟
AI 特征生成	需要外部系统计算后回写	PyPaimon Shard Update 原地计算，支持 Ray 分布式推理
索引构建	ES 自行构建	Flink/Spark 分布式并行构建，利用大数据集群算力
跨引擎查询	仅 ES	ES + Lumina + 全文 + 标量，灵活组合
存储成本	ES 集群存储（高）	OSS 冷热分离（低），ES 按需缓存
弹性扩展	依赖 ES 集群扩容	计算（Flink/Spark）和存储（OSS）独立弹性

四.索引对比说明
4.1 ES 索引引擎对比
维度	ES DiskBBQ (✅ 当前)	ES Native / Lumina (🚧 规划)	ES Lucene 标量 (🚧 规划)	Paimon Lumina DiskANN
算法	IVF + Binary Quantization	HNSW / QGraph (Havenask Native)	Lucene BKD-Tree / 倒排索引	Vamana Graph (DiskANN)
SPI 标识	es-vector-diskbbq	es-vector-native	es-scalar-lucene	lumina-vector-ann
索引结构	IVF 倒排列表 + BBQ 编码	多层导航图 + 量化	BKD-Tree / 倒排表	单层 Vamana 图 + 磁盘布局
数据类型	向量	向量	标量（数值/字符串）	向量
量化支持	Binary Quantization（内置）	int8 / PQ（可配）	—	rawf32 / PQ / SQ8
距离度量	L2 / Cosine / IP	L2 / Cosine / IP	—	L2 / Cosine / IP
联合查询	🚧 后续与 ES Lucene 标量联合	🚧 后续与 ES Lucene 标量联合	🚧 提供标量过滤能力	两阶段串行（BTree 预过滤）
分布式查询	原生多 Shard 并行	原生多 Shard 并行	原生多 Shard 并行	单机（依赖上层框架分片）
ES 集群导入	OSS CopyObject 零拷贝	OSS CopyObject 零拷贝	OSS CopyObject 零拷贝	需要格式转换
适用场景	大规模分布式检索	高精度 ANN + 分布式	标量过滤、范围查询	单机高精度、低延迟 ANN
4.2 ES 向量+标量联合查询架构（规划）


五、ES 能力演进路线


六、总结
Elasticsearch × Paimon 向量索引方案实现了 "数据湖统一存储 + ES 分布式向量检索" 的深度融合：
1. 对 Paimon 用户：无需搬迁数据即可获得 ES 级别的分布式向量检索能力
2. 对 ES 用户：扩展了 ES 的数据源，可直接查询 Paimon 数据湖中的向量数据
3. 对平台方：一份数据、多种索引、多引擎查询，大幅降低存储和运维成本
   当前能力：ES DiskBBQ 向量索引构建和查询 + OSS 零拷贝导入 ES 集群
   规划能力：Native ES 向量索引（HNSW/QGraph） → Lucene 标量索引 → 向量+标量联合查询（核心差异化优势）
   关键技术亮点：SPI 可插拔引擎 → Flink/Spark 分布式构建 → OSS 零拷贝导入 ES → OpenStore 透明直读 → 多 Shard 并行混合检索 → 向量+标量同步剪枝（规划）