<div align="center">

# <img src="images/logo.svg" alt="OceanBase Logo" width="40%" />

### **🔷 面向 AI 应用的混合搜索数据库**

**强大的 AI 搜索能力 · 轻量级 · 生产级别**

</div>

---

<p align="center">
    <a href="https://oceanbase.ai">
        <img alt="Documentation" height="20" src="https://img.shields.io/badge/OceanBase.ai-4285F4?style=for-the-badge&logo=read-the-docs&logoColor=white" />
    </a>
    <a href="https://space.bilibili.com/3546900567427713">
        <img height="20"  alt="Static Badge" src="https://img.shields.io/badge/Bilibili-00A1D6?logo=bilibili">
    </a>
    <a href="https://ask.oceanbase.com/"  >
        <img height="20" src="https://img.shields.io/badge/Forum-中文社区-FF6900?style=for-the-badge" alt="Forum" />
    </a>
    <a href="https://h5.dingtalk.com/circle/joinCircle.html?corpId=ding320493024256007024f2f5cc6abecb85&token=be84625101d2c2b2b675e1835e5b7988&groupCode=v1,k1,EoWBexMbnAnivFZPFszVivlsxkpAYNcvXRdF071nRRY=&from=group&ext=%7B%22channel%22%3A%22QR_GROUP_NORMAL%22%2C%22extension%22%3A%7B%22groupCode%22%3A%22v1%2Ck1%2CEoWBexMbnAnivFZPFszVivlsxkpAYNcvXRdF071nRRY%3D%22%2C%22groupFrom%22%3A%22group%22%7D%2C%22inviteId%22%3A1057855%2C%22orgId%22%3A313467091%2C%22shareType%22%3A%22GROUP%22%7D&origin=11?#/">
        <img height="20"  src="https://img.shields.io/badge/钉钉群-33254054-0084FF?style=for-the-badge&logo=dingtalk&logoColor=white" alt="钉钉群 33254054" />
    </a>
    <a href="https://pepy.tech/projects/seekdb">
        <img height="20" alt="Downloads" src="https://static.pepy.tech/badge/seekdb" />
    </a>
    <a href="https://github.com/oceanbase/seekdb/blob/master/LICENSE">
        <img height="20" alt="License" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" />
    </a>
</p>

<div align="center">

[English](README.md) | **中文版**

---

</div>

## 🚀 什么是 OceanBase SeekDB？

**OceanBase SeekDB** 是 一个专为 AI 应用设计的强大混合搜索数据库。它结合了企业级数据库能力与前沿的 **AI 搜索** 功能，支持向量, 全文索引, Json, 是构建下一代 AI 应用的完美选择。

---

## 🔥 为什么选择 OceanBase SeekDB？

---
| 特性 | OceanBase SeekDB | 传统数据库 | 纯向量数据库 | 全文搜索引擎 |
|---------|----------------|----------------|----------------|------------------|
| **嵌入式模式** | ✅ 原生支持 | ⚠️ 可能 | ⚠️ 可能 | ⚠️ 可能 |
| **SQL 支持** | ✅ 完整 SQL | ✅ 完整 SQL | ❌ 有限 | ❌  有限 |
| **向量搜索** | ✅ 内置 | ❌ 有限 | ✅ 支持 | ❌ 有限 |
| **全文搜索** | ✅ 内置 | ✅ 支持 | ❌ 有限 | ✅ 高级 |
| **Json** | ✅ 是 | ⚠️ 因产品而异 | ❌ 不支持 | ❌ 不支持 |
| **ACID 事务** | ✅ 完整支持 | ✅ 完整支持 | ❌ 有限  | ❌ 有限 |
| **易于迁移** | ✅ MySQL 兼容 | ✅ 标准 | ❌ 不支持 | ❌ 不支持 |


---

## ✨ 核心特性

### 🎯 **AI 驱动的搜索**
- **混合搜索**：将向量搜索, 标量搜索, 全文检索 混合查询，获得更佳效果
- **向量相似度搜索**：针对不同的场景, 使用不同的算法, 获得更佳的向量查询准确率, 性能和成本. 
- **全文搜索**：内置全文索引，支持基于关键词的搜索
- **Json 支持**：支持Json 查询, 支持Json 多值索引.

### 📦 **嵌入式与轻量级**
- **零依赖**：嵌入到您的应用程序中运行——无需单独的数据库服务器
- **小巧占用**：最小内存和磁盘使用，非常适合边缘设备和容器
- **单一二进制文件**：易于部署和与应用程序一起分发
- **本地优先**：离线工作，准备好时同步

### ⚡ **简单且开发者友好**
- **MySQL 兼容**：使用熟悉的 SQL 语法——无需学习曲线
- **即时设置**：几秒钟内即可开始，无需分钟级
- **丰富的 API**：支持 Python、Java、Go 等
- **全面的文档**：清晰的文档，每个用例都有示例

### 🚀 **生产就绪**
- **稳定性**：历经15年+的技术沉淀, 2000+ 企业落地实践
- **ACID 合规**：完整的事务支持，具有强一致性保证
- **水平可扩展**：从单节点无缝扩展到分布式集群
- **企业级安全**：内置加密、身份验证和访问控制

---

## 🎬 快速开始

### 安装

选择您的平台：

<details>
<summary><b>🐍 Python（推荐用于 AI/ML）</b></summary>
```bash
pip install pylibseekdb
```
</details>

<details>
<summary><b>🐳 Docker（快速测试）</b></summary>

```bash
docker run -d \
  --name seekdb \
  -p 2881:2881 \
  -v ./data:/var/lib/oceanbase/store \
  oceanbase/seekdb:latest
```

</details>

<details>
<summary><b>📦 二进制文件（独立安装）</b></summary>

```bash
# Linux
rpm -ivh seekdb-1.x.x.x-xxxxxxx.el8.x86_64.rpm
```
请将版本号替换为实际的 RPM 包版本。

</details>

<details>
<summary><b>☁️ 云平台</b></summary>

快速体验 OceanBase Cloud——一个高度可扩展的分布式数据库，适用于事务、分析和 AI 工作负载。[立即开始](https://console-cn.oceanbase.com/)

</details>

### 🎯 AI 搜索示例

在 5 分钟内构建语义搜索系统：

<details>
<summary><b>🗄️ 🐍 新版 Python SDK</b></summary>

```python
import pyseekdb
client = pyseekdb.Client()

collection = client.get_or_create_collection(name="my_collection")

collection.upsert(
    ids=["item1", "item2", "item3"],
    documents=[
        "It's rainy today",
        "It was cloudy yesterday",
        "The forecast for tomorrow is fine weather"
   ]
)

results = collection.query(
    query_texts=[" What's the weather like today"], # SeekDB will embed this for you
    n_results=2 # how many results to return
)

print(results)
```
更多详情请参考[用户指南](docs/user-guide/zh/pyseekdb-sdk.md)。
</details>
<details>
<summary><b>🗄️ 🐍 旧版 Python SDK</b></summary>

```python
from pyobvector import *

# Don't support Embedded mode
client = MilvusLikeClient(uri="127.0.0.1:2881", user="test@test")

test_collection_name = "ann_test"
# define the schema of collection with optional partitions
range_part = ObRangePartition(False, range_part_infos = [
    RangeListPartInfo('p0', 100),
    RangeListPartInfo('p1', 'maxvalue'),
], range_expr='id')
schema = client.create_schema(partitions=range_part)
# define field schema of collection
schema.add_field(field_name="id", datatype=DataType.INT64, is_primary=True)
schema.add_field(field_name="embedding", datatype=DataType.FLOAT_VECTOR, dim=3)
schema.add_field(field_name="meta", datatype=DataType.JSON, nullable=True)
# define index parameters
idx_params = client.prepare_index_params()
idx_params.add_index(
    field_name='embedding',
    index_type=VecIndexType.HNSW,
    index_name='vidx',
    metric_type="L2",
    params={"M": 16, "efConstruction": 256},
)
# create collection
client.create_collection(
    collection_name=test_collection_name,
    schema=schema,
    index_params=idx_params,
)

# insert value
# prepare data
vector_value1 = [0.748479, 0.276979, 0.555195]
vector_value2 = [0, 0, 0]
data1 = [{'id': i, 'embedding': vector_value1} for i in range(10)]
data1.extend([{'id': i, 'embedding': vector_value2} for i in range(10, 13)])
data1.extend([{'id': i, 'embedding': vector_value2} for i in range(111, 113)])
# insert data
client.insert(collection_name=test_collection_name, data=data1)

# do search
res = client.search(collection_name=test_collection_name, data=[0, 0, 0], anns_field='embedding', limit=5, output_fields=['id'])
# For example, the result will be:
# [{'id': 112}, {'id': 111}, {'id': 10}, {'id': 11}, {'id': 12}]

```
更多详情请参考[用户指南](https://github.com/oceanbase/pyobvector)。
</details>

<details>
<summary><b>🗄️ SQL</b></summary>

```python
import pylibseekdb as seekdb

# Open a database
seekdb.open()

# Connect to a database
conn = seekdb.connect()

# Use the connection
cursor = conn.cursor()
cursor.execute("""-- Create table with vector column
CREATE TABLE articles (
    id INT PRIMARY KEY,
    title TEXT,
    content TEXT,
    embedding VECTOR(384)
);""")

cursor.execute("""-- Create vector index for fast similarity search
CREATE INDEX idx_vector ON articles USING VECTOR (embedding);""")

cursor.execute("""-- Insert documents with embeddings
-- Note: Embeddings should be pre-computed using your embedding model
INSERT INTO articles (id, title, content, embedding) 
VALUES 
    (1, 'AI and Machine Learning', 'Artificial intelligence is transforming...', '[0.1, 0.2, ...]'),
    (2, 'Database Systems', 'Modern databases provide high performance...', '[0.3, 0.4, ...]'),
    (3, 'Vector Search', 'Vector databases enable semantic search...', '[0.5, 0.6, ...]');""")

cursor.execute("""-- Example: Hybrid search combining vector and full-text
-- Replace '[query_embedding]' with your actual query embedding vector
SELECT 
    title,
    content,
    embedding <-> '[query_embedding]' AS vector_distance,
    MATCH(content) AGAINST('your keywords' IN NATURAL LANGUAGE MODE) AS text_score
FROM articles
WHERE MATCH(content) AGAINST('your keywords' IN NATURAL LANGUAGE MODE)
ORDER BY vector_distance ASC, text_score DESC
LIMIT 10;""")

results = cursor.fetchall()

# Close the connection
conn.close()
```
</details>


## 📚 使用场景

<div align="center">

| 🎯 **RAG 系统** | 🔍 **语义搜索** | 💬 **聊天机器人** | 🎬 **推荐系统** |
|:---:|:---:|:---:|:---:|
| 使用向量搜索构建检索增强生成管道 | 跨文档、图像和多媒体进行语义搜索 | 创建具有记忆和上下文的智能聊天机器人 | 使用混合搜索构建推荐引擎 |

</div>

### 🎯 真实世界示例

- **📚 文档问答**：使用 RAG 构建类似 ChatGPT 的文档搜索
- **🖼️ 图像搜索**：使用视觉嵌入查找相似图像
- **💼 电商**：语义产品搜索和推荐
- **🔬 科学研究**：搜索研究论文和数据集
- **📊 商业智能**：将 SQL 分析与 AI 搜索相结合

---

## 🌟 生态系统与集成

<div align="center">

<p>
    <a href="https://huggingface.co">
        <img src="https://img.shields.io/badge/HuggingFace-✅-00A67E?style=flat-square&logo=huggingface" alt="HuggingFace" />
    </a>
    <a href="https://github.com/langchain-ai/langchain/pulls?q=is%3Apr+is%3Aclosed+oceanbase">
        <img src="https://img.shields.io/badge/LangChain-✅-00A67E?style=flat-square&logo=langchain" alt="LangChain" />
    </a>
    <a href="https://github.com/langchain-ai/langchain/pulls?q=is%3Apr+is%3Aclosed+oceanbase">
        <img src="https://img.shields.io/badge/LangGraph-✅-00A67E?style=flat-square&logo=langgrap" alt="LangGraph" />
    </a>
    <a href="https://github.com/langgenius/dify/pulls?q=is%3Apr+is%3Aclosed+oceanbase">
        <img src="https://img.shields.io/badge/Dify-✅-00A67E?style=flat-square&logo=dify" alt="Dify" />
    </a>
    <a href="https://github.com/coze-dev/coze-studio/pulls?q=is%3Apr+oceanbase+is%3Aclosed">
        <img src="https://img.shields.io/badge/Coze-✅-00A67E?style=flat-square&logo=coze" alt="Coze" />
    </a>
    <a href="https://github.com/run-llama/llama_index/pulls?q=is%3Apr+is%3Aclosed+oceanbase">
        <img src="https://img.shields.io/badge/LlamaIndex-✅-00A67E?style=flat-square&logo=llama" alt="LlamaIndex" />
    </a>
    <a href="https://firecrawl.dev">
        <img src="https://img.shields.io/badge/Firecrawl-✅-00A67E?style=flat-square&logo=firecrawl" alt="Firecrawl" />
    </a>
    <a href="https://github.com/labring/FastGPT/pulls?q=is%3Apr+oceanbase+is%3Aclosed">
        <img src="https://img.shields.io/badge/FastGPT-✅-00A67E?style=flat-square&logo=FastGPT" alt="FastGPT" />
    </a>
    <a href="https://db-gpt.io">
        <img src="https://img.shields.io/badge/DB--GPT-✅-00A67E?style=flat-square&logo=db-gpt" alt="DB-GPT" />
    </a>
    <a href="https://github.com/camel-ai/camel/pulls?q=is%3Apr+oceanbase+is%3Aclosed">
        <img src="https://img.shields.io/badge/camel-✅-00A67E?style=flat-square&logo=camel" alt="Camel-AI" />
    </a>
    <a href="https://github.com/alibaba/spring-ai-alibaba">
        <img src="https://img.shields.io/badge/spring--ai--alibaba-✅-00A67E?style=flat-square&logo=spring" alt="spring-ai-alibaba" />
    </a>
    <a href="https://developers.cloudflare.com/workers-ai">
        <img src="https://img.shields.io/badge/Cloudflare%20Workers%20AI-✅-00A67E?style=flat-square&logo=cloudflare" alt="Cloudflare Workers AI" />
    </a>
    <a href="https://jina.ai">
        <img src="https://img.shields.io/badge/Jina%20AI-✅-00A67E?style=flat-square&logo=jina" alt="Jina AI" />
    </a>
    <a href="https://ragas.io">
        <img src="https://img.shields.io/badge/Ragas-✅-00A67E?style=flat-square&logo=ragas" alt="Ragas" />
    </a>
    <a href="https://jxnl.github.io/instructor">
        <img src="https://img.shields.io/badge/Instructor-✅-00A67E?style=flat-square&logo=instructor" alt="Instructor" />
    </a>
    <a href="https://baseten.co">
        <img src="https://img.shields.io/badge/Baseten-✅-00A67E?style=flat-square&logo=baseten" alt="Baseten" />
    </a>
</p>

<p>
更多详情请参考[用户指南](docs/user-guide/README.md)。
</p>


</div>

---


## 🤝 社区与支持

<div align="center">

<p>
    <a href="https://h5.dingtalk.com/circle/joinCircle.html?corpId=ding320493024256007024f2f5cc6abecb85&token=be84625101d2c2b2b675e1835e5b7988&groupCode=v1,k1,EoWBexMbnAnivFZPFszVivlsxkpAYNcvXRdF071nRRY=&from=group&ext=%7B%22channel%22%3A%22QR_GROUP_NORMAL%22%2C%22extension%22%3A%7B%22groupCode%22%3A%22v1%2Ck1%2CEoWBexMbnAnivFZPFszVivlsxkpAYNcvXRdF071nRRY%3D%22%2C%22groupFrom%22%3A%22group%22%7D%2C%22inviteId%22%3A1057855%2C%22orgId%22%3A313467091%2C%22shareType%22%3A%22GROUP%22%7D&origin=11?#/">
        <img src="https://img.shields.io/badge/钉钉群-33254054-0084FF?style=for-the-badge&logo=dingtalk&logoColor=white" alt="钉钉群 33254054" />
    </a>
    <a href="https://ask.oceanbase.com/">
        <img src="https://img.shields.io/badge/社区-问答论坛-FF6900?style=for-the-badge" alt="Forum" />
    </a>
</p>

</div>

---

## 🛠️ 开发

### 从源码构建

```bash
# Clone the repository
git clone https://github.com/oceanbase/seekdb.git
cd seekdb
bash build.sh debug --init --make
./debug/observer
```

详细说明请参见[开发者指南](docs/developer-guide/zh/README.md)。

### 贡献

我们欢迎贡献！请查看我们的[贡献指南](CONTRIBUTING.md)开始。

---


## 📄 许可证

OceanBase SeekDB 采用 [Apache License, Version 2.0](LICENSE) 许可证。


