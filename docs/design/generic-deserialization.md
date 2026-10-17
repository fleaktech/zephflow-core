# 通用反序列化框架设计（Generic Decoding Framework）

- 状态：Draft v10。只做翻译机本身（第三轮审查流程第 2 轮之后的修订版）。
- 范围：只管反序列化（bytes → records），不管序列化。
- 代码基线：`main` @ `7c9673a`。
- 新代码放在 `lib` 模块的 `io.fleak.zephflow.lib.serdes.decode` 包。不改任何现有类和 source command，也不改别的仓库。

## 已定的用户决策

| # | 决策 |
|---|------|
| T1 | **这次只做翻译机**：做成独立的库，有自己的接口和配置格式。接入各个来源以后再做。 |
| T2 | **文本只切不解析**：只负责一行一条、多行合并、整个算一条，切出来的内容原样放进 `__raw__` 字段。syslog、CEF、kv、Windows 日志、grok 交给下游的 `parser` 节点。 |
| T3 | **新设计不迁就老代码的怪行为**。兼容问题以后在转接层处理。 |
| T4 | 类型映射：decimal → 字符串；时间戳 → 毫秒数；日期 → 从 1970 年起的天数；bytes → base64；**超过 2^53 的 int64 丢精度可以接受**；**proto3 没赋值的普通字段输出默认值**。 |
| T5 | 不做：avro/protobuf 的 Confluent 格式、Schema Registry、zip/tar 归档、Java 堆栈的结构化解析。 |
| T6 | "自动"解压只认 gzip。 |

以前为"接入来源"做的决定放在**附录 A**，接来源时再用。

---

## 1. 现状的问题（R1）

- **认的格式少，参数写死**（R4、R8）：`EncodingType` 是不带参数的 enum。CSV 写死成逗号分隔、首行是表头，charset 也配不了。
- **没有二进制格式**（R7）：avro、protobuf、parquet 都不能反序列化。
- **没有多行合并**（R10）："按行切"写在某几个格式的子类里，没法和别的格式组合。
- **API 默认整份数据都在内存里**（R5）：`deserialize(SerializedEvent)` 要拿到整个 payload。只有 FsSource 在 source 这一层自己做了分块和流式读取。
- **解压只支持 gzip**，而且有好几种写法（R6）。
- **入口太多，还有重复实现**：`FleakDeserializer` 有四个入口、两个能力开关；CSV 有三份解析代码。
- **中间类型多余**：每加一个格式，都要写 factory、typed deserializer，再挑一个 converter。

---

## 2. 目标和不做的事

**目标**
- G1 统一接口：输入字节流，输出一条一条的结果（R3、I4）。小数据也可以一次全拿。
- G2 用一份声明式配置写清楚压缩、格式、切分方式（R4）。
- G3 **内存有上限**（R5）：内存里同时最多放"一条记录"或者"一个必须整个拿进来的单元"（整个文档、avro block、parquet row group）。两种上限都由调用方给，超了就报出来，不会把内存撑爆。
- G4 解压算法由用户指定（R6）。
- G5 覆盖 R7 到 R10 里的格式；以后加一种格式，只需要一个 config 类、一个实现、一行注册。
- G6 每条结果只会是四种之一：成功、数据坏了（尽量带原文）、太大、读取本身出错（I5）。

**不做**
- N1 序列化。
- N2 接入任何来源，不动老配置（T1），见附录 A。
- N3 跨输入拼接。比如一条多行日志被拆进了两条消息，不负责把它们拼回去。
- N4 T5 里列的那些。
- N5 类型推断：CSV 和文本的值都当字符串。
- N6 合并 metadata（比如 Kafka 的 key）。这件事由调用方在拿到记录之后自己做。

---

## 3. 整体流程

```
ByteInput（字节流；需要时提供随机读取）
   │  ← 包一层：底层读取一出错就做记号 [读]
   ▼
① 解压：用户指定的算法链；"auto" 只认 gzip
   │  ← 包一层：解压一出错就做记号 [解压]
   ▼
② 非 UTF-8 的文本类格式（text、csv）先转码成 UTF-8
   ▼
③ 单条字节预算：给"当前这一条"计数，超了立刻停，做记号 [预算]
   │
   ├────────────────────────────┐
   ▼                            ▼
④a 格式自己知道一条从哪到哪       ④b 文本：在 UTF-8 字节上切
   json / csv / xml / yaml        一行一条 | 多行合成一条 | 整个算一条
   avro / protobuf / parquet      每条输出成 {"__raw__": "..."}
   │                            │
   └─────────────┬──────────────┘
                 ▼
⑤ 调用方一条一条地拉：Decoded | Failed | Oversized；读取本身出错时直接抛异常
```

**为什么让调用方"拉"：**
- 调用方想凑多大一批就凑多大，不用自己分块，多行记录也就不会被块边界切断；
- 不想要了就不拉，直接 `close()`；
- avro、parquet 的 reader 本来就是这种方式；
- 调用方自己代码里的错误，不会被当成"数据坏了"。

---

## 4. 接口（R3、I4）

```java
/** 由 DecodingConfig 编译出来；不可变、线程安全，编译一次、反复用。 */
public interface RecordDecoder {
  /** open() 不读任何字节；字节在第一次 next() 时才开始读，有问题也在那时报出来。 */
  RecordCursor open(ByteInput input, DecodeLimits limits);

  /** 小数据的便捷入口：一次拿到全部结果。内部基于 byte[] 提供随机读取，所以 parquet 也能用。 */
  default DecodeResult decodeAll(byte[] bytes, DecodeLimits limits) { ... }
}

/**
 * 上限由调用方给。0 表示不设上限，这时没有任何内存保护。
 * maxRecordBytes：一条记录（一行、一个多行事件、一行 CSV、一个 JSON 数组元素、一个 protobuf 消息等）的上限。
 * maxDocumentBytes：必须整个拿进内存的单元（整个文档、avro block 解压后、parquet row group 解压后）的上限。
 */
public record DecodeLimits(long maxRecordBytes, long maxDocumentBytes) {
  public static final DecodeLimits UNLIMITED = new DecodeLimits(0, 0);
}

/**
 * 输入，由调用方实现。翻译机只调用它，不负责它的生命周期。
 * 实现方如果知道数据应该有多长，提前读到结尾时必须抛 IOException，不能只返回 -1。
 */
public interface ByteInput {
  InputStream openStream() throws IOException;           // 原始字节（还没解压），只打开一次
  Optional<RandomAccessInput> randomAccess();            // parquet 需要；拿不到时为 empty
}

/** 随机读取：每次只读一个有边界的区间，读完就关。 */
public interface RandomAccessInput {
  long length() throws IOException;
  InputStream read(long offset, long length) throws IOException;  // 只读 [offset, offset+length)
}

public interface RecordCursor extends AutoCloseable {
  /** 读完返回 null。数据问题用结果返回；只有读取本身出错才抛 SourceReadException。 */
  DecodeOutcome next() throws SourceReadException;
  @Override void close();                                // 关掉它自己打开的流
}

/** 读取本身出错（断网、远端出错、磁盘读写失败），跟数据内容无关。 */
public class SourceReadException extends IOException { ... }
```

```java
public sealed interface DecodeOutcome {
  long recordIndex();
  RawSpan span();      // 这一条在【解压并转码之后】的字节流里的位置；给不出来时为 null

  record Decoded(RecordFleakData record, long recordIndex, RawSpan span) implements DecodeOutcome {}

  record Failed(ErrorKind kind, byte[] raw, long recordIndex, RawSpan span, Exception error,
      boolean terminal) implements DecodeOutcome {}

  /** 太大：一条记录超过 maxRecordBytes，或者一个单元超过 maxDocumentBytes。不带内容。 */
  record Oversized(long recordIndex, RawSpan span, long sizeSoFar, byte[] preview,
      boolean terminal) implements DecodeOutcome {}
}

public enum ErrorKind {
  MALFORMED,             // 格式不对，也包括解析库抛出的各种意外异常
  CORRUPT_COMPRESSION    // 解压出错：数据被截断、损坏，或者尾部跟着不是压缩数据的字节
}

public record RawSpan(long offset, long length) {}   // length = -1 表示"从 offset 一直到输入结尾"
public record DecodeResult(List<RecordFleakData> records, List<DecodeOutcome> problems) {}
```

### 4.1 出错怎么分

**三个记号**（见流程图）：`[读]`、`[解压]`、`[预算]`。
- 每一层都把自己负责的调用包起来，出错的那一刻先做记号，再把异常往上抛。
- `[读]` 管的范围：`ByteInput` 返回的流、`RandomAccessInput` 的 `length()`、`read()`，以及 `read()` 返回的流。
- 解析库可能把这些异常包上好几层，也可能直接吞掉。所以**判断错误种类时只看记号，不看异常的类型和 message**。

**硬规则：** cursor 每次要返回一个结果、或者要结束（返回 null）之前，先按优先级查记号：

1. `[读]` 在 → 抛 `SourceReadException`。不返回结果，也不当作正常结束。
2. `[解压]` 在 → `Failed(CORRUPT_COMPRESSION, terminal=true)`。
3. `[预算]` 在 → `Oversized`。
4. 三个记号都不在，但解析报错了 → `Failed(MALFORMED)`。

"解压出错"可能是数据真坏了，也可能只是文件还没传完，翻译机分不出来。所以它单独占一种，由调用方决定丢掉还是下次重读。

**`terminal` 的意思：**
- `false`：这一条坏了，但能找到下一条从哪开始，所以接着往下解析。
- `true`：找不到下一条从哪开始，返回这一个结果之后 cursor 就结束。这时 `raw` 可能是空的：
  - 如果给得出 `span`，它表示"从出错的位置一直到输入结尾"；
  - 如果 `span` 是 null，调用方要找回数据，就只能整个输入重新拿。

**截断**：只有带结束标记的格式才能发现截断，发现了就报 `Failed`，不当作正常结束。这些格式是：
- json array、json object、xml、avro ocf、parquet；
- protobuf varint_delimited（读到半条消息时，长度前缀对不上）；
- 所有压缩格式。

没有结束标记的格式（text、csv、json lines、yaml multi_document）分不出"文件就到这儿"和"文件被截断了"。这几种靠 `ByteInput` 的约定兜底：调用方知道长度却提前读完时，`ByteInput` 抛 IOException，翻译机把它当成"读取出错"。

**太大**：
- **单条预算**（③）：一条记录读到超过 `maxRecordBytes` 时立刻停下，不让解析库继续往内存里塞，做 `[预算]` 记号，然后返回 `Oversized`。
  - 这一层要给解析库的预读留 64 KiB 余量。所以对解析库解析的格式（csv、json array、xml elements、yaml multi_document）来说，上限是**近似**的：比 `maxRecordBytes` 稍大、但在余量以内的记录，仍然可能正常解析出来。
  - 翻译机自己切分的格式（line、multiline、json lines、protobuf varint_delimited）上限是**精确**的。
  - 例子：CSV 里有一个引号没闭合，后面整个文件都会被当成同一个字段。超过上限时它会停下，不会把内存撑爆。
- **解析库自带的上限**一律放开，统一交给预算层判断，免得被误报成"格式不对"：
  - Jackson 的字符串长度、名字长度，以及 SnakeYAML 的 `codePointLimit`，都设成最大值；
  - Jackson 的 `maxDocumentLength` 只在 json `object` 模式下设置，设成 `maxDocumentBytes`；
  - Woodstox 的属性长度设成最大值。
  - 嵌套深度（1000 层）和数字长度（1000 位）保持默认，超了报 `MALFORMED`。
- **必须整个拿进来的格式**（json `object`、xml `document`、yaml `document`、text `whole`、protobuf `single`、avro `datum`）只看 `maxDocumentBytes`，超了报 `Oversized(terminal=true)`。
- **avro block、parquet row group**：读之前先看大小，超过 `maxDocumentBytes` 就报 `Oversized(terminal=true)`，不去申请内存，详见 5.2。
- **XZ** 解压需要的内存超过上限（`max(maxDocumentBytes, 128 MiB)`）时，报 `Oversized(terminal=true)`，不报成"解压出错"。
- 太大的那一条只保留开头 4 KiB 作为预览（avro block 和 parquet row group 没有预览），其余的边读边丢，只计数。
- line、multiline、json lines、protobuf varint_delimited 能找到下一条开头（protobuf 靠长度前缀跳过去），所以 `terminal=false`。其余格式都是 `terminal=true`（D13）。

**各格式的出错表**（每一行都要有测试）：

| 格式 | 情况 | 结果 |
|------|------|------|
| 任何格式（`protobuf single` 和 `avro datum` 除外） | 输入为空（解压后 0 字节） | 没有结果，第一次 `next()` 就返回 null |
| `protobuf single`、`avro datum` | 输入为空 | 解析出一条记录：proto3 是"所有字段取默认值"；如果 avro schema 不能用 0 字节表示，就报 `Failed(MALFORMED)` |
| 任何格式 | 读到一半网络断了（不管断在 gzip 片段的头部还是中间，也不管解析库怎么包装异常） | 抛 `SourceReadException` |
| 任何格式 | 配了 gzip，但输入根本不是 gzip | 第一次 `next()` 返回 `Failed(CORRUPT_COMPRESSION, terminal=true)` |
| 任何格式 | 压缩数据被截断、损坏，或者后面跟着垃圾 | 损坏位置**之前**的记录照常全部返回，然后返回 `Failed(CORRUPT_COMPRESSION, terminal=true)` |
| json lines | 一行不是合法 JSON；或者是合法 JSON 但不是对象；或者对象后面还有别的内容（比如 `{"a":1} xyz`、`{"a":1}{"b":2}`） | `Failed(MALFORMED, terminal=false)`，raw 是这一行 |
| json array | 根节点不是数组，或者中间有语法错 | `Failed(MALFORMED, terminal=true)` |
| json array | 某个元素不是对象 | `Failed(MALFORMED, terminal=false)` |
| json object、xml document、yaml document | 格式不对、根节点不是对象、或者根节点后面还有别的内容 | `Failed(MALFORMED, terminal=true)`，raw 是整个文档 |
| csv | 某一行列数比表头少或者多 | `Failed(MALFORMED, terminal=false)` |
| csv | 引号没闭合 | 在超过 `maxRecordBytes` 之前就读到了结尾：`Failed(MALFORMED, terminal=true)`；先超过了上限：`Oversized(terminal=true)` |
| xml elements | 结构坏了 | `Failed(MALFORMED, terminal=true)` |
| xml elements | 输入不为空，但 `recordPath` 一个都没匹配到 | `Failed(MALFORMED, terminal=true)`，错误信息说明"路径没有匹配到任何元素" |
| xml 任何模式 | 含有 DTD 或外部实体 | `Failed(MALFORMED, terminal=true)`，不展开、不读取外部资源 |
| yaml multi_document | 某个文档解析失败 | `Failed(MALFORMED, terminal=true)` |
| yaml multi_document | 某个文档不是映射 | `Failed(MALFORMED, terminal=false)` |
| avro ocf | 文件头、block 头、同步标记坏了；数据坏了；或者输入没有正好在 block 边界上结束 | `Failed(MALFORMED, terminal=true)` |
| avro datum | 解析失败，或者解析完之后还有剩余字节 | `Failed(MALFORMED, terminal=true)`，raw 是整个输入 |
| protobuf single | 解析失败 | `Failed(MALFORMED, terminal=true)`，raw 是整个输入 |
| protobuf varint_delimited | 某条消息解析失败 | `Failed(MALFORMED, terminal=false)`，按长度前缀跳到下一条 |
| protobuf varint_delimited | 长度前缀本身坏了，或者读到半条消息输入就结束了 | `Failed(MALFORMED, terminal=true)` |
| parquet | 不是 parquet 文件，或者 footer、页数据坏了 | `Failed(MALFORMED, terminal=true)` |
| text line、multiline | 不会解析失败，只可能出现 `Oversized` | — |

### 4.2 编号和位置

- **`recordIndex`：**
  - 按行切的格式：物理行号，从 1 开始，空行也算。
  - 多行合并：这一条第一行的行号。
  - 其他格式：第几条，从 1 开始。
  - 整个拿进来的格式：成功是 1，失败是 -1。
- **`span`：** 只在翻译机能精确算出来的地方才给。
  - 给：line、multiline、json lines、protobuf varint_delimited、整个拿进来的格式（`(0, 总长度)`），以及 json array 的元素（用 Jackson 的字节偏移）。
  - 不给（null）：csv、xml elements、yaml multi_document、avro ocf、parquet。原因：这几种格式的库只提供字符位置，或者压根没有单条记录自己的字节。
  - `span` 指的是**解压、转码之后**的位置，不能直接拿到原文件上按字节读。

### 4.3 `Failed.raw`（I5）

| 格式 | raw |
|------|-----|
| line、multiline、json lines、protobuf varint_delimited、json array 的元素 | 这一条的字节（UTF-8 输入时就是原始字节） |
| 整个拿进来的格式 | 整个文档的字节（不超过 `maxDocumentBytes`） |
| csv | 按 CSV 格式把这一行的各个值重新拼成一行（D8） |
| xml elements、yaml multi_document | 空（D8） |
| avro ocf、parquet | 空：avro ocf 的记录在压缩过的 block 里，没有单独的字节 |
| terminal，而且定位不到是哪一条 | 空 |

- 所有文本类格式都是先转成 UTF-8 再处理的，所以只有原输入是 UTF-8 时，"这一条的字节"才等于原始字节；其他编码的输入，raw 是转码后的 UTF-8（D9）。
- `Oversized` 不带内容，只带开头 4 KiB 预览、已经数到的字节数，以及 `span`（给得出时）。需要原文的调用方，按 `span` 去取；`span` 是 null 时，就整个输入重新拿（D10）。

### 4.4 输出记录

- 每条 `RecordFleakData` 都是新对象，不复用，也不在原地修改。
- 翻译机不合并 metadata（N6）。

---

## 5. 配置（R4）

```jsonc
{
  "compression": ["gzip"],          // 可选。按顺序逐层解压；也可以写 "auto"（只认 gzip）；不写就是不解压
  "format": { "type": "...", ... }  // 必填
}
```

- Java 里叫 `DecodingConfig`。`format` 用 Jackson 的 `@JsonTypeInfo(property = "type")` 区分子类型，写法和现有的 `ExtractionConfig` 一样。
- `DecodingConfigCompiler.compile(config)` 把配置编译成 `RecordDecoder`。参数组合、正则、schema、charset 都在这一步检查，**配错了编译就报错**。比如 parquet 配了任何 `compression`（包括 `auto`），编译时就会报错。
- 上限不放在配置里，由调用方通过 `DecodeLimits` 传进来。

### 5.1 解压（R6）

- 新增一个只给解码用的 `DecompressionCodec`：`GZIP, ZSTD, SNAPPY, LZ4, BZIP2, XZ, DEFLATE`。现有的 `CompressionType` 不改。
- **"auto" 只认 gzip（T6）**。bzip2 的识别标志是文本 "BZh"，普通文本也可能这么开头，容易误判。
- **所有算法都按同一套严格规矩读：**
  1. 几段压缩数据首尾相接时，全部读出来；
  2. 尾部跟着的字节如果不是压缩数据，报 `CORRUPT_COMPRESSION`；
  3. **出错位置之前已经解压出来的数据，一个字节都不能丢**，前面的记录照常交出去。
- **各算法的做法：**
  - **gzip**：一段一段地读。用 commons-compress 读完一段，自己检查后面是不是下一段 gzip 的开头，再决定往下读还是报错。gzip 头也自己解析，并且给头的长度设上限（64 KiB），防止坏掉的文件名字段把内存撑爆。
    - 不开 commons-compress 的"多段拼接"模式：那个模式遇到尾部垃圾时，会把同一次读出的好数据一起扔掉。
    - 也不用 JDK 的 `GZIPInputStream`：它读下一段的头时遇到 IOException 会直接吞掉，当成"读完了"。
  - **bzip2、xz、lz4**：打开 commons-compress 的多段模式。算法报告读完以后，再检查后面还有没有剩余字节。
  - **deflate**：指 zlib 包装的格式（RFC 1950）。用 JDK 的 `Inflater` 自己写读取循环，读完后还有剩余字节就报错。
  - **zstd、snappy**：用 commons-compress。snappy、lz4 只支持标准的 frame 格式（D11）。
- **依赖**：commons-compress、zstd-jni 改成显式声明；新加 `org.tukaani:xz`。snappy、lz4 用 commons-compress 自带的纯 Java 实现。
- **XZ 内存上限**：`max(maxDocumentBytes, 128 MiB)`，超了按"太大"处理，见 4.1。
- avro、parquet 格式自带的压缩由格式自己处理，不在这里配。

### 5.2 格式自己知道一条从哪到哪的

所有格式都要求**一条记录是一个对象**，不是对象的报 `MALFORMED`，见 4.1 的表。

| type | 参数 | 整个拿进来？ | 需求 |
|------|------|------|------|
| `json` | `mode`：`object`（整个输入是一个对象）、`array`（顶层是数组，一个元素一条）、`lines`（一行一个对象，空行跳过） | object 是 | R8 |
| `csv` | `delimiter`（默认 `,`）、`quote`（默认 `"`，设成 null 表示不用引号）、`escape`、`header`（`first_line`；`none`，此时列名是 `_c0`、`_c1`……；或者直接给一组列名）、`skipLines`、`commentMarker`、`ignoreSurroundingSpaces`（默认 false，和 RFC 4180 一致）、`charset`（非 UTF-8 会先转码）。开头的 BOM 总是去掉 | 否 | R8 |
| `xml` | `mode`：`document`（整个算一条），或者 `elements` 加 `recordPath`（比如 `/Events/Event`，用 StAX 一段一段切，按元素的本地名匹配，忽略命名空间） | document 是 | R8 |
| `yaml` | `mode`：`document`，或者 `multi_document`（按 `---` 分开，一个文档一条） | document 是 | R8 |
| `avro` | `container`：`ocf`（文件头里自带 schema），或者 `datum`（一个输入就是一条二进制记录，`schema` 写在配置里） | datum 是；ocf 一次拿一个 block | R7 |
| `protobuf` | `descriptorSet`（base64 编码的 FileDescriptorSet）、`messageType`、`framing`：`single`（整个输入是一条）或者 `varint_delimited`（多条首尾相接，每条前面带长度） | single 是 | R7 |
| `parquet` | — | 一次拿一个 row group | R7。需要 `ByteInput.randomAccess()`；拿不到就在第一次 `next()` 时抛 `IllegalStateException`，因为这是调用方用错了，不是数据坏了 |

**XML：**
- **安全设置**：两种模式都关掉 DTD 和外部实体（`SUPPORT_DTD=false`，`IS_SUPPORTING_EXTERNAL_ENTITIES=false`），防止 XXE 攻击读出服务器上的本地文件。
- **怎么变成对象**：XML 元素转换成对象的规则，和 Jackson `XmlMapper` 一样（属性、重复的子元素、文本内容都照它的规则处理）。`elements` 模式下，把 `XmlMapper` 套在已经定位好的 StAX reader 上，一次读一个元素。所有的值都是字符串。

**avro 的内存上限（G3）：**
- **ocf 文件的结构由我们自己解析**：文件头、每个 block 的条数和大小、同步标记、最后是不是正好结束在 block 边界上，都由自己的代码读。
  - block 的压缩后大小，在申请内存**之前**就和 `maxDocumentBytes` 比较。
  - 解压时也给输出设上限，解压后的大小超过 `maxDocumentBytes` 就停。
  - 然后再把解压好的 block 交给 avro 的 `DatumReader`，一条一条解。
  - 不用 `DataFileStream`：它在 `hasNext()` 里就已经按 block 头里写的大小分配了内存，还会不受限制地解压；而且读到 EOF 时直接当作正常结束，发现不了截断。
- **记录内部的长度也要检查**：字符串、bytes、数组的长度，在申请内存之前先和"这个 block（或这个 datum 输入）还剩多少字节"比较，超过就报 `MALFORMED`。这样，schema 配错或者数据被改了一个比特，也不会去申请 2 GB 内存。实现上，在 avro 的 decoder 外面包一层来做这个检查。不用 avro 的 JVM 全局设置，因为那会影响到别的代码。

**parquet 的内存上限：** 读一个 row group 之前，用 footer 里记录的**解压后**大小（`getTotalByteSize()`）和 `maxDocumentBytes` 比较。

### 5.3 文本（`type: "text"`，T2）

```jsonc
"format": {
  "type": "text",
  "charset": "UTF-8",          // 默认 UTF-8；如果是别的编码，会先转码成 UTF-8
  "framing": { "type": "line" }
}
```

| framing | 参数 | 说明 |
|---------|------|------|
| `whole` | — | 整个输入算一条，受 `maxDocumentBytes` 限制 |
| `line` | `skipBlank`（默认 true） | 按 LF、CRLF、CR 切行 |
| `multiline` | `startPattern` 或 `continuePattern` 二选一；`maxLines`（默认不限） | 把多行合成一条（R10）。超过 `maxLines` 或 `maxRecordBytes` 时，整条按 `Oversized` 处理；后面属于这一条的续行也一起跳过，直到下一条开头为止。这样一条堆栈不会被拆成好几条看起来正常的记录 |

- 不是 UTF-8 的输入（比如 UTF-16，Windows 事件查看器导出的文件就是这种），会先整个转码成 UTF-8，再在 UTF-8 字节上切。开头的 BOM 总是去掉。
- 非法字节在解码时变成 U+FFFD。UTF-8 输入时，`raw` 里保留原始字节。
- 每一条都输出成 `{"__raw__": "<文字>"}`，字段名和现在 STRING_LINE、TEXT 的输出一样。下游 `parser` 节点的 `targetField` 设成 `__raw__`，就能接着拆字段。

**例子（R9、R10）：**

```text
Java 堆栈：framing = multiline，continuePattern = "^(\s|Caused by: )"
Windows 事件：framing = multiline，startPattern = "^Log Name:"   → parser 节点用 windows_multiline
syslog / CEF / key=value：framing = line                          → parser 节点用 syslog / cef / kv_pair
```

### 5.4 类型怎么转成字段（R7、R8，T4）

**二进制格式（avro、protobuf、parquet）：**

| 原类型 | 转成 |
|--------|------|
| boolean | 布尔 |
| string | 字符串 |
| 有符号整数、float、double | 数字（FleakData 内部用 double 存，超过 2^53 的 int64 会丢精度，T4 已接受） |
| 无符号整数：protobuf `uint32`/`fixed32`/`uint64`/`fixed64`，parquet `UINT_*` | 按无符号解释后的数字，不会变成负数；超过 2^53 时丢精度 |
| decimal | 字符串，不丢精度 |
| bytes、fixed | base64 字符串 |
| enum | 字符串 |
| 带时区的时间戳：avro/parquet 的毫秒、微秒、纳秒，parquet INT96，protobuf `google.protobuf.Timestamp` | epoch 毫秒，微秒、纳秒截到毫秒 |
| 不带时区的时间戳：avro `local-timestamp-*`，parquet `isAdjustedToUTC=false` | 把墙上时间当成 UTC 算出的毫秒数 |
| date | 从 1970-01-01 起的天数 |
| time-of-day | 当天的毫秒数 |
| protobuf `google.protobuf.Duration` | 毫秒数 |
| union、optional | 取实际那个分支的值；值为 null 的字段不输出 |
| map、record、struct、message | 对象 |
| array、repeated、list | 数组 |
| protobuf 带 presence 的字段（proto2、proto3 `optional`、message 类型）没赋值时 | 不输出 |
| protobuf proto3 普通标量字段 | 总是输出，没赋值就输出默认值（T4） |
| protobuf `oneof` | 只输出赋了值的那个 |

**YAML：**

| YAML 值 | 转成 |
|--------|------|
| 时间戳（如 `2024-01-15T10:00:00Z`） | epoch 毫秒；没写时区就按 UTC |
| 日期（如 `2024-01-15`） | 从 1970-01-01 起的天数。不依赖 JVM 时区 |
| `!!binary` | base64 字符串 |
| 不是字符串的 key（数字、布尔） | 转成字符串 |
| 其他标量 | 布尔、数字、字符串照原样 |

**XML、CSV、文本**：所有值都是字符串。

---

## 6. 内部结构和扩展

```java
interface RecordFormat { RecordCursor open(DecodeContext ctx); }   // 自己知道边界的格式
interface Framer       { FrameCursor open(DecodeContext ctx); }    // 文本切分
record Frame(byte[] raw, long recordIndex, RawSpan span) {}
final class DecodeContext { ... }   // 处理后的流、预算、limits、randomAccess、三个记号
```

- **注册**：在 `FormatConfig`、`FramingConfig` 上用 `@JsonSubTypes` 登记，每个 config 类实现 `compile()`。不用 ServiceLoader。
- **4.1 的"查记号"硬规则只写在 cursor 外壳这一个地方**，所有格式共用，避免每个格式各写一遍时漏掉。
- **每种格式只有一份解析代码**。`decodeAll` 就是 `open` 加一个循环。

---

## 7. 测试

- **出错表**：4.1 表里的每一行都要有 fixture。另外还要覆盖：空行、CRLF、只有 CR、BOM、非法 UTF-8、UTF-16 和 windows-1252 的文本与 CSV、CSV 引号里带空格、XXE 攻击样例（包括读本地文件和外部 DTD）。
- **每种压缩算法**都要测：
  - 多段数据首尾相接，要全部读出来；
  - 尾部有垃圾、数据被截断时，**之前的每一条记录都要正常输出**，然后才是 `CORRUPT_COMPRESSION`；
  - gzip 头里有超长的文件名字段时，不能撑爆内存；
  - xz 用 `-6`、`-9` 压缩的文件，在上限较小时要报 `Oversized`。
- **截断**：只测带结束标记的格式（4.1 列出的那些），压缩、不压缩各测一遍。截断必须报 `Failed`，不能当成正常结束。
- **断网**：每种格式都要测，包括 parquet 的随机读取、gzip 段与段之间的交界、gzip 头的中间。不管解析库怎么包装异常，都必须抛 `SourceReadException`。
- **小内存测试**：单独建一个 Gradle Test 任务 `lowMemoryTest`，最大堆设为 64m。它的测试打上 `lowMemory` 标签，普通的 `test` 任务排除这个标签（`excludeTags 'golden', 'lowMemory'`），并且让 `check` 依赖它。
  - 设好 `DecodeLimits`，用生成器造出远大于堆的输入，每种流式格式都要跑完。
  - 每种格式都造一个超大的单条：CSV 引号不闭合、超长 JSON 字符串、超大 XML 元素或属性、超大 YAML 文档。结果必须是 `Oversized`，而且没有 OOM。
  - avro 的大 block，要覆盖每种 avro 内部压缩：null、deflate、snappy、zstd、bzip2、xz。还有 avro 里超大的字符串长度、parquet 的大 row group。结果必须是 `Oversized` 或者 `MALFORMED`，而且没有 OOM。
- **上限边界**：翻译机自己切的格式，大小正好等于 `maxRecordBytes` 时正常解析，多 1 个字节就是 `Oversized`。解析库解析的格式，测"超过上限加余量之后一定报 `Oversized`"。
- **parquet 随机读取**：用一个假的 `RandomAccessInput` 记下每次请求的 `(offset, length)`。检查两点：每次请求都有边界；所有请求加起来不超过真正需要的字节数再加一个 chunk。
- **类型转换**：5.4 两张表的每一行都要测，包括无符号整数的最大值、YAML 的日期（要用两个不同的 JVM 时区各跑一遍）。
- **配置编译**：参数配错、正则写错、schema 不合法、parquet 配了 compression，都要在编译时报错。
- **断言方式**：把 `DecodeOutcome` 转成一个可以直接比较的视图，再整体比较。视图里，`raw` 和 `preview` 比内容，`error` 比类名加 message。
- **门禁**：`./gradlew check`，包括 `:lib:test`、`lowMemoryTest`、`goldenTest`。CI 跑的是 `./gradlew build`，会带上 `check`。

---

## 8. 分几步做（每步一个 PR）

1. 接口、`DecodingConfig` 和编译器；三个记号和"查记号"的硬规则；预算层；gzip（一段一段读）和 "auto"；`json` 三种模式；`text` 的 `line` 和 `whole`。同时搭好 `lowMemoryTest` 和测试框架，并接进 `check`。
2. `text` 的 `multiline`，以及非 UTF-8 的转码。
3. `csv`。
4. `xml`（包括安全设置）、`yaml`。
5. 其余的解压算法，加上 `org.tukaani:xz`。avro 的 xz codec 要用到它，所以排在 avro 前面。
6. `avro`（ocf 的结构自己解析）。
7. `protobuf`。
8. `parquet`（显式声明 parquet-hadoop，锁在 delta-kernel 现在用的 1.12.3）。

---

## 9. 偏差（和需求比做少了什么）

| # | 需求 | 这次做到什么程度 | 状态 |
|---|------|----------------|------|
| D1 | R3、R4、R5 端到端 | 翻译机做完，但**还没接任何来源**，用户暂时用不上 | 用户提议（T1） |
| D2 | R6 | 不支持 zip、tar | 已接受（T5） |
| D3 | R7 | 不接 Schema Registry，不做 Confluent 格式 | 已接受（T5） |
| D4 | R7 | 超过 2^53 的 int64 会丢精度 | 已接受（T4） |
| D5 | R10 | Java 堆栈只做多行合并，不做结构化解析 | 已接受（T5） |
| D6 | R5 | 必须整个拿进来的单元（整个文档、avro block、parquet row group）受 `maxDocumentBytes` 限制，超了就报"太大"、不读 | 格式本身决定 |
| D7 | R7 | parquet 需要调用方提供随机读取；外面再套一层压缩的 parquet 不支持 | 格式本身决定 |
| D8 | I5 | csv 的 raw 是用各个值重新拼出来的一行；xml elements、yaml multi_document 的 raw 是空的；csv、xml elements、yaml multi_document 都不给 `span` | 这几个库拿不到单条记录准确的字节位置 |
| D9 | I5 | 非 UTF-8 的文本和 csv 会先转码，所以 raw 是转码后的 UTF-8 字节 | 统一按 UTF-8 字节切分 |
| D10 | I5 | `Oversized` 只带 4 KiB 预览；terminal 而且定位不到是哪一条时，raw 是空的；avro ocf、parquet 的 raw 是空的。需要原文时，按 `span` 去原输入里取；`span` 是 null 就整个输入重新拿 | 为了内存有上限 |
| D11 | R6 | snappy、lz4 只支持标准 frame 格式；Hadoop 的 snappy、raw block 格式的 LZ4 要读的时候才报 `CORRUPT_COMPRESSION` | 需要时以后加 |
| D12 | R7 | avro ocf 中间有一条坏了就直接结束，不尝试跳到下一个 block | 简化实现 |
| D13 | R5 | csv、json array、xml elements、yaml multi_document 里出现一条超大记录时，整个输入就读到这里结束，后面的记录不再解析 | 这几种格式超大之后找不到下一条的开头 |
| D14 | — | 解析库解析的格式，单条上限是近似的：比 `maxRecordBytes` 稍大（64 KiB 以内）的记录仍然可能被正常解析出来 | 要给解析库的预读留余量 |

---

## 附录 A：以后接来源时要用的决定（这次不做）

下面这些是用户以前定好的。接来源时照着做，每个来源单独出一份方案。

- **直接替换底层**：老的 `encodingType` 配置自动改走新翻译机。兼容只放在翻译机外面的转接层里，老行为分三类处理：
  1. 合理的，映射成普通选项；
  2. 有人依赖的老格式，由转接层转换；
  3. 老毛病，直接改掉，写进变化清单。

  下面这些老行为新翻译机故意没做，要在转接层里处理，或者写进变化清单：
  - 老 JSONL 允许一行是一个对象数组；
  - 老 TEXT、STRING_LINE 会保留 BOM；
  - 老 CSV 会去掉引号外面的空格；
  - 老 gzip 能容忍尾部垃圾；
  - JSON 对象后面跟着别的内容时，老代码直接忽略；
  - metadata 平铺合并进记录，冲突时以 metadata 为准；
  - `subrecordIndex` 的编号规则。
- **老配置怎么映射**：

  | 老配置 | 映射成 |
  |---|---|
  | `JSON_OBJECT` | json object |
  | `JSON_ARRAY` | json array |
  | `JSON_OBJECT_LINE` | json lines（一行是数组的情况交给转接层） |
  | `STRING_LINE` | text line，`skipBlank=false` |
  | `TEXT` | text whole |
  | `CSV` | csv，`header=first_line`，`ignoreSurroundingSpaces=true` |
  | `XML` | xml document |
- **其他决定：**
  - 远端文件先下载到本地硬盘再读，默认上限 1 GiB，可以调；超过就退回到边下边读。
  - 单条太大：DLQ 里只放一张取货单，写明文件地址、版本、位置。
  - 下游出错：写进 DLQ，存解析好的 JSON，并加上格式标记。
  - 没配 DLQ：记 metrics、打 error 日志，然后丢掉，不重试。
  - S3FileReader 只保证行为不变。
- **接入顺序建议**：FsSource → 消息类来源 → S3 实时来源和 FileSource。
- **别的仓库要做的事：**
  - dynamicapi 的 DLQ 预处理和采样要认识新配置；
  - dynamicapi 现在"一条读不回来就丢掉所有 DLQ 信息"的老问题要修；
  - zephflow-plus 的重放工具要支持新的 DLQ 标记。

## 附录 B：前几轮审查踩过的坑（接每个来源时逐条对照）

1. 断网和数据坏了要分清楚，包括解析库把网络错误包起来、或者吞掉的情况。
2. `CORRUPT_COMPRESSION` 可能只是文件还没传完。可以重试，但次数要有上限。
3. 生产环境停机就是直接杀进程，正在处理的文件下次会重读，所以会有重复。
4. 每一个 metrics、告警、"运行失败"信号变不变，都要写清楚。
5. 读到一半出了不可恢复的错，前面已经发出去的数据算不算，要写清楚。
6. DLQ 里新加的东西，重放工具和自愈功能能不能读；重放工具处理完一个文件就会把它整个删掉。
7. `__md_` 是现有的 metadata 前缀，会被合并进记录。新加的控制字段要换一个前缀。
8. SQS 一次收多条消息时，排在后面的会超时。
9. 本地暂存要管好：目录、锁、清理、容量、多副本，还有 k8s 的 ephemeral-storage。
10. 版本信息要从读取时的响应里取，不要从 listing 里取。按偏移读要用有边界的 range，否则 S3 的流在关闭时会把剩下的内容全读完。
11. DLQ writer 要按字节数 flush，阈值在测试里能调小；GCS 的 writer 也要一起改。
12. 每一处行为变化都要写进清单（I2）。
13. `CORRUPT_COMPRESSION` 之前的记录已经交出去了，"重读"时要考虑重复。

---

## 置信度

约 75%。这一版补的都是审查员亲手复现出来的库行为问题，涉及 gzip、bzip2、deflate、avro、CSV 的位置、YAML 的类型、XML 的安全设置；也去掉了几处容易出错的"精确"承诺。剩下的风险是可能还有没被发现的库行为，比如某个库还会在什么地方吞掉错误、或者偷偷申请大块内存。这些由第 7 节的截断、断网、小内存测试按格式逐个兜底。
