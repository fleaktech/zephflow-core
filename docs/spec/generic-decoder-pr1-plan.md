# Generic decoder — PR1 plan（地基：接口 + gzip + JSON + 按行/整体文本）

- 设计文档：`docs/design/generic-deserialization.md` v10，§8 第 1 步
- 分支：从 `feature/generic-decoder` 拉出 `feature/generic-decoder-pr1`，PR 的 base 是 `feature/generic-decoder`
- 只新增代码，不改任何现有类、source command 或 ScalarCommand

## 1. 这个 PR 做什么

| 设计文档章节 | PR1 的内容 |
|---|---|
| §4 接口 | `RecordDecoder`、`RecordCursor`、`ByteInput`、`DecodeLimits`、`DecodeOutcome`（`Decoded` / `Failed` / `Oversized`）、`ErrorKind`、`RawSpan`、`DecodeResult`、`SourceReadException`；另外提供 `ByteInput.ofBytes(byte[])` |
| §4.1 出错分类 | 三个记号（`[读]` `[解压]` `[预算]`）和"查记号"的硬规则；硬规则只写在一个地方：cursor 外壳 |
| §4.1 太大 | 单条预算层 |
| §5 配置 | `DecodingConfig`、`DecodingConfigCompiler` |
| §5.1 解压 | 只做 `gzip` 和 `auto` |
| §5.2 JSON | `json` 的 `object` / `array` / `lines` |
| §5.3 文本 | `text` 的 `line` / `whole`；本 PR 只支持 UTF-8，别的 charset 在编译配置时报"暂不支持"，留给 PR2 |
| §7 测试 | `lowMemoryTest` 任务，并接进 `check` |

**这次先不做**，等到对应的 PR 再加，到时候只增不改：

- `RandomAccessInput` 和 `ByteInput.randomAccess()`：PR8 parquet 才用得上；
- 其余解压算法：PR5；
- multiline 和转码：PR2。

## 2. 审查遗留问题里要在 PR1 解决的

都来自 `docs/design/review/loop3/` 第 3 轮的审查记录。每一条都要有对应的测试。

| 来源 | 问题 | PR1 的做法 |
|---|---|---|
| R3-B2、R3-B3、R3-A1（gzip 部分） | commons-compress 的 gzip 会多读，出错时还会丢掉已经解压出来的数据 | **gzip 完全自己实现**：<br>• 用 JDK 的 `Inflater(nowrap=true)` 自己写解压循环；<br>• 头和尾也自己解析：头部最多 64 KiB，尾部校验 CRC32 和 ISIZE；<br>• 每一段（member）读完后，看缓冲区里接下来的字节：没有了就结束；是 `1f 8b` 就接着读下一段；是别的字节就报 `CORRUPT_COMPRESSION`；<br>• 解压出来的字节一产出就交给下游，所以出错时之前的数据一个都不会丢 |
| R3-B9 | 解压可能卡死在"需要字典"的状态 | `needsDictionary()`、或者 `inflate` 返回 0 但既不需要输入也没结束，都报 `CORRUPT_COMPRESSION`，不会死循环 |
| R3-B13 | 配了 gzip，但输入是 0 字节 | 0 字节的原始输入不产生任何结果，配了 gzip 也一样（第一次 `next()` 就返回 null） |
| R3-A5、R3-B12 | 整个文档的格式由谁来限制大小 | 整个读进来的格式（json `object`、text `whole`）由预算层**精确**限制在 `maxDocumentBytes`；不设 Jackson 的 `maxDocumentLength` |
| R3-A7 | terminal 时 `span` 的起点不对 | `span` 从"第一条还没交出去的记录"的第一个字节开始算 |
| R3-A8 | `[预算]` 记号会一直留着 | 这个记号只管当前这一条。`terminal=false` 的格式交出 `Oversized` 后要清掉它，下一条重新计数。测试要验证：超大一行后面紧跟的正常一行能正常解析 |
| R3-B11 | Jackson 在 UTF-16/32 时没有字节偏移；字符串 token 的结束位置不准 | 只有 Jackson 识别出输入是 UTF-8 时才给 `span`，其他编码 `span` 为 null。数组元素的边界用"元素开始 token 的偏移"和"元素读完之后的偏移"来算，不用中间某个 token 的位置 |
| R2-B9 | Jackson 自带的长度限制 | `maxStringLength`、`maxNameLength` 设成最大值；嵌套深度和数字长度保持默认，超了报 `MALFORMED` |
| R2-A12、R2-B14 | JSON 后面跟着多余内容 | json `object` 和 json `lines` 的每一行：根值后面只允许空白，有别的内容就报 `MALFORMED` |

## 3. 关键实现方式

**数据从进到出要经过这几层**：

```
ByteInput.openStream()
 → ReadGuard（任何 IOException 都打上 [读]，再包成 SourceReadException 抛出）
 → 解压（gzip 用自己的 GzipMemberStream；auto 先看前 2 个字节；none 什么都不做）
     解压自己检测到的错误打上 [解压]；下层抛上来的 SourceReadException 原样往上抛，不打 [解压]
 → 交给格式实现
```

**每种格式怎么切、怎么解析**：

| 格式 | 做法 | raw 和 span |
|---|---|---|
| json `lines`、text `line` | 共用一个**自己写的按行切分器**：直接在字节上找 LF / CRLF / CR。一行超过 `maxRecordBytes` 时，只留开头 4 KiB 作为预览，剩下的边读边丢、只计数，直到下一个换行符，然后交出 `Oversized(terminal=false)`。json lines 每切出一行，就用 Jackson 解析这一行的字节 | 都精确 |
| json `object`、text `whole` | 把输入整个读进内存，最多读到 `maxDocumentBytes`。超了就交出 `Oversized(terminal=true)`，带开头 4 KiB 预览。没超就整个解析 | raw 就是整个文档 |
| json `array` | Jackson 流式解析。中间加一层 `BudgetCaptureStream`：记下当前元素从哪个字节开始，把元素的字节一边读一边存起来（用于 raw 和预览）。字节数超过"元素起点 + `maxRecordBytes` + 64 KiB 余量"时，打上 `[预算]` 并抛出异常 | 能拿到 Jackson 字节偏移时精确 |

**cursor 外壳**（`GuardedCursor`）负责统一收尾：

- 包住具体格式的 cursor。
- 每次要交出结果、或者要返回 null 之前，先按 `[读]` > `[解压]` > `[预算]` 的顺序查记号，按硬规则处理。
- 格式实现抛出来的异常：查完记号以后，归类成 `Failed(MALFORMED)`。`Error`（比如 OOM）不拦，直接往外抛。
- 只要交出过一个 `terminal` 的结果，之后每次 `next()` 都返回 null。

**配置格式**：

```jsonc
{ "compression": ["gzip"] | "auto" | [], "format": {"type": "json", "mode": "array"} }
```

- `compression` 写一个字符串（比如 `"auto"`）或者一个数组都可以。`auto` 必须单独出现，不能和别的算法写在一起。
- `format.type` 用 Jackson 的 `@JsonTypeInfo` 区分，写法和 `ExtractionConfig` 一样。
- 编译时就检查：未知的 mode、`auto` 和别的算法混用、text 用了 UTF-8 以外的 charset，都直接报错。

## 4. 要改的文件

**改一个现有文件：**

- `lib/build.gradle`
  - `test` 任务的 `excludeTags` 改成 `'golden', 'lowMemory'`；
  - 新增 `lowMemoryTest` 任务：`includeTags 'lowMemory'`，`maxHeapSize = '64m'`；
  - `check` 增加依赖 `lowMemoryTest`。

**新增文件**，都在 `lib/src/main/java/io/fleak/zephflow/lib/serdes/decode/` 下：

| 目录 | 文件 |
|---|---|
| `decode/` | `RecordDecoder`、`RecordCursor`、`ByteInput`、`DecodeLimits`、`DecodeOutcome`、`ErrorKind`、`RawSpan`、`DecodeResult`、`SourceReadException` |
| `decode/config/` | `DecodingConfig`、`DecompressionCodec`、`FormatConfig`（JSON 子类型登记处）、`JsonFormatConfig`、`TextFormatConfig`、`FramingConfig`（`line` / `whole`）、`DecodingConfigCompiler` |
| `decode/internal/` | `DecodeFlags`、`ReadGuardStream`、`GuardedCursor`、`BudgetCaptureStream`、`LineFramer`、`WholeInputReader` |
| `decode/compress/` | `GzipMemberStream`、`Decompressors` |
| `decode/json/` | `JsonObjectFormat`、`JsonLinesFormat`、`JsonArrayFormat` |
| `decode/text/` | `TextLineFormat`、`TextWholeFormat` |

**测试**，放在 `lib/src/test/java/io/fleak/zephflow/lib/serdes/decode/` 下，对应上面各个包，另外加：

- `OutcomeView`：把一条结果转成可以直接比较的形式（raw 和 preview 比内容，error 比类名和消息），测试里构造完整的期望值整体比较；
- 测试用的"坏流"：一种是 `available()` 永远返回 0、每次只给几个字节；另一种是读到第 N 个字节时抛 IOException。

## 5. 测试清单（每一条都要真正跑到对应的代码路径）

1. **4.1 出错表里，PR1 涉及的每一行**：
   - 空输入；
   - 配了 gzip 但输入不是 gzip；
   - gzip 被截断、损坏、尾部有垃圾（在段与段之间、段的尾部、段的中间各测一次）；
   - json lines：一行不是合法 JSON、不是对象、后面跟着多余内容；
   - json array：根节点不是数组、中间有语法错、某个元素不是对象；
   - json object：格式不对、后面跟着多余内容；
   - text line、text whole 的 `Oversized`。
2. **gzip**：
   - 多段 gzip，用"`available()` 永远返回 0、每次只给几个字节"的流读，所有字节都要读出来；
   - 尾部有垃圾、或者被截断时，**之前的每一条记录都要交出来**，最后才交出 `CORRUPT_COMPRESSION`；
   - 头部的文件名字段没有结尾（不带 NUL）时，最多读 64 KiB 就报错，不能 OOM；
   - 需要字典的坏数据、0 字节的 gzip 输入。
3. **断网**：
   - 在 gzip 头的中间、两段的交界处、段的中间、未压缩输入的中间各断一次；
   - json array 的中间断一次：Jackson 会把异常包一层，照样要抛 `SourceReadException`。
4. **预算**：
   - 按行切的格式：正好等于 `maxRecordBytes` 的行能解析，多 1 个字节就是 `Oversized`；超大的一行后面紧跟一行正常的，正常的那行要能解析（R3-A8）；
   - json array：超过上限加余量的元素必须是 `Oversized(terminal=true)`；
   - 整体格式：正好等于 `maxDocumentBytes` 能解析，多 1 个字节就是 `Oversized`。
5. **`span` 和 raw**：
   - 按行切的格式、json array 元素（UTF-8）给出的 `span` 要精确；
   - UTF-16 的 JSON `span` 为 null；
   - terminal 时 `span` 的起点正确（R3-A7）。
6. **`lowMemoryTest`**（堆 64m）：
   - 用生成器造远大于堆的输入（几百 MB，不落盘），json lines、json array、text line 都要能跑完；
   - 超大的单行、超大的数组元素、超长的 JSON 字符串都要报 `Oversized`，不能 OOM。
7. **配置编译**：上面第 3 节列出的每一种配置错误都要测；`compression` 写字符串和写数组两种形式都要能解析。

## 6. 验证和门禁

- 在 worktree 里跑 `./gradlew :lib:spotlessApply`，然后跑 `./gradlew build`，也就是 CI 跑的命令，包括 `check`、`goldenTest`、`lowMemoryTest`。
- 写完代码后，进入 code 模式的 adversarial review：A、B 两个审查员，最多 3 轮；审到的问题和处理结果记在 `docs/spec/generic-decoder-pr1-review-log.md`。
- 审查通过、门禁全绿之后，开 PR 到 `feature/generic-decoder`。

## 7. 偏差（和设计文档 v10 不一样的地方）

| # | 设计文档 | PR1 实际做法 | 原因 |
|---|---|---|---|
| P1 | `RandomAccessInput` 放在第 1 步 | 推迟到 PR8 | 目前没有任何地方用得到 |
| P2 | text 支持各种 charset | 只支持 UTF-8，其他 charset 编译时报"暂不支持" | 按设计文档，转码在 PR2 做 |
| P3 | §5.1：gzip"用 commons-compress 读完一段" | 用 JDK `Inflater(nowrap)` 自己写解压循环，头、尾也自己解析 | 审查 R3-B2/B3：commons-compress 会多读，出错时还会丢掉已经解压出来的数据 |
| P4 | §4.1：json `object` 设置 Jackson 的 `maxDocumentLength` | 不设置，整个文档的大小由 `WholeInputReader` 精确限制 | 审查 R3-A5/B12：Jackson 超限抛的异常带不了标记，会被误分类 |
| P5 | json 不转码；计划 §2 原本写的是"UTF-16 时 span 为 null" | 非 UTF-8 的 JSON 先转成 UTF-8 再处理。编码按 RFC 4627 §3 的 4 字节规则加上 BOM 来识别；只要前两个字节都不是 0、而且不是 UTF-16 的 BOM，就直接认定为 UTF-8，不再往下读。转码用自己写的 `Utf8TranscodingStream`：每读一次底层就把结果交出去，出错时已经转好的字节不会丢；遇到坏的码元只把那一个码元替换成 U+FFFD；转码后的输出不带 BOM。`span` 和 `raw` 都指转码后的 UTF-8 字节，和设计 D9 一样 | 代码审查 R1-A1、R2-B1/B3/B4/B5/B6。直接拒绝会丢数据；commons-io 的 `ReaderInputStream` 要攒满一整块才交出去，出错时会丢数据 |
| P6 | §5.1 规则 3："出错前的数据一个字节都不能丢" | 改成：解压器在损坏点之前已经吐出来的数据，全部交出去，一个字节都不丢。JDK 21 的 `Inflater` 在抛 `DataFormatException` 之前，会把已经写出的字节计进 `getBytesWritten()`。**注意**：损坏的数据在被发现之前，可能已经解成了一段看起来合法、其实是错的内容；最坏的情况是 zlib 一直没报错，直到这一段（member）末尾的 CRC 校验才发现，这时这一段剩下的全部记录都已经交给下游了。所以一段 member 里的记录，要等它末尾的 CRC 校验通过才算确认无误 | 代码审查 R1-B1、R2-B8 |
| P7 | 计划 §2 要处理“需要字典”的情况 | 不处理，相关代码已删除 | `Inflater(nowrap=true)` 永远不会要求字典；这个问题只出现在 zlib，留到 PR5 的 deflate |
| P8 | 计划 §2 R3-A8："清掉 `[预算]` 记号" | 按行切的格式在切分器里自己做精确的单条预算，**不用** `[预算]` 记号；`[预算]` 只给库来解析的格式用（PR1 里只有 json array），它的 Oversized 一律是 terminal | 记号只服务于 terminal 的情况，所以不存在"清掉"这一步 |
| P9 | R2-A12 只要求 json object 和 json lines 检查尾部多余内容 | json array 的 `]` 后面有别的内容，也报 terminal 的 `MALFORMED` | 和"带结束标记的格式要严格检查"保持一致 |
| P10 | 设计没定义"空白行" | `skipBlank` 跳过空行，以及只有空格或制表符的行 | — |
| P11 | — | 公开 API 多了几个小工具：`DecodeOutcome.terminal()` 默认方法、`RawSpan.toEnd`、`DecodeLimits` 不能为负数的检查、`ByteInput` 上的 `@FunctionalInterface`。另外，配置里的名字（压缩算法、json mode）不区分大小写 | 方便使用，没有新增行为 |
| P12 | 计划 §2：gzip 头的上限是 64 KiB | 改成 1 MiB | RFC 1952 允许合法的 FEXTRA 长度到 65535 字节（代码审查 R1-B7）。头的内容只是跳过、不保存，所以这个上限只用来防止"永远读不完的头" |
| P13 | — | **已知限制**：json array 里，`[` 前面、两个元素之间、`]` 后面的空白，如果连续超过 64 KiB，会被算进前一条或后一条记录的预算，可能被误判成 `Oversized` | 代码审查 R1-B6、R2-A3，影响很小，暂不修 |
| P14 | 计划 §3："每次要交出结果之前都先查记号" | 只在三种情况下查记号：format 抛异常、到达结尾、给出 terminal 的结果。查到解压错误时，terminal 的结果会被改报成 `CORRUPT_COMPRESSION`。正常解出来的记录、以及非 terminal 的失败照常交出去，不受影响 | 代码审查 R2-A1：每条都查的话，出错前已经解出来的好记录会被扣下；只看 terminal 的结论，就不会再把"解压出错"误报成"格式不对" |
| P15 | 设计 §6：每个 config 类自己实现 `compile()` | `FormatConfig` 改成 sealed interface，由 `DecodingConfigCompiler` 里的 switch 统一编译；实现接口是 `FormatCursor` / `FormatFactory` | 少写一个子类时编译器会直接报错。代价是加一种格式要改三处：`permits`、switch、`@JsonSubTypes`。设计 §6 已同步修改（代码审查 R2-A4） |
| P16 | — | `GzipMemberStream` 里"inflate 没有任何进展"那个分支只是防御代码，按 JDK 文档的约定不可能走到，所以没有测试 | 代码审查 R2-A6 |
