# 本地内容寻址对象存储（Content-Addressed Object Store, Go）

用本地磁盘模拟对象仓库：数据按**内容摘要**寻址，天然去重；每个逻辑键维护
**只追加的版本链**；GC 只回收**不被任何版本引用**的数据块。重复写入、版本
切换与并发清理在统一的读写锁下得到一致、可解释的结果。

## 目录布局

```
<root>/
  objects/<aa>/<其余62位hex>   内容寻址的不可变数据块（SHA-256）
  meta/<bb>/<64位hex>.json     以 sha256(key) 命名的键元数据（版本链）
  tmp/*.tmp                    写入过程中的临时文件；崩溃残留由 GC 清理
```

- `objects` 与 `meta` 都按摘要前 2 位分桶，避免单目录文件过多。
- 数据块文件名即其内容地址，因此**同名即同内容**，且文件发布后不可变。

## 寻址方式（去重与完整性的统一依据）

1. 写入时流式计算 `SHA-256` 摘要并统计字节数，地址为摘要的小写十六进制（64 位）。
2. 相同内容必然得到相同地址：无论写多少次、写到哪个键，物理上只存一份。
   - 发布数据块前若目标地址文件已存在，直接丢弃本次临时文件（去重命中）。
3. 读取时重新流式计算摘要并与地址、元数据中的大小比对：
   - **大小不符**在打开对象时立即判定；
   - **等长篡改**在读到 EOF（或 `Close` 时补验）通过摘要判定；
   - 校验是流式的，不需要把整个对象读进内存。

### 错误如何区分

所有失败都可用 `errors.Is` 判定，而不是笼统的一个错误：

| 错误 | 含义 |
| --- | --- |
| `cas.ErrNotFound` | 键在元数据中不存在 |
| `cas.ErrVersionNotFound` | 键存在，但请求的版本序号越界 |
| `cas.ErrDataMissing` | 元数据引用了数据块，但磁盘文件缺失（丢失） |
| `cas.ErrCorrupt` | 数据块存在，但大小或 SHA-256 摘要与地址不符（损坏/篡改） |
| `cas.ErrInvalidArgument` | 空键、nil reader、空根目录等参数错误 |

## 版本规则

- 每次 `Put(key, r)` 都向该键的版本链**追加**一条记录
  `{hash, size, created_at}`，序号从 0 开始连续递增，绝不修改历史记录。
- `Get(key)` 读取最新版本；`GetVersion(key, idx)` 读取指定历史版本
  （`-1` 同最新）；`Versions(key)` 返回完整版本链。
- 覆盖写**不会删除旧数据块**——旧版本仍引用它，仍可完整读回。

### 写入原子性（中断 / 并发下看不到半成品）

`Put` 全程持有进程内写锁，并严格按以下顺序提交：

1. 数据写入 `tmp/obj-*.tmp` 并 `fsync`，同时计算地址；
2. 原子 `rename` 为内容地址（不可变数据块，不存在覆盖不同内容的问题）；
3. 元数据以 `tmp/meta-*.tmp` 写完 `fsync` 后原子 `rename` 替换。

任何一步失败（包括传输中途 `io.ErrUnexpectedEOF`）都会清理临时文件并返回错误：
- 元数据未提交 ⇒ 版本链不增加，旧版本照常可读；
- 外界通过 `Get` 永远只能看到已完整提交的版本。

## 垃圾回收规则

`GC()` 返回 `GCStats{Scanned, Kept, Removed, BytesRemoved, TempFilesRemoved}`，
判定过程全程持有写锁，与 `Put`/`Get` 串行化：

1. 扫描 `meta/` 下所有键，汇总被**任意键、任意版本**引用的地址集合（存活集合）；
2. 扫描 `objects/`，地址不在存活集合中的数据块才删除；
3. 清理 `tmp/` 中超过 1 小时的 `.tmp` 残留（崩溃中断的写入）；新鲜临时文件保留。

关键性质：

- **不误删**：存活集合在写锁内采集。Put 的“数据块改名 → 元数据提交”两阶段
  不可能与 GC 交错，因此不会出现“块刚发布、元数据还没提交就被当垃圾删掉”
  的悬挂引用；
- **不重复释放 / 不遗漏**：每个地址一轮只判定、删除一次；块按地址唯一命名，
  对同一批垃圾重复执行 GC 结果幂等；
- **共享内容受保护**：多个键/版本去重共享同一块时，只要还有一个引用存在，
  该块就保留。

> 注意：当前版本只提供“追加版本”，不主动删除版本；因此 GC 真正回收数据的
> 典型场景是元数据文件被外部删除（键/版本整体作废），或未来加入版本删除 API。
> 崩溃遗留的临时文件由 GC 依据年龄清理。

## 快速上手

```go
s, _ := cas.Open("/var/lib/my-store")
defer s.Close()

v, _ := s.Put("report-2026", strings.NewReader("...")) // 追加一个版本
rc, ver, err := s.Get("report-2026")                   // 读最新版，读取时校验
defer rc.Close()
data, _ := io.ReadAll(rc)                              // 损坏会在此处返回 ErrCorrupt

rc2, _, _ := s.GetVersion("report-2026", 0)            // 读历史版本
defer rc2.Close()

st, _ := s.GC()                                        // 回收无引用块
fmt.Printf("scanned=%d kept=%d removed=%d\n",
    st.ObjectsScanned, st.ObjectsKept, st.ObjectsRemoved)
```

并发模型：并发安全保证限于**同一进程**内多个 goroutine（`*Store` 内部
`sync.RWMutex`）；不支持多个进程同时打开同一目录。

## 验证方法

```bash
go build ./...
go vet ./...
go test -v ./...          # 日志会打印输入数据与“判定依据”
go test ./... -count=10   # 多次重复压测并发场景
```

测试覆盖场景：

| 测试 | 覆盖点 |
| --- | --- |
| `TestPutGet_Deduplication` | 相同内容跨键/重复写入只落盘一份，读回校验通过 |
| `TestVersions_HistoryReadable` | 覆盖写产生连续新版本，历史版本与最新版分别可读且内容不被改动 |
| `TestRead_MissingVsCorrupt` | 键缺失、版本越界、数据块文件缺失、等长篡改、截断五类原因可区分 |
| `TestConcurrentPut` | 16 goroutine 并发写 + 并发读：版本无丢失、无半成品、每个版本可校验 |
| `TestPut_Interrupted` | 传输中断不追加版本、不污染旧版本 |
| `TestGC_ReclaimsUnreferenced` | 无引用块被回收、重复 GC 幂等（不重复释放） |
| `TestGC_SharedContent` | 多键共享块在仍有引用时不被误删 |
| `TestGC_StaleTempCleanup` | 仅清理陈旧临时文件，保留正在写入的临时文件 |
| `TestGC_ConcurrentWithReadWrite` | GC 与持续读写并发：零误删、所有读取通过校验 |
| `TestGC_NoDanglingReferenceUnderRace` | 高频写/GC 赛跑后 480 个版本无缺失、无损坏 |
| `Example` | 可运行的端到端演示（`go test -run Example`） |
