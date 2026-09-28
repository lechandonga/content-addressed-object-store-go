# 本地内容寻址对象存储（Go）

用本地磁盘模拟的内容寻址对象仓库（Content-Addressed Object Store）。支持：

- **内容去重**：数据按 SHA-256 摘要寻址，相同内容只存一份；
- **多版本**：覆盖写入只追加新版本，历史版本始终可读；
- **原子发布**：临时文件 + `fsync` + 同目录 `rename`，中断/并发下外界看不到半成品；
- **安全回收**：基于全量版本引用的垃圾回收，与读写通过跨进程文件锁互斥，不误删、不重复释放；
- **可解释的错误**：「缺失」与「损坏」是两类明确可区分的错误，而不是笼统的 I/O 失败。

仅依赖 Go 标准库。模块路径：`github.com/lechandonga/content-addressed-object-store-go`。

---

## 1. 磁盘布局与寻址方式

```
<repo-root>/
├── LOCK                    # 跨进程访问协调锁（flock）
├── blobs/                  # 内容寻址数据块（只追加，不可变）
│   └── ab/cd/abcd…64hex   # 两层分片：前 2 字节 / 次 2 字节 / 完整摘要
├── objects/                # 每个对象一个目录（对象名做 URL PathEscape）
│   └── <escaped-name>/
│       ├── meta.json       # 版本链 + 当前版本指针（原子替换）
│       └── commit.lock     # 该对象的跨进程提交锁
└── tmp/                    # 写入临时文件（崩溃残留由 GC 清理）
```

**寻址规则**

- 写入时流式计算 SHA-256（与落盘同步完成），摘要的 64 位十六进制字符串即数据块地址 `ContentKey`；
- 数据块路径为 `blobs/<key[0:2]>/<key[2:4]>/<key>`，两层分片避免单目录文件过多；
- 相同内容 ⇒ 相同摘要 ⇒ 相同路径。发布时若目标块已存在则直接复用并删除本次临时文件，因此**重复写入自动合并，只存一份**；
- 数据块一经发布即**不可变**：任何版本引用的字节永远不会被原地修改，这是读一致性与去重安全的基础。

## 2. 版本规则

- `meta.json` 保存完整版本链 `versions[]` 与当前版本指针 `current`；每个版本是一个不可变引用 `{id, key, size, created_at}`；
- `Put(name, r)`：在对象的 `commit.lock` 内重读元数据 → 分配单调递增的新版本号（从 1 开始）→ 追加到链尾 → 更新 `current`；
- 覆盖写入**不删除旧版本**：`Get` 返回当前版本，`GetVersion(name, id)` 可读取任意历史版本，`Versions(name)` 按版本号从新到旧列出全部版本；
- 同一对象的并发提交由该对象的提交锁串行化，因此版本号唯一、连续、无丢失；不同对象的提交互不阻塞。

## 3. 原子性与中断恢复

写入分为三段，每段都不会向读者暴露半成品：

1. **接收数据**：写入 `tmp/incoming-*` 并同步计算 SHA-256，完成后 `fsync`；
2. **发布数据块**：同目录原子 `rename` 到 `blobs/` 分片路径（块已存在则复用），随后 `fsync` 目录；
3. **发布元数据**：在对象提交锁内写 `meta.json.tmp` → `fsync` → 原子 `rename` 覆盖 → `fsync` 对象目录。

- 数据源中途失败、进程崩溃：未 rename 的临时文件留在 `tmp/`，已 rename 但未提交元数据的数据块成为**无引用块**——外界都看不到对应对象（旧的 `meta.json` 完好无损）；
- 这些残留物由 `GC` 统一清理；读者永远只会看到「旧版本」或「完整的新版本」，绝不会读到半截文件。

## 4. 回收（GC）规则

`Store.GC(ctx)` 的判定与保证：

- **引用集合**：遍历所有对象的 `meta.json`，汇总**全部历史版本**引用的数据块键（集合去重：同一份内容被 N 个版本共享也只算一个）；
- **删除条件**：数据块存在于 `blobs/` 但不在引用集合中 ⇒ 删除（含崩溃后未提交的无引用块）；同时清空 `tmp/` 残留；
- **绝不误删**：被任何对象的任何版本引用的块一律保留；GC 持**独占访问锁**，与进程内外的所有读写互斥，因此发布过程中的块不会被误判删除；
- **不重复释放/不遗漏**：删除走「先收集引用、再扫描删除」的单趟快照；第二次 GC 对已删除的块报告 0 删除、0 回收字节；
- **安全中止**：任一对象元数据损坏时，本次 GC 直接返回 `KindCorrupt` 错误，**一个块都不删**——宁可遗留无引用块，也不在信息不可信时冒误删风险。

## 5. 并发模型

两把锁各司其职：

| 锁 | 范围 | 模式 | 作用 |
|---|---|---|---|
| `LOCK`（访问锁） | 整个仓库 | 读/写取 `LOCK_SH`，GC 取 `LOCK_EX` | 读写彼此完全并行；GC 与一切读写互斥（`flock`，跨进程） |
| `objects/<name>/commit.lock` | 单个对象 | 提交时 `LOCK_EX` | 串行化同一对象的并发版本提交 |

- 进程内用 `sync.RWMutex` 对齐同一套语义；跨进程用 POSIX `flock`，每次操作独立打开锁文件描述符、结束即关闭，避免不同 goroutine 共用 fd 时提前释放彼此的锁；
- 读路径只依赖数据块不可变性与 `meta.json` 的原子替换，不需要对象级读锁。

## 6. 错误分类（缺失 vs 损坏）

所有错误都带类别（`errors.As` 可判）：

| 场景 | 错误类型 | 判定依据 |
|---|---|---|
| 对象从未写入 | `*NotFoundError{Obj}` | `meta.json` 不存在 |
| 对象在、但请求的版本号不存在 | `*NotFoundError{Obj, Version}` | 版本链中找不到该 id |
| 版本在、但引用的数据块文件缺失 | `*NotFoundError{Obj, Version, Key}` | `blobs/` 下找不到块 |
| 数据块字节与内容键不符（被篡改/截断） | `*CorruptionError{Obj, Version, Key, Got}` | 读完重算 SHA-256 ≠ Key，或长度不符 |
| `meta.json` 无法解析 | `*Error{Kind: KindCorrupt}` | JSON 反序列化失败 |
| 只读模式下 Put/GC | `*Error{Kind: KindIO}` | 打开时指定 `ReadOnly: true` |

读取流在 `Read` 返回 `EOF` 和 `Close` 时都会校验：截断读取同样被识别为损坏。

## 7. 快速上手

```go
s, _ := store.Open("/var/lib/caos", nil)
defer s.Close()

v, _ := s.Put(ctx, "report.txt", strings.NewReader("...")) // 写/覆盖，返回新版本
rc, ver, err := s.Get(ctx, "report.txt")                   // 读当前版本
rc2, _, err := s.GetVersion(ctx, "report.txt", v.ID-1)     // 读历史版本
vs, _ := s.Versions(ctx, "report.txt")                     // 列出全部版本
stats, _ := s.GC(ctx)                                      // 回收无引用数据
```

## 8. 验证方法

```bash
# 全部单测（含去重/多版本/缺失与损坏区分/中断恢复/并发写入/并发 GC/跨进程锁）
go test ./... -v

# 竞态检测（需要 CGO）
CGO_ENABLED=1 go test ./... -race

# 静态检查
go vet ./...
```

测试用例与覆盖点：

| 测试 | 覆盖场景 |
|---|---|
| `TestDedup` | 相同内容写两个对象只产生 1 个数据块、键相同 |
| `TestOverwriteKeepsHistory` | 覆盖产生新版本、当前指针切换、历史版本逐版本可读、版本列表顺序 |
| `TestLargeAndBinaryPayload` | 1 MiB 二进制数据往返一致 |
| `TestMissingObject` / `TestMissingBlob` | 对象缺失与数据块缺失分别得到带不同字段的 `NotFoundError` |
| `TestCorruptBlob` / `TestTruncatedBlob` | 篡改/截断数据块得到 `CorruptionError`，错误携带实算摘要，与缺失互斥 |
| `TestPutInterrupted` | 数据源中途失败后对象不可见、无半成品块；模拟崩溃残留由 GC 清掉 |
| `TestGCPreservesReferenced` | 含共享块在内的所有被引用块及历史版本 GC 后完好 |
| `TestGCRemovesOrphansOnce` | 无引用块删除恰好一次，第二次 GC 0 删除 0 回收 |
| `TestGCAbortsOnCorruptMeta` | 元数据损坏时 GC 中止且不删任何数据 |
| `TestConcurrentPutSameObject` | 40 goroutine 并发覆盖：版本号唯一连续、每版内容完整 |
| `TestConcurrentGCAndIO` | 8 写 + 8 读 + 持续 GC：全程无损坏/误删，结束后 240 个版本全可读 |
| `TestCrossProcessLock` | 子进程持 GC 独占锁时父进程读锁阻塞，释放后才获取 |
| `TestReadOnlyRejectsMutations` | 只读模式拒绝 Put/GC |

每个用例的日志都打印了**输入数据**与**判定依据**（`t.Logf`，见 `-v` 输出）。
