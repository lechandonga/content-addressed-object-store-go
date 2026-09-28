package store

import "strconv"
import "time"

// nowFn 可在测试中被替换以固定版本时间戳。
var nowFn = time.Now

func nowUTC() time.Time { return nowFn().UTC() }

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
