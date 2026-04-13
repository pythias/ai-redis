# ai-redis 实现进度

## 已完成
- [x] Task 2: SCAN MATCH 修复 (glob_match backtracking bug)
- [x] Task 2b: 多DB存储架构 (MOVE, SELECT, SWAPDB)

## 进行中
- [ ] Task 4: Lua脚本 (EVAL, EVALSHA, FCALL)

## 待处理
- [ ] Task 4: Lua脚本 (EVAL, EVALSHA, FCALL)
- [ ] Task 5: CONFIG命令 (GET/SET/REWRITE/RESETSTAT)
- [ ] Task 6: 事务 (WATCH, UNWATCH, MULTI, EXEC, DISCARD)
- [ ] Task 7: 内省命令 (SLOWLOG, OBJECT, DEBUG, COMMAND, CLIENT)
- [ ] Task 8: 流和地理扩展 (XCLAIM, XTRIM, GEORADIUSBYMEMBER, GEOHASH)
- [ ] Task 9: 服务器命令 (SHUTDOWN, SLAVEOF, ROLE, MIGRATE, MONITOR等)
- [ ] Task 10: Clippy清理 (bitmap.rs setbit + list.rs mut)

---

## Task 3: 持久化
**目标**: 实现 SAVE, BGSAVE, LASTSAVE, BGREWRITEAOF

### SAVE
- 同步将所有数据库写入 dump.rdb
- 格式: Redis RDB 兼容格式
- 需要生成 checksum (CRC64)

### BGSAVE
- 后台异步执行 SAVE
- 返回 Background saving started
- 记录开始时间供 LASTSAVE 使用

### LASTSAVE
- 返回最近一次 BGSAVE 的 Unix 时间戳

### BGREWRITEAOF
- 后台重写 AOF 文件
- 返回 Background append only file rewriting started

---

## Task 4: Lua脚本
**目标**: 实现 EVAL, EVALSHA, FCALL

- 使用 mlua 或 rlua 库执行 Lua 5.1 脚本
- Redis Lua 沙箱限制 (禁用危险函数)
- EVALSHA 缓存脚本
- 支持 KEYS 和 ARGV

---

## Task 5: CONFIG命令
**目标**: 实现 CONFIG GET, CONFIG SET, CONFIG REWRITE, CONFIG RESETSTAT

### CONFIG GET
- 返回匹配的配置参数

### CONFIG SET
- 设置运行时配置
- 支持: maxmemory, port, bind, etc.

### CONFIG REWRITE
- 将当前配置写回 redis.conf

### CONFIG RESETSTAT
- 重置统计计数器

---

## Task 6: 事务
**目标**: 实现 WATCH, UNWATCH, MULTI, EXEC, DISCARD

### MULTI/EXEC
- 开启事务，队列命令，执行

### DISCARD
- 清空事务队列

### WATCH
- 乐观锁，监视 key 变化

### UNWATCH
- 取消所有监视

---

## Task 7: 内省命令
**目标**: 实现 SLOWLOG, OBJECT, DEBUG, COMMAND, CLIENT

### SLOWLOG
- 记录慢查询日志

### OBJECT
- OBJECT REFCOUNT, OBJECT ENCODING, OBJECT TTL

### DEBUG
- DEBUG SLEEP, DEBUG SEGFAULT 等

### COMMAND
- 返回所有命令信息

### CLIENT
- CLIENT LIST, CLIENT KILL, CLIENT GETNAME, CLIENT SETNAME

---

## Task 8: 流和地理扩展
**目标**: 实现 XCLAIM, XTRIM, GEORADIUSBYMEMBER, GEOHASH

### XCLAIM
- 声明消息所有权

### XTRIM
- 裁剪流

### GEORADIUSBYMEMBER
- 查找成员周围地理范围内的点

### GEOHASH
- 返回 geohash 字符串

---

## Task 9: 服务器命令
**目标**: 实现 SHUTDOWN, SLAVEOF, ROLE, MIGRATE, MONITOR, INFO, TIME 等

### SHUTDOWN
- 优雅关闭，保存数据

### SLAVEOF
- 设置主从复制

### ROLE
- 返回实例角色 (master/slave/child)

### MIGRATE
- 原子性迁移 key

### MONITOR
- 实时监控命令

### INFO
- 服务器信息统计

### TIME
- 返回服务器当前时间

---

## Task 10: Clippy清理
**目标**: 消除所有 clippy 警告

- bitmap.rs setbit 冗余检查
- list.rs mut 不需要可变
- 其他 style 警告
