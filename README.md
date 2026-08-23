# DeepSeek Harness Cron

一个面向 DeepSeek Harness Host 的耐久定时任务 Bundle。它把 Cron 作业、运行记录和心跳保存在独立 SQLite 数据库中，并为每次触发创建隔离的 Agent Session。

当前实现针对并锁定 `dsh-v0.1.1-rc.2`。DeepSeek Harness 仍处于 developer preview；升级 Harness 前请重新运行本仓库的完整检查与临时 Profile 启动测试。

## 能力

- 严格五字段 Cron：`minute hour day-of-month month day-of-week`
- IANA 时区与 DST 计算
- SQLite WAL、原子 claim/advance、乐观 revision、禁止同一 Job 重叠
- Host 重启后的 misfire 处理：`latest` 或 `skip`
- 每次触发使用独立 Session，并持久化结构化完成回执
- Root Agent 管理工具：创建、查看、更新、暂停、恢复、删除、立即运行、查看历史
- 所有写操作都经过两轮、参数绑定的人类确认；Cron/Plugin/Goal/Subagent 来源不能改计划
- Runner 工具 allowlist、最大并发、单次超时和外部健康检查

## 与官方 `dsh-schedule` 的关系

`dsh-v0.1.1-rc.2` 已包含一个可选的 `@deepseek-ai/dsh-schedule`，适合“30 分钟后提醒我”或“每 5 分钟在当前对话提醒”这类 Session-local 提醒。它支持 `after`、绝对 `at` 和 fixed-rate `every`，到期后向原 Session 排入 follow-up；不支持日历/Cron 表达式，也不提供独立业务回执。

本 Bundle 面向另一类需求：Host 级长期作业、日历 Cron、独立 Runner Session、权限与工具隔离、运行历史、超时和心跳。普通个人提醒优先使用官方 Schedule；无人值守的办公/开发执行任务再使用本 Bundle。二者工具名与存储域不同，可以同时安装。

## 安装

本仓库目前是本地 Bundle，尚未发布到 npm。

```bash
cd /absolute/path/to/dsh-cron
pnpm install
pnpm check

dsh plugin --profile web add .
dsh --profile web --dump-config
```

`add .` 必须从本仓库目录执行；也可以传入本仓库绝对路径。安装、移除或更新 Bundle 后，需要重启正在运行的 Profile。`--dump-config` 只验证组合后的配置，不会启动插件。

rc.2 Profile 明确使用 `autoInstallPeers: false`，由 `$DSH_HOME/profiles/node_modules` 的 Harness installation fallback 提供 Cordis/DSH 单例。因此 pnpm 安装外部 Bundle 时可能显示这些 peer 为 missing；不要为了消除提示而在 Profile 内另装一套 Harness 核心包，应以随后 `--dump-config` 和实际启动结果为准。

默认 Bundle 配置见 [`cordis.patch.yml`](./cordis.patch.yml)。它使用 `America/Los_Angeles`、`standard` Agent Preset、`workspace-write` 权限 Preset、最多一个并发运行，并将数据库放在 `$DSH_HOME/cron/cron.sqlite`。

## 使用

安装并重启 `web` Profile 后，可以直接在顶层对话中说：

```text
每天工作日上午 9:00，在 /Users/me/work/project 中检查测试和未提交改动，
生成一份简短报告。使用 America/Los_Angeles 时区。
```

Agent 会使用以下工具管理作业：

| 工具 | 用途 |
| --- | --- |
| `cron_create` | 创建并启用作业 |
| `cron_list` | 列出作业及当前 `revision`、`nextRunAt` |
| `cron_update` | 按 `expected_revision` 比较并更新 |
| `cron_pause` / `cron_resume` | 暂停或恢复未来触发 |
| `cron_delete` | 删除作业定义；历史运行记录保留 |
| `cron_run_now` | 立即申请一次手动运行 |
| `cron_runs` | 查看有界运行历史与结构化回执 |

`cron_list` 和 `cron_runs` 可直接读取；其余六个写工具均采用两轮确认：

1. Agent 第一次提交经过完整校验的写请求时，工具会拒绝执行并返回一个 `DSH_CRON_CONFIRM_v1_...` token。
2. 你需要在下一条顶层人类消息中原样包含该 token；Agent 再用完全相同的规范化参数调用工具，并把 token 放入 `confirmation_token`，操作才会执行。

token 与 Root Session、具体工具名和规范化后的全部写参数绑定，且成功放行后立即失效。把 token 只放进同一轮的工具参数、模型回复、网页内容、工具输出，或改变任何参数，都不能完成确认。若运行时随后因 revision 过期等原因拒绝，本次 token 也不会复用，需要重新发起两轮确认。待确认 challenge 只保存在当前 Host 进程内；Host 重启后旧 challenge 失效，需要重新完成“发起—确认”两轮。

除创建外，修改操作还要先用 `cron_list` 读取最新 revision。过期 revision 会失败，避免两个会话互相覆盖。这个确认门要求人工复制 token，因此不适合把 Cron 管理本身做成无人值守自动化；计划执行仍然是无人值守的。

Cron 示例：

```text
0 9 * * 1-5      工作日上午 09:00
30 18 * * *      每天 18:30
0 */4 * * *      每四小时整点
15 10 1 * *      每月 1 日 10:15
```

本实现采用严格模式，不允许同时限定 day-of-month 与 day-of-week；不接受秒字段、`@daily` 等宏、固定 UTC offset 或 `PST` 之类缩写。

## 配置

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `databasePath` | `$DSH_HOME/cron/cron.sqlite` | Host 自有 SQLite 文件 |
| `defaultTimeZone` | `UTC` | `UTC` 或 IANA Area/Location |
| `defaultCwd` | 创建会话的工作目录 | 可选绝对路径 |
| `defaultAgentPreset` | `standard` | 每次触发挂载的 Agent Preset |
| `defaultPermissionPreset` | `workspace-write` | 每次触发固定的权限 Preset |
| `defaultMisfirePolicy` | `latest` | `latest` 或 `skip` |
| `allowedTools` | 编程所需的窄 allowlist | Runner 可见的宿主工具 |
| `maxConcurrentRuns` | `1` | 单 Host 并发，范围 1–16 |
| `maxRuntimeMs` | `3600000` | 每次运行默认超时 |
| `heartbeatIntervalMs` | `30000` | SQLite 心跳间隔，最小 1000 ms |

Bundle 的 patch 配置是整对象替换语义。覆盖 `cron` 行时，应重写希望保留的完整 `config`，不要假设字段会深度合并。

官方 `standard` Preset 默认提供 `web_search`，但没有启用 `web_fetch`。只有在 Profile 已安装 Fetch Provider 且 Preset 确实注册了 `web_fetch` 后，才能把它加入 `allowedTools`；Harness 会拒绝 allowlist 中不存在的工具名。

## 运行语义

- Job 创建时计算并持久化下一次 UTC 触发时间。
- Scheduler 在同一 SQLite 事务中推进 `nextRunAt` 并领取 occurrence，避免重复领取。
- `overlapPolicy` 在 v0.1 固定为 `forbid`；重叠 occurrence 会记为 `skipped`，计划仍继续推进。
- `misfirePolicy=latest` 在停机跨度内只执行最近一次；`skip` 跳过已经错过的 occurrence。
- 每次 occurrence 都有确定性的 Run ID 与 Session ID。Runner 必须调用 `cron_complete` 写入 `succeeded`、`failed` 或 `needs_review` 回执；普通文本回复不算完成。
- 超时、Host 停机、没有回执或异常结束会以保守的非成功状态记录。

v0.1 只支持一个持续运行的 Host 进程共享该数据库。崩溃发生在外部副作用与最终回执之间时无法做分布式 exactly-once；任务应尽量使用幂等写入、目标端幂等键或“先检查后写”的方式。

暂停只阻止未来 occurrence，不会取消已经领取的运行。删除 Job 也不会删除其审计历史。

## macOS 常驻与健康检查

[`launchd/ai.deepseek.harness.cron.plist.example`](./launchd/ai.deepseek.harness.cron.plist.example) 是 macOS 模板。先把所有 `/ABSOLUTE/...` 替换为真实绝对路径，尤其是 `dsh`、工作目录、`DSH_HOME`、日志目录和包含 `node` 的 `PATH`。

```bash
plutil -lint ./launchd/ai.deepseek.harness.cron.plist.example
mkdir -p /ABSOLUTE/PATH/TO/LOGS
mkdir -p ~/Library/LaunchAgents
cp ./launchd/ai.deepseek.harness.cron.plist.example \
  ~/Library/LaunchAgents/ai.deepseek.harness.cron.plist
launchctl bootstrap "gui/$(id -u)" \
  ~/Library/LaunchAgents/ai.deepseek.harness.cron.plist
```

`launchd` 不会替你创建 `StandardOutPath` / `StandardErrorPath` 的父目录；上面的日志目录必须与替换后的 plist 路径一致，并在 `bootstrap` 前存在。

检查 Host 心跳：

```bash
node /absolute/path/to/dsh-cron/bin/dsh-cron-health.mjs \
  "${DSH_HOME:-$HOME/.dsh}/cron/cron.sqlite" 90
```

输出 JSON 且退出码为 0 表示最近 90 秒内有心跳；数据库、心跳缺失或过期时退出码为 1。此检查只能证明 Scheduler 进程存活，不代表某个具体 LLM Provider、网络或业务任务成功；业务结果应看 `cron_runs`。

## 开发验证

```bash
pnpm run typecheck
pnpm run test
pnpm run build
pnpm pack
```

许可证：MIT。
