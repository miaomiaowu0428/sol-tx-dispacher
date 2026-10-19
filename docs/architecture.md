# sol-tx-dispacher 架构

> 状态：2026-10。描述**当前**实现，不是规划。

## 1. 定位与边界

**做**：把一批已构建好的指令，按目标 slot 的 leader 类型/地区，路由到一个或多个发送平台，并在超时内等待链上确认。

**不做**：

- 不构造业务指令（调用方给 `&[Instruction]`）
- 不管密钥/ALT 从哪来（`SendContext` 由调用方装配）
- 不做链上状态读取以外的业务判断

**依赖方向**：

```text
业务进程 (pumpswap-snipe / trade-solana-impl)
        │  只调 send* / bundle_sender
        ▼
sol-tx-dispacher ──► sol-tx-send      平台客户端（endpoint / 保活 / BuildTx / SendTx）
        │        ──► sol-slot-leader  slot → leader（client_type / name / region）
        │        ──► nonce_cache      交易确认（subscribe + confirm_tx）
        └────────► grpc-client        TransactionFormat
```

依赖是单向的：`sol-tx-send` 与 `sol-slot-leader` 都不知道 dispatcher 的存在。

## 2. 模块地图

| 文件 | 职责 |
|---|---|
| `lib.rs` | 门面：`SendRoute`、`resolve_route`、`ingress_for`、`send*` 系列、`keep_alive_targets` |
| `strategy.rs` | 各发送模式的编排（harmonic / fallback / jito / tip-only × tip / cost） |
| `fire.rs` | 单平台 fire-and-forget：build → 取 sig → `tokio::spawn` 发送 |
| `region_plan.rs` | `RegionPlan`（TOML）、`geo_to_region`、`RegionProfile::ingress_for` |
| `region_pool.rs` | `RegionPool<T>`（多地区实例池）+ `SEND_REGION` task-local |
| `config.rs` | `TxDispacher::from_config` / `from_env`：按计划自建 client |
| `builder.rs` | 注入式构造（低层 API，测试与特殊场景） |
| `context.rs` | `SendContext`（payer / hash_param / ALT，ALT 按 key 去重） |
| `fifo.rs` | FIFO leader 白名单（`(vote account, client_type_id)`） |
| `bundle.rs` | 多平台 bundle 聚合（`MultiBundleSender` / `V1`） |

## 3. 一次发送的完整链路

```text
send_v1(ixs, ctx, target_slot, SpendConfig, timeout)
  │
  ├─ resolve_route(slot)      ──► SendRoute（Harmonic / Jito / P2pTurbo / TipOnly / Fallback）
  ├─ is_fifo_leader(slot)     ──► 命中则 tip=None、cu_price=None（不参与竞价）
  ├─ ingress_for(slot)        ──► Region（leader 地区 ∈ fanout ? 该地区 : own）
  │
  └─ with_region(ingress, dispatch(...))
        │
        ├─ harmonic_mode / fallback_mode / jito_mode / tip_only_auto
        │     └─ fire_client(&pool, ...)   ← 每个平台一次
        │           ├─ build_v1_tx（借用 client，取 sig）
        │           ├─ current_region() 取到 ingress
        │           └─ tokio::spawn { with_region(r, pool.send_tx(tx)) }
        │                                   └─ RegionPool::get(r) → 具体地区实例
        └─ confirm_tx(rx, sigs, timeout)   ← 等第一个确认
```

**一个 nonce ⇒ 所有平台发的是同一份字节、同一个签名 ⇒ 链上只会成功一次、只付一次费。** 这是"全平台广播"安全的前提，重复投递同理（见 §6）。

## 4. 路由决策

`resolve_route(slot)` 的**顺序**（先命中先返回）：

1. **tip-only 白名单**：`TIP_ONLY_LEADERS`（按 vote account）→ `SendRoute::TipOnly`
2. **P2P.org Turbo**：`name == "P2P.org Turbo"` → `SendRoute::P2pTurbo`（Fallback + Syncro）
3. **Harmonic**：`client_type` 是 Harmonic 变体，或 `name` 含 `harmonic` → `SendRoute::Harmonic`
4. **Jito**：`client_type == JitoLabs` → `SendRoute::Jito`
5. 其余（含 DB 无记录 / NoopOracle）→ `SendRoute::Fallback`

**FIFO leader**（`fifo.rs`）与 route 正交：命中时强制 `tip=None`（下游折成平台最低）+ `cu_price=None`，因为这类 leader 先到先得、不看竞价。

**入口地区**（`ingress_for`）：

| 情况 | 入口地区 |
|---|---|
| leader 地区 ∈ `fanout` | 该地区（就近网关） |
| leader 在别洲 / 不在 fanout | `own_region` |
| DB 无地区 / `geo_to_region` 识别不出 | `own_region` |

## 5. 发送模式与 tip 语义

| 模式 | 发什么 | tip |
|---|---|---|
| `harmonic_mode` | Harmonic 直发 ×3（`HARMONIC_FIRE_TIMES`）+ **全平台** `fire_no_price` | Harmonic 不收 tip；Astralane(_Quic)/Temporal 按 90%；其余走 `tip_strategy` |
| `fallback_mode` | 全平台，按「both / with_price / no_price」三分组 | 带 price 的 `Ratio(1.05)`，纯 tip 的走 `tip_strategy` |
| `jito_mode` | 只发 tip 笔的子集：Astralane(_Quic)、FlashBlock、Temporal、ZeroSlot、Jito | `tip_strategy` |
| `tip_only_auto` | 全平台并发、只发 tip（`fire_all_parallel`，Harmonic 场景下跳过 Temporal） | 平台最低或调用方指定 |
| `*_cost_*` | 同上，但拆成「cu_price 笔」+「纯 tip 笔」 | price 笔 `Ratio(tip_rate)`，tip 笔 `Absolute(cost_amount)` |

`fallback_mode` 的平台分组：

- **`fire_both`**（tip + cu_price 各发一笔）：Astralane、AstralaneQuic、FlashBlock、Temporal、ZeroSlot、Syncro（Syncro 只在 `P2pTurbo` 路由追加）
- **`fire_with_price`**（只发 cu_price 笔，tip 仅保底 `Ratio(1.05)`）：EverStake、EverStakeQuic、NodeOne、Blockrazor、HeliusMax、HeliusSwqos、NextBlock、Stellium、LandX
- **`fire_no_price`**（只发 tip 笔）：Jito（bundle 靠 tip 排序，cu_price 无意义）

**tip 语义三种，别混**：

| 变体 | 含义 |
|---|---|
| `Absolute(n)` | 竞价意图 = n，**落地时会抬到平台最低** |
| `Ratio(r)` | 平台最低 × r |
| `Exact(n)` | 严格 n，**不抬不缩**（可能低于平台门槛被丢弃，调用方明确要求） |

两个入口对 tip 的表达不同，别对着文档猜：

- **V1 `SpendConfig`**：`SpendConfig::tip(n, cu)` → `SpendMode::FixedTip` → `TipStrategy::Exact(n)`，即**严格 n，不抬不缩**（低于平台门槛会被丢弃，这是刻意的）；`SpendConfig::cost(n, cu)` → 拆成 cu_price 笔 + 纯 tip 笔。
- **V0 `send(.., tip_strategy, ..)`**：由调用方直接给 `TipStrategy`，`Absolute(n)` 会被抬到平台最低，`Exact(n)` 不会。

## 6. 多地区架构

### 6.1 配置：`config/RegionPlan.toml`

```toml
[[profiles]]
own_region = "Frankfurt"
fanout = ["Frankfurt", "Amsterdam", "Limburg", "Lithuania"]   # 顺序 = 优先级
```

- **编译期内嵌**（`include_str!`），部署不依赖额外文件；`REGION_PLAN_PATH` 可运行时覆盖
- 只配「本机要开哪些入口地区」，**不写平台名**——平台能力由各 client 的 `endpoint_for(region)` 决定
- 启动期校验：版本、`own_region` 唯一且非 Unknown、`fanout` 非空/无重复/含 `own_region`、地区名必须合法
- 不跨洲：跨洲的 leader 一律走本机入口

### 6.2 构造：`from_config`

```text
for 平台 in 环境变量里有凭据的:
    pool = RegionPool::build(profile, |region| 平台::init_with(key, region))
```

- 凭据是**部署级**的（一个 key 服务所有地区），先校验再用；空串视为未配置
- 平台缺某地区机房时，`init_with` 内部就近回退，**不会失败**（见 §6.4）
- QUIC（AstralaneQuic / EverStakeQuic）与 Harmonic 不参与 fanout：前者单地区，后者多端点广播

### 6.3 选择：task-local `SEND_REGION`

`SendTx::send_tx(&self, tx)` 不带上下文，而发送发生在 `tokio::spawn` 出来的 task 里。**task-local 不会跨 spawn 继承**，所以：

```rust
// 入口处设置
with_region(ingress, dispatch(...)).await
// fire 时取出，再在新 task 里重新 scope
let region = current_region();
tokio::spawn(async move { with_region(r, pool.send_tx(&tx)).await });
```

没设置 hint 时 `RegionPool` 一律回退 `own`，因此单独调 `send_tx` 的行为与单实例完全一致。

### 6.4 退化规则（在 `endpoint_for` 里，每平台一张表）

1. 精确匹配该地区
2. 同洲按平台表内优先级取第一个
3. 兜底 **Frankfurt**（所有平台都有 FRA 入口）
4. `Unknown` → Frankfurt

**不做去重**：两个目标地区退化成同一 endpoint 就是两个实例。多开一个 HTTP client 的代价可忽略（共享全局连接池），换来的是热路径只需一次 HashMap 查找。

### 6.5 保活

`keep_alive_targets()` 遍历**池里每个地区**，取该平台的保活 URL，再按 URL 去重。默认 20s 一轮、首轮错峰、`MissedTickBehavior::Delay`。

- 大多数平台直接用 `endpoint_for(region)`；
- **LandX / Helius 用官方 `GET <origin>/ping`**（Helius 不能用提交端点 `.../fast` 探测；Max 与 SWQOS 共用 origin，去重后每个地区只探一次）。

必须比 `HTTP_CLIENT` 的 `pool_idle_timeout`（90s）短，否则连接刚被淘汰又重建。

### 6.6 单地区时完全等价旧版

`builder.xxx(client)` 会包成 `RegionPool::single(*REGION, client)`，`region_profile` 默认 `RegionProfile::single(*REGION)`，`ingress_for` 恒等于 own，`Display` 在单实例时输出与旧版逐字节一致。

## 7. 平台矩阵

| 平台 | 参与 fanout | 传输 | 备注 |
|---|---|---|---|
| Astralane / AstralaneQuic | ✅ / ❌ | HTTP / QUIC | 11 个地区；QUIC 单地区 |
| Temporal | ✅ | HTTP | 无 Limburg/Lithuania |
| NodeOne | ✅ | HTTP | 8 地区 |
| Blockrazor | ✅ | HTTP | 仅 FRA/NY/TYO/AMS |
| FlashBlock | ✅ | HTTP | 无 Tokyo（回退 Singapore） |
| ZeroSlot | ✅ | HTTP | 无 London（回退 AMS） |
| NextBlock / Stellium | ✅ | HTTP | 有 SLC |
| Jito | ✅ | HTTP | bundle，只发 tip 笔 |
| HeliusMax / HeliusSwqos | ✅ | HTTP | 同一 key；保活用官方 `/ping` |
| LandX | ✅ | HTTP | 官方 `/ping` 做保活 |
| Syncro | ✅ | HTTP | 仅 `P2pTurbo` 路由；无 key 不注册 |
| EverStake / EverStakeQuic | ✅ / ❌ | HTTP / QUIC | 无需 key |
| Harmonic | ❌ | gRPC | 多端点并发广播 |

## 8. 配置与环境变量

| 变量 | 作用 |
|---|---|
| `REGION` | 本机地区，决定选哪个 profile 与 `own_region`，默认 `Frankfurt` |
| `REGION_PLAN_PATH` | 覆盖内嵌的 `RegionPlan.toml` |
| `KEEP_ALIVE_INTERVAL_SECS` | 保活间隔，默认 20 |
| `SLOT_LEADER_DB_URL` / `DB_URL_NODE1` | slot leader 库（由应用层构造 oracle） |
| `ASTRALANE_KEY` / `TEMPORAL_KEY` / `NODEONE_KEY` / `BLOCKRAZOR_KEY` / `FLASHBLOCK_KEY` / `ZEROSLOT_KEY` / `NEXTBLOCK_TOKEN` / `STELLIUM_API_KEY` / `LANDX_API_KEY` / `SYNCRO_API_KEY` / `HELIUS_KEY` / `JITO_UUID` | 各平台凭据；缺失则**不注册该平台** |
| `EVER_STAKE_QUIC_KEYPAIR` / `HARMONIC_SEARCHER_PRIVATE_KEY_BASE58` | QUIC / gRPC 凭据 |

## 9. 新增一个发送平台

按顺序改，缺一处就会出现"能建但不能发"或"发了但没保活"：

1. **`sol-tx-send`**：`endpoint_for(Region) -> &'static str`（穷举、无 `_` 分支）+ `init_with(key, region)`
2. **feature**：`Cargo.toml` 的 `[features]` 与 `default`
3. **dispatcher 字段**：`TxDispacher` 加 `Option<Arc<RegionPool<T>>>`
4. **builder**：字段 + `xxx(client)`（包 single）+ `xxx_pool(pool)`
5. **`strategy.rs`**：在目标模式里加一行 `fire_both!` / `fire_with_price!` / `fire_no_price!`
6. **`keep_alive_targets`**：加一个遍历 `pool.regions()` 的块
7. **`config.rs`**：加一段 `env_key(...)` + `RegionPool::build`
8. **测试**：`region_fallback_tests` 里补一条不变量（不能映射空串、`Unknown == Frankfurt`）

## 10. 设计取舍与已知限制

- **重复投递是安全的**：nonce 固定 ⇒ 同签名 ⇒ 链上只接受一次。所以 Harmonic 敢连发 3 次、fallback 敢全平台广播。
- **不做地区去重**：见 §6.4，用实例数换热路径简单。
- **QUIC 未多地区**：QUIC 多地区要引入重连 supervision + ready 位图，收益（欧洲内个位数 ms）暂不抵复杂度。
- **`region` 是节点机房位置**，不是投票账户所有者所在地；解析在 `geo_to_region`，认不出就回退本机，绝不跨洲猜。
- **`trade-solana-impl` 暂不在 workspace**（solana-sdk 3.0.0），其 `init_tx_dispacher` 仍是旧的逐平台注入写法，合回时需要同步。
- **应用层仍有两份 client**：dispatcher 自建的池 + `constants::*_CLIENT` 静态单例（`send_utils/prebuilt.rs`、`migrate_v2.rs`、`all_tip_recvs()` 还在用后者）。

## 11. 可观测性

| 日志 | 含义 |
|---|---|
| `[TxDispacher] 地区计划：own=... fanout=[...]` | 启动时选中的 profile |
| `[fire] 🚀 <client> sending <sig>` | 某平台已构建并发起发送；`<client>` 形如 `Astralane[4x]` 表示多地区池 |
| `📤 [send] platform=... 构建耗时 / 发送耗时` | 单平台耗时，排查慢平台 |
| `[fallback_mode] fired N tx(s)` | 本轮实际发起的平台数 |
| `[keep-alive] <平台> <url> -> <状态> (<ms>)` | 保活探测；>50ms 会 warn |
| `[RegionPlan] 没有 own_region=... 的 profile` | `REGION` 与配置对不上，退化成单地区 |

**排查顺序**：先看 `地区计划` 是否符合预期 → 看 `fired N`（平台是否注册）→ 看 `send failed`（凭据/端点）→ 看 `keep-alive` 是否慢。
