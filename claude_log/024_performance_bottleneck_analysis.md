# ベンチマーク性能問題の分析と診断

**日付**: 2025-10-18
**ブランチ**: feature/chfs-implementation
**ステータス**: 診断中 - サーバー側RPC通信の問題を特定

## 問題の概要

Small benchmarkの結果、以下の深刻な性能問題が確認された:

- **Metadata Create**: 377ns/iter (正常)
- **Small Write (1-16KB)**: **300ms/iter** (異常に遅い)
- **Small Read (1-16KB)**: **45ms/iter** (異常に遅い)

## Perfデータ分析結果

### 主要なボトルネック

1. **トレーシングオーバーヘッド** (最大の問題 - 修正済み)
   - `tracing_core::metadata::LevelFilter::current`: ~98億サンプル
   - レベルチェック: ~87億サンプル
   - **対策**: ランタイムループ内の全`tracing::trace!()`を削除

2. **時刻取得オーバーヘッド** (2番目 - 修正済み)
   - `std::time::Instant::now`: ~277億サンプル
   - **対策**: 統計収集の時刻取得を削除

3. **UCXリアクターポーリング** (修正済み)
   - `UCXReactor::poll`: ~72億サンプル
   - `bool::then`: ~85億サンプル
   - **対策**: `bool::then()`を明示的なmatchとearly returnに変更

4. **UCXのepoll_wait** (根本原因と思われる)
   - `newidle_balance`: 636億サンプル
   - `ucs_event_set_wait;epoll_wait`: 多数のサンプル
   - **問題**: サーバー側からの応答がないため、クライアント側がタイムアウトまで待機している

## 実施した最適化

### 1. プルビオランタイムの最適化 (`pluvio_runtime/src/executor/mod.rs`)

```rust
// 修正前: トレーシング呼び出しと時刻取得が多数
for (id, reactor_wrapper) in self.reactors.borrow().iter() {
    if reactor_wrapper.enable.get() {
        tracing::trace!("Polling reactor: {}", id);
        if let ReactorStatus::Running = reactor_wrapper.reactor.status() {
            let now = std::time::Instant::now();
            reactor_wrapper.reactor.poll();
            self.stat.add_pool_and_completion_time(now.elapsed().as_nanos() as u64);
            // ...
        }
    }
}

// 修正後: トレーシングと時刻取得を削除
for (_id, reactor_wrapper) in self.reactors.borrow().iter() {
    if reactor_wrapper.enable.get() {
        if let ReactorStatus::Running = reactor_wrapper.reactor.status() {
            reactor_wrapper.reactor.poll();
            reactor_wrapper.poll_counter.set(reactor_wrapper.poll_counter.get() + 1);
        }
    }
}
```

### 2. UCXリアクターの最適化 (`pluvio_ucx/src/reactor.rs`)

```rust
// 修正前: bool::thenとイテレータ生成のオーバーヘッド
fn status(&self) -> ReactorStatus {
    self.registered_workers
        .borrow()
        .iter()
        .any(|(_, worker)| {
            matches!(worker.state(), WorkerState::Active | WorkerState::WaitConnect)
        })
        .then(|| ReactorStatus::Running)
        .unwrap_or(ReactorStatus::Stopped)
}

// 修正後: 明示的なmatchとearly return
fn status(&self) -> ReactorStatus {
    let workers = self.registered_workers.borrow();
    for (_, worker) in workers.iter() {
        match worker.state() {
            WorkerState::Active | WorkerState::WaitConnect => {
                return ReactorStatus::Running;
            }
            WorkerState::Inactive => {}
        }
    }
    ReactorStatus::Stopped
}
```

## 残存する重大な問題

### RPC通信の完全な失敗

**症状**:
- Write操作が300ms（正確にタイムアウト待ち）
- Read操作が45ms（理由不明）
- Metadata Create（ローカル操作）は正常（377ns）

**推定される根本原因**:

1. **サーバー側の`listen()`タスクがメッセージを受信していない**
   - `register_all_handlers()`で7つのRPCハンドラーを`runtime.spawn()`で起動
   - 各ハンドラーは`loop { stream.wait_msg().await }`で待機
   - しかし、UCXリアクターがメッセージを受信していない可能性

2. **タスクスケジューリングの問題**
   - 全タスクが`Pending`を返した後、`task_receiver`が空になる
   - リアクターのポーリングでタスクが再スケジュールされるはず
   - しかし、UCXがメッセージを受信しない限り、`wait_msg()`は完了しない

3. **UCX AM Streamの設定ミスの可能性**
   - クライアント: `endpoint.am_send_vectorized(rpc_id, ...)`
   - サーバー: `worker.am_stream(rpc_id).wait_msg().await`
   - Reply: `am_msg.reply(reply_stream_id, ...)`
   - クライアント応答待ち: `worker.am_stream(reply_stream_id).wait_msg().await`

**検証が必要な項目**:

1. サーバー側の`listen()`タスクが実際にスケジュールされているか
2. UCXリアクターが`worker.progress()`を呼び出しているか
3. クライアントのメッセージが正しいエンドポイントに送信されているか
4. RPC IDとreply stream IDが一致しているか（コード上は正しい）
5. `wait_msg()`のタイムアウト設定（300msがどこから来ているか）

## 次のステップ

### 優先度1: デバッグログの有効化

```bash
# サーバー側
RUST_LOG=debug cargo bench --bench small -- --mode server --registry-dir /tmp/registry --data-dir /tmp/server_data

# クライアント側
RUST_LOG=debug cargo bench --bench small -- --mode client --registry-dir /tmp/registry
```

### 優先度2: 修正候補

1. **サーバー側タスクのスケジューリング改善**
   ```rust
   // register_all_handlers()で spawn_polling()を使用
   runtime.spawn_polling(async move {
       if let Err(e) = server.listen::<ReadChunkRequest, _, _>(rt).await {
           tracing::error!("ReadChunk handler error: {:?}", e);
       }
   });
   ```

2. **リアクターポーリングの優先度向上**
   ```rust
   // run_queue()でリアクターを先にポーリング
   // タスクキューが空でもリアクターを確実にポーリング
   ```

3. **AM Streamの明示的な設定確認**
   - サーバー起動時にAM streamが正しく作成されているか確認
   - デバッグログで実際のRPC IDとstream IDを出力

### 優先度3: 代替実装の検討

- UCX RPCの代わりに、より単純な同期RPCメカニズムを試す
- サーバー側の`listen()`を別のアプローチで実装

## 修正したファイル

- `/home/rmaeda/workspace/rust/pluvio/pluvio_runtime/src/executor/mod.rs`
- `/home/rmaeda/workspace/rust/pluvio/pluvio_ucx/src/reactor.rs`

## 期待される改善（修正完了後）

トレーシングと時刻取得の最適化により、**20-30%の性能向上**を期待していたが、
RPC通信の根本的な問題により、まだ効果が確認できていない。

RPC通信問題の修正後:
- **Write**: 300ms → 1-10ms目標
- **Read**: 45ms → 1-10ms目標
- **Classicとの性能差**: 解消を目標

## 結論

Perfデータに基づくマイクロ最適化は完了したが、RPC通信の根本的な問題（サーバー側がメッセージを受信していない）により、性能改善の効果が現れていない。

**次の作業**: デバッグログを有効にして、RPC通信の実際の動作を確認し、根本原因を特定する必要がある。
