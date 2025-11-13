# BenchFS Mini Docker Test Environment

このディレクトリには、Stream RPC問題をデバッグするためのBenchFS Miniの最小構成Docker環境が含まれています。

## 概要

BenchFS Miniは、既存のBenchFSで発生しているStream RPC接続問題をデバッグするために作成された、極めて簡素化した実装です。このDocker環境は、IORベンチマークを使ってBenchFS Miniをテストするための環境を提供します。

## アーキテクチャ

### クラスタ構成

- **サーバーノード**: 2台 (server1, server2)
  - MPI Rank: 0-1
  - IPアドレス: 172.21.0.11-12
- **クライアントノード**: 2台 (client1, client2)
  - MPI Rank: 2-3
  - IPアドレス: 172.21.0.21-22
- **コントローラノード**: 1台 (controller)
  - IPアドレス: 172.21.0.10
  - MPI実行とテスト実行の起点

### ストレージ

- **shared_registry_mini**: サービスディスカバリ用の共有レジストリ
- **server1_data, server2_data**: 各サーバーの独立したデータボリューム
- **controller_results**: テスト結果保存用

## 前提条件

1. **ベースイメージのビルド**

   docker-miniを使用する前に、UCX/MPIベースイメージをビルドする必要があります:

   ```bash
   cd ../docker
   make build-base
   ```

2. **benchfsd_miniバイナリ**

   benchfsd_miniバイナリは、Makefileの`build-binary`ターゲットで自動的にビルドされます。

## 使用方法

### 1. イメージのビルド

```bash
cd tests/docker-mini
make build-mini
```

このコマンドは:
- benchfsd_miniバイナリをビルド
- BenchFS Miniライブラリをコンパイル
- IORをBENCHFSMINIバックエンド付きでビルド
- Dockerイメージ`benchfs-mini:latest`を作成

### 2. クラスタの起動

```bash
make up
```

これにより:
- 5つのコンテナが起動(server1, server2, client1, client2, controller)
- ネットワークとボリュームが作成

### 3. IORテストの実行

```bash
make test-ior
```

このコマンドは:
- コントローラコンテナ内で`run_ior_mini.sh`スクリプトを実行
- MPIを使用して4ランク(2サーバー + 2クライアント)でIORを起動
- BENCHFSMINIバックエンドを使用してI/Oテストを実行
- 詳細なログを出力

### 4. クラスタの停止

```bash
make down
```

### 5. クリーンアップ

```bash
# コンテナとボリュームを削除
make clean

# イメージも含めて全てクリーンアップ
make clean-all
```

## デバッグ

### コントローラシェルへのアクセス

```bash
make shell
```

### レジストリファイルの確認

```bash
make show-registry
```

サーバー登録情報を表示します。

### ログの確認

```bash
make logs
```

### クラスタステータスの確認

```bash
make status
```

## IOR テストパラメータ

デフォルトの設定(`run_ior_mini.sh`):

- **転送サイズ**: 1 MB
- **ブロックサイズ**: 4 MB
- **セグメント数**: 2
- **ファイルモード**: File-per-process (-F)
- **操作**: Write + Read

パラメータを変更するには、`scripts/run_ior_mini.sh`を編集してください。

## ディレクトリ構造

```
tests/docker-mini/
├── Dockerfile.mini           # BenchFS Mini Dockerイメージ定義
├── docker-compose.yml        # クラスタ構成
├── Makefile                  # ビルドとテストターゲット
├── README.md                 # このファイル
└── scripts/
    └── run_ior_mini.sh       # IORテストスクリプト
```

## トラブルシューティング

### ベースイメージが見つからない

エラー: `ERROR: Base image benchfs/ucx-mpi-base:1.0 not found!`

解決策:
```bash
cd ../docker
make build-base
```

### SSH接続エラー

コンテナ間でSSH接続に失敗する場合:

1. コンテナが起動しているか確認:
   ```bash
   make status
   ```

2. SSHサービスが動作しているか確認:
   ```bash
   docker exec benchfs-mini_controller ssh server1 echo OK
   ```

### IORテストが失敗する

1. デバッグログを有効化(既に有効):
   - `RUST_LOG=debug`
   - `UCX_LOG_LEVEL=info`

2. レジストリファイルを確認:
   ```bash
   make show-registry
   ```

3. コントローラに入って手動実行:
   ```bash
   make shell
   /scripts/run_ior_mini.sh
   ```

## 既知の問題

- **Stream RPC接続**: これがデバッグの主な目的です。エンドポイント情報がログに出力されるので、既存のBenchFSと比較してください。

## 次のステップ

テストが成功した場合:
- エンドポイント情報を既存のBenchFSと比較
- 動作する設定を既存コードに適用

テストが失敗した場合:
- エンドポイント情報から問題箇所を特定
- UCX設定を調整
- さらに簡素化したテストケースを作成

詳細は`claude_log/benchfs_mini_plan.md`を参照してください。
