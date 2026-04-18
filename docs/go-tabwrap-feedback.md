# go-tabwrap に対するフィードバック・要望（spannerplan）

このリポジトリでは [github.com/apstndb/go-tabwrap](https://github.com/apstndb/go-tabwrap) **v0.1.3** を利用している。用途は主に次のとおり。

- `tabwrap.StringWidth` — ASCII ツリー装飾と幅の整合（`internal/treerender` のプレフィックス幅、`PrefixMetrics`）
- `tabwrap.Condition` と `Wrap` — クエリプランノードタイトルの折り返し（`plantree`）。既定の `Condition` では `TrimTrailingSpace: true`（v0.1.3 で追加）を有効にしている
- `tabwrap.FillLeft` — 述語ブロックの ID 列の右寄せ埋め（`plantree/reference`）

## 要望（優先度順）

### 1. ~~折り返し結果の行末空白を制御したい~~（v0.1.3 で対応）

**状況:** `Condition.Wrap` の結果に、改行直前の行末にスペースやタブが残ることがあった。

**解消:** v0.1.3 の `Condition.TrimTrailingSpace` を使用。呼び出し側の手動 `trimWrappedLinesRight` は削除した。

### 2. `StringWidth` と装飾文字列の前提の明文化

**状況:** ツリー用 `Style` の各フィールド（`EdgeLink` / `EdgeMid` など）の表示幅に `StringWidth` を使い、レンダラと折り返しバジェットを一致させている。

**要望:**

- `StringWidth` が **どの範囲の Unicode（東アジア幅、結合文字など）をどう数えるか** を godoc に短く書いておくと、カスタム `Style` を使う利用者が安全に選べる。（v0.1.3 で README / godoc が拡充されているが、引き続き追従したい。）

### 3. （任意）幅計算の再利用 API

**状況:** 同一スタイルに対して `StringWidth` をノードごとに繰り返さないよう、`PrefixMetrics` や `styleWidths` 側でキャッシュしている。

**要望:**

- ライブラリ側に **スタイル文字列の幅をまとめてキャッシュするヘルパー**（例: `type WidthCache struct` と `StringWidth` の薄いラッパー）があると、似たパターンの利用者に便利。必須ではない。

## `FillLeft` について

述語の ID 列整形での利用に問題は出ていない。幅の定義が `StringWidth` と一貫していることだけ、ドキュメントで分かると安心である。

---

*最終更新: go-tabwrap v0.1.3 取り込み時。upstream の issue 化する際は必要に応じて英訳・再編すること。*
