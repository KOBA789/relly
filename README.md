# relly

![](docs/header.png)

**:warning: これは [liumOS](https://github.com/hikalium/liumos) 向けに移植されたコードのブランチです :warning:**

「WEB+DB PRESS Vol. 122『特集3 作って学ぶ RDBMS のしくみ』」で取り扱っているコードについては [wdb ブランチ](https://github.com/KOBA789/relly/tree/wdb) を参照してください。

## relly とは

relly は RDBMS のしくみを学ぶための小さな RDBMS 実装です。

## liumOS 向けの移植作業の動画

2021/04/30に、liumOS の作者である [hikalium](https://github.com/hikalium) さんと共同で、この relly を liumOS に移植する様子をライブ配信しました。

[![](docs/on-liumos.png)](https://www.youtube.com/watch?v=qLZAkj4XfIw)

[自作RDBMSを自作OSの上で動かしてみよう](https://www.youtube.com/watch?v=qLZAkj4XfIw)

## 動かし方

まず、liumOS をビルドして実行できる環境を整えてください。

次に、このリポジトリを liumOS のソースツリーの `app/` 以下に clone してください。ディレクトリ構成は次のようになります。

```
$ pwd
/path/to/liumos
$ tree -L 1 app
app
├── Makefile
(中略)
├── relly
(中略)
└── udpserver

17 directories, 1 file
```

正しい位置に relly のコードを配置できたら、`app/Makefile` を次のように書き換えます

```patch
--- a/app/Makefile
+++ b/app/Makefile
@@ -10,6 +10,7 @@ APPS=\
         pi/pi.bin \
         ping/ping.bin \
         readtest/readtest.bin \
+        relly/relly.bin \
         rusttest/rusttest.bin \
         shelium/shelium.bin \
         udpclient/udpclient.bin \
```

この状態で liumOS をビルドすれば、relly も一緒にビルドされます。

liumOS を起動して、`relly.bin` を実行できるはずです。
