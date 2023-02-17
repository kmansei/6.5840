# 課題
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

## 設計

## 躓いたところ
RPCで使用するstructのフィールドは大文字でないと
`gob: type mr.MRTask has no exported field`
といったエラーが出る

https://qiita.com/Yarimizu14/items/e93097c4f4cfd5468259

Exportedというのはパッケージの外から参照できる変数
https://go.dev/tour/basics/3

## 参考
RPCパッケージについて
https://pkg.go.dev/net/rpc#Server.RegisterName

Unix domain socketについて
https://qiita.com/toshihirock/items/b643ed0edd30e6fd1f14
https://haibara-works.hatenablog.com/entry/2021/12/01/021218

スライスとappendを用いてQueueを実装する方法
https://qiita.com/ruiu/items/8edf22cd5fc8f7511687


