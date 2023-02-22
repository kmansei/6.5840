# 課題
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

## 設計
<div align="center">
<figure>
  <img
  src="map%20phase.png"
  alt="map-phase.png">
  <figcaption>map phase</figcaption>
</figure>
</div>

- RPCのargs, replyの型はMRTaskを使用する
- Coordinatorが公開しているGetTaskをそれぞれのworkerが呼び出し、タスクをもらう
    - Map, Reduce, Sleep, Exitのどれか
    - タスクをアサイン時点で対応するmapタスクのStatus, Timeを更新
- MapやReduceタスクの完了報告もGetTaskの初めに行う。
    - Argsが前回workerが取り組んだタスクの情報を持つ
- CoordinatorはmapRemain(残りの完了していないmapタスク数)が0になるまで、reduceタスクをアサインせず、workerからgetTaskを呼ばれたらsleepさせる
- CoordinatorはGetTaskを呼ばれた際に、状態がExecutingかつ10秒以上立っているタスクがあれば、それを別のworkerに割り振る
    - 同タスクを複数のworkerが並行して、行うことになるため一度tempファイルへの書き込みを行い、完了したら正しいファイル名に名前を変える
    - Reduce phase後に障害が起きたworkerのmapが始まり、reduceタスクが書き込み途中のファイルを読み込むのを防ぐため

## 躓いた点
- RPCで使用するstructのフィールドは大文字でないと
`gob: type mr.MRTask has no exported field`
といったエラーが出る
    - Exportedというのはパッケージの外から参照できる変数のこと

## 参考
RPCパッケージについて  
https://pkg.go.dev/net/rpc#Server.RegisterName

Unix domain socketについて  
https://qiita.com/toshihirock/items/b643ed0edd30e6fd1f14
https://haibara-works.hatenablog.com/entry/2021/12/01/021218

Structの大文字、小文字について
https://qiita.com/Yarimizu14/items/e93097c4f4cfd5468259
