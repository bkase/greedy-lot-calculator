open Lib

let () =
  let _h = Heap.create Lot_entry.compare in
  Core.printf "Hello, world!"
