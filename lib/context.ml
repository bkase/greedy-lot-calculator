open Core

module Ctx = struct
  type t = int [@@deriving hash, sexp, compare]

  let run () = Core.printf "hello\n"
end
