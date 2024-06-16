open Core
open Lib

let () =
  let argv = Sys.get_argv () in
  let args = Array.sub argv ~pos:1 ~len:(Array.length argv - 1) in
  let config = Config.read_config args.(0) in
  let simples, mores = Config.load config in
  let simple_lots =
    List.map simples ~f:(List.map ~f:Lot_entry.Parse.of_simple)
  in
  let more_lots = List.map mores ~f:(List.map ~f:Lot_entry.Parse.of_more) in

  let input =
    Demux.demux ~compare:Lot.Compare_earliest.compare
      ( simple_lots @ more_lots
      |> List.map ~f:(List.map ~f:(fun e -> Lot.Static e)) )
  in
  Core.printf !"%{sexp: Lot.t list}\n" input
