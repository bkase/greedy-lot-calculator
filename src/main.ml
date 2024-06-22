open Core
open Lib

let () =
  let argv = Sys.get_argv () in
  let args = Array.sub argv ~pos:1 ~len:(Array.length argv - 1) in
  let config = Config.read_config args.(0) in
  let synthetic, rest = Config.load config in
  let synthetic_timings = List.map synthetic ~f:Timing.Parse.parse in
  let synthetic_lots =
    List.map synthetic_timings ~f:(fun timing ->
        Lot.Vesting (timing, Timing.initial_entry timing) )
  in
  let rest_lots =
    List.bind rest ~f:(fun (csvs, direction) ->
        match direction with
        | `Out ->
            List.map csvs ~f:(List.map ~f:Lot_entry.Parse.of_simple)
        | `In taxable ->
            List.map csvs ~f:(List.map ~f:(Lot_entry.Parse.of_more ~taxable)) )
  in
  let input =
    Demux.demux ~compare:Lot.Compare_earliest.compare
      ( synthetic_lots
      :: (rest_lots |> List.map ~f:(List.map ~f:(fun e -> Lot.Static e))) )
  in
  Core.printf !"%{sexp: Lot.t list}\n" input ;
  let report = Run.run input in
  Core.printf !"%{sexp: string list}\n" report
