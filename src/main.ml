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
            List.map csvs ~f:(fun l ->
                List.map ~f:Lot_entry.Parse.of_simple l
                |> List.sort ~compare:Lot_entry.Compare_earliest.compare )
        | `In taxable ->
            List.map csvs ~f:(fun l ->
                List.map l ~f:(Lot_entry.Parse.of_more ~taxable)
                |> List.sort ~compare:Lot_entry.Compare_earliest.compare ) )
  in
  let input =
    Demux.demux ~compare:Lot.Compare_earliest.compare
      (rest_lots |> List.map ~f:(List.map ~f:(fun e -> Lot.Static e)))
  in
  let synthetics =
    match synthetic_lots with
    | [] ->
        failwith "unexpected synthetics"
    | [ _x ] ->
        failwith "unexpected synthetics"
    | [ x; y ] ->
        (x, y)
    | _ ->
        failwith "unexpected synthetics"
  in
  let report = Run.run ~synthetics input in

  Csv.save "/tmp/report.csv"
    ( [ "id"
      ; "date"
      ; "price"
      ; "amount"
      ; "dollars"
      ; "income"
      ; "witheld"
      ; "short_gains"
      ; "long_gains"
      ; "ids_consumed"
      ; "income_by_month"
      ; "income_by_quarter"
      ; "income_by_year"
      ; "income_by_all"
      ; "witheld_by_month"
      ; "witheld_by_quarter"
      ; "witheld_by_year"
      ; "witheld_by_all"
      ; "short_gains_by_month"
      ; "short_gains_by_quarter"
      ; "short_gains_by_year"
      ; "short_gains_by_all"
      ; "long_gains_by_month"
      ; "long_gains_by_quarter"
      ; "long_gains_by_year"
      ; "long_gains_by_all"
      ]
    :: report ) ;
  Core.printf "Done written to /tmp/report.csv\n"
