let cwd () = Sys.getcwd ()

open Core

module Config = struct
  type t =
    { csv_dir : string
    ; simple_csvs_basenames : string list
    ; more_csvs_basenames : string list
    }

  (* for laziness assume, two simple csvs, then more csvs *)
  let parse row =
    match row with
    | csv_dir :: simple1 :: simple2 :: rest ->
        { csv_dir
        ; simple_csvs_basenames = [ simple1; simple2 ]
        ; more_csvs_basenames = rest
        }
    | _ ->
        failwith "Parse error, unexpected config format"

  let read_config filename =
    match Csv.load filename with
    | x :: [] ->
        parse x
    | _ ->
        failwith "Unexpected format for config.csv"

  let load t =
    let load' names =
      List.map names ~f:(fun basename -> Csv.load (t.csv_dir ^ basename))
    in

    let simples = load' t.simple_csvs_basenames in
    let mores = load' t.more_csvs_basenames in
    (simples, mores)
end
