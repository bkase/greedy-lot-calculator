let cwd () = Sys.getcwd ()

open Core

module Config = struct
  type t =
    { csv_dir : string
    ; synthetic : string
    ; simple_csvs_basenames : string list
          (* all_sends is the union of basis_non_zero_no_tax + basis_zero_no_tax *)
    ; basis_zero_incoming : string
    ; basis_non_zero_incoming : string
    ; basis_non_zero_rewards : string list
    }

  (* for laziness assume, two simple csvs, then more csvs *)
  let parse row =
    match row with
    | csv_dir :: synthetic :: simple_out1 :: simple_out2 :: basis_zero_incoming
      :: basis_non_zero_incoming :: rest_rewards ->
        { csv_dir
        ; synthetic
        ; basis_zero_incoming
        ; simple_csvs_basenames = [ simple_out1; simple_out2 ]
        ; basis_non_zero_incoming
        ; basis_non_zero_rewards = rest_rewards
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
    let load' basename = Csv.load (t.csv_dir ^ basename) in
    let load'' names = List.map names ~f:load' in

    let simple_outs = load'' t.simple_csvs_basenames in
    let basis_non_zero_incoming = load' t.basis_non_zero_incoming in
    let basis_zero_incoming = load' t.basis_zero_incoming in
    let basis_non_zero_rewards = load'' t.basis_non_zero_rewards in
    let synthetic = load' t.synthetic in

    ( synthetic
    , [ ([ basis_zero_incoming ], `In (`Non_taxable `Z))
      ; (simple_outs, `Out)
      ; ([ basis_non_zero_incoming ], `In `Taxable)
      ; (basis_non_zero_rewards, `In `Taxable)
      ] )
end
