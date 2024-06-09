let cwd () = Sys.getcwd ()
open Core

let read dir =
  let ic = In_channel.create (dir ^ "/sample.csv") in
  let _csv = Csv.load_in ic in
  In_channel.close ic;
  (*Core.printf !"list: %{sexp: string list list}\n" csv*)
  ()

let%test_module "Csv loader" = (module struct
  let%test_unit "read sample" = read "../../../"
end)

