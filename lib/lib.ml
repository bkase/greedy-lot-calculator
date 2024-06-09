open Core

include Context

module Id : sig
  type t [@@deriving hash, sexp, compare]

  val next : unit -> t
end = struct
  type t = int [@@deriving hash, sexp, compare]

  let ram = ref 0

  let next () = incr ram ; !ram
end

module Lot_entry = struct
  type t = {
      id : Id.t
    ; date : Time_ns.Alternate_sexp.t
    ; amount : Bignum.t
    ; price : Bignum.t
    }
    [@@deriving hash, sexp, compare]

  let price t = t.price
  let date t = t.date

  exception ParseError of string

  module Parse = struct
    let of_triple (date, amt, price) =
        { id = Id.next ()
        ; date = Time_ns.of_string_with_utc_offset (Core.sprintf "%sT00:00:00Z" date)
        ; amount = Bignum.of_string amt
        ; price = Bignum.of_string price
        }

    let of_simple ss = match ss with
      | [date; amt; price] -> of_triple (date, amt, price)
      | _ -> raise (ParseError "list not formatted as [date;amt;price]")

    let%test_unit "simple parses correctly" =
      let _simple = of_simple ["2021-02-07"; "181.2777"; "1.7882"] in
      (*Core.printf !"Parsed %{sexp: t}\n" simple*)
      ()

    let of_more ss = match ss with
      | [date; price; _; _; _; _; _; _; _; amt; _; _] -> of_triple (date, amt, price)
      | _ -> raise (ParseError "list not formatted as [date;price;_*7;amount]")
    let%test_unit "more parses correctly" =
      let _more = of_more ["2021-07-08"; "4.283872034873"; "";"";"";"";"";"";"";"18.883723";"";""] in
      (*Core.printf !"Parsed %{sexp: t}\n" more*)
      ()
  end

  module For_tests = struct
    let zero = { id = Id.next (); date = Time_ns.epoch; amount = Bignum.zero; price = Bignum.zero }

    let another_priced t p = { t with id = Id.next () ; price = p }
    let another_dated t d = { t with id = Id.next () ; date = d }
  end
end

module Curve = struct
  type t = Todo
    [@@deriving hash, sexp, compare]
end

module Lot = struct
  type t =
    | Static of Lot_entry.t
    | Vesting of Curve.t * [`Adjusted of Bignum.t]
    [@@deriving hash, sexp, compare]

  let view t = match t with
      Static e -> e
    | Vesting (_c, (`Adjusted _b)) -> failwith "todo vesting view"

  let s e = Static e

  let v1 (f : Lot_entry.t -> 'a) t = f (view t)
  let v2 (f : Lot_entry.t -> Lot_entry.t -> 'a) t1 t2 = f (view t1) (view t2)
end

module Heap = Hash_heap.Make(Id)

module Context = struct
  type t =
    { mixed_lots : Lot.t Heap.t
    ; short_term_dates : Lot.t Heap.t
    ; short_term_lots : Lot.t Heap.t
    ; long_term_lots : Lot.t Heap.t
    ; now : Time_ns.Alternate_sexp.t
    ; income_tax : Bignum.t
    ; short_gains_tax : Bignum.t
    ; long_gains_tax : Bignum.t
    }

  let high_priced_heap () = Heap.create (Lot.v2 (fun e1 e2 -> Bignum.compare e2.price e1.price))
  let oldest_date_heap () = Heap.create (Lot.v2 (fun e1 e2 -> Time_ns.compare e1.date e2.date))

  let create () =
    let high_priced_heap () = Heap.create (Lot.v2 (fun e1 e2 -> Bignum.compare e1.price e2.price)) in
    let oldest_date_heap () = Heap.create (Lot.v2 (fun e1 e2 -> Time_ns.compare e1.date e2.date)) in
    { mixed_lots = high_priced_heap ()
    ; short_term_dates = oldest_date_heap ()
    ; short_term_lots = high_priced_heap ()
    ; long_term_lots = high_priced_heap ()
    ; now = Time_ns.epoch
    ; income_tax = Bignum.zero
    ; short_gains_tax = Bignum.zero
    ; long_gains_tax = Bignum.zero
    }

end


let%test_module "Contexts" = (module struct
  let heap_to_list h =
    let h' = Heap.copy h in
    let rec go xs =
      if Heap.length h' > 0 then
        go (Heap.pop_exn h' :: xs)
      else
        xs
    in
    List.rev (go [])

  let%test_unit "high priced heap does high prices first" =
    let h = Context.high_priced_heap () in
    let add e =
      match Heap.push ~key:e.Lot_entry.id ~data:(Lot.s e) h with
      | `Key_already_present -> failwith "unexpected key already present"
      | `Ok -> ()
    in
    let ez = Lot_entry.For_tests.zero in
    add ez;
    let open Lot_entry.For_tests in
    let z = another_priced ez in
    add (z (Bignum.of_int 10));
    add (z (Bignum.of_int 5));
    add (z (Bignum.of_int 20));
    add (z (Bignum.of_int 1));
    [%test_eq: Bignum.t List.t] (heap_to_list h |> List.map ~f:(fun s -> (Lot.view s) |> Lot_entry.price)) Bignum.[of_int 20; of_int 10; of_int 5; of_int 1; of_int 0]

  let%test_unit "oldest heap does oldest dates first" =
    let h = Context.oldest_date_heap () in
    let add e =
      match Heap.push ~key:e.Lot_entry.id ~data:(Lot.s e) h with
      | `Key_already_present -> failwith "unexpected key already present"
      | `Ok -> ()
    in
    let timed i = Time_ns.of_string_with_utc_offset (Core.sprintf "1970-01-01 0%d:00:00Z" i) in
    let timed2 i = Time_ns.of_string_with_utc_offset (Core.sprintf "1970-01-01 %d:00:00Z" i) in
    let ez = Lot_entry.For_tests.zero in
    add ez;
    let open Lot_entry.For_tests in
    let z = another_dated ez in
    add (z (Time_ns.add ez.Lot_entry.date (Time_ns.Span.of_hr 10.)));
    add (z (Time_ns.add ez.Lot_entry.date (Time_ns.Span.of_hr 3.)));
    add (z (Time_ns.add ez.Lot_entry.date (Time_ns.Span.of_hr 5.)));
    [%test_eq: Time_ns.Alternate_sexp.t List.t] (heap_to_list h |> List.map ~f:(fun s -> (Lot.view s) |> Lot_entry.date)) Time_ns.[epoch; timed 3; timed 5; timed2 10]

end)
