open Core
include Context

module Constants = struct
  let genesis_time = Time_ns.of_string_with_utc_offset "2021-03-17T00:00:00Z"

  let slot_time_mins = 3
end

module Id : sig
  type t [@@deriving hash, sexp, compare]

  val next : unit -> t
end = struct
  type t = int [@@deriving hash, sexp, compare]

  let ram = ref 0

  let next () = incr ram ; !ram
end

module Lot_entry = struct
  type t =
    { id : Id.t
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
      ; date =
          Time_ns.of_string_with_utc_offset (Core.sprintf "%sT00:00:00Z" date)
      ; amount = Bignum.of_string amt
      ; price = Bignum.of_string price
      }

    let of_simple ss =
      match ss with
      | [ date; amt; price ] ->
          of_triple (date, amt, price)
      | _ ->
          raise (ParseError "list not formatted as [date;amt;price]")

    let%test_unit "simple parses correctly" =
      let _simple = of_simple [ "2021-02-07"; "181.2777"; "1.7882" ] in
      (*Core.printf !"Parsed %{sexp: t}\n" simple*)
      ()

    let of_more ss =
      match ss with
      | [ date; price; _; _; _; _; _; _; _; amt; _; _ ] ->
          of_triple (date, amt, price)
      | _ ->
          raise (ParseError "list not formatted as [date;price;_*7;amount]")

    let%test_unit "more parses correctly" =
      let _more =
        of_more
          [ "2021-07-08"
          ; "4.283872034873"
          ; ""
          ; ""
          ; ""
          ; ""
          ; ""
          ; ""
          ; ""
          ; "18.883723"
          ; ""
          ; ""
          ]
      in
      (*Core.printf !"Parsed %{sexp: t}\n" more*)
      ()
  end

  let dummy_date = Time_ns.of_string_with_utc_offset "2022-06-06T00:00:00Z"

  module For_tests = struct
    let zero =
      { id = Id.next ()
      ; date = Time_ns.epoch
      ; amount = Bignum.zero
      ; price = Bignum.zero
      }

    let another_priced t p = { t with id = Id.next (); price = p }

    let another_dated t d = { t with id = Id.next (); date = d }
  end
end

module Timing = struct
  (* note: vesting period always 1 *)
  type t =
    { initial_minimum_balance : Bignum.t
    ; cliff_time : int
    ; cliff_amount : Bignum.t
    ; vesting_increment : Bignum.t
    }
  [@@deriving hash, sexp, compare]
end

module Lot = struct
  type t = Static of Lot_entry.t | Vesting of Timing.t * Lot_entry.t
  [@@deriving hash, sexp, compare]

  let time_to_slot at =
    let span = Time_ns.abs_diff at Constants.genesis_time in
    Int.of_float
      (Time_ns.Span.to_min span /. Float.of_int Constants.slot_time_mins)

  let liquid (at : Time_ns.t) (timing : Timing.t) =
    let global_slot = time_to_slot at in
    let before_cliff = global_slot < timing.cliff_time in
    if before_cliff then Bignum.zero
    else
      let num_periods = global_slot - timing.cliff_time in
      let min_balance_less_cliff_decrement =
        Bignum.(timing.initial_minimum_balance - timing.cliff_amount)
      in
      let vesting_decrement =
        Bignum.(of_int num_periods * timing.vesting_increment)
      in
      let res = Bignum.(min_balance_less_cliff_decrement - vesting_decrement) in
      let clamped = Bignum.(if res < zero then zero else res) in
      Bignum.(timing.initial_minimum_balance - clamped)

  (* synthesize a static lot from a vesting one taking into account
     liquidity and how much was spent out of it *)
  let synthesize at timing e =
    (* imagine we start at 100
           now there's 5 liquid
           we spend 2

       we need to compute that there's 3 left liquid, but 98 total left

       initial_minimum_balance = 100
       balance = 98
       liquid_at = 5

       5 - (100-98) *)
    let open Bignum in
    { e with
      Lot_entry.amount =
        liquid at timing - (timing.initial_minimum_balance - e.Lot_entry.amount)
    }

  let%test_module "liquidity tests" =
    ( module struct
      let timing =
        { Timing.cliff_time = 500
        ; initial_minimum_balance = Bignum.of_int 100
        ; cliff_amount = Bignum.of_int 50
        ; vesting_increment = Bignum.of_string "0.1"
        }

      let entry100 =
        { Lot_entry.For_tests.zero with
          amount = timing.initial_minimum_balance
        }

      let%test_unit "fully liquid" =
        let full_liquid100 =
          liquid
            (Time_ns.of_string_with_utc_offset "2024-01-01T00:00:00Z")
            timing
        in
        [%test_eq: Bignum.t] (Bignum.of_int 100) full_liquid100

      let%test_unit "before cliff" =
        let cliff100 =
          liquid
            (Time_ns.add_saturating Constants.genesis_time
               (Time_ns.Span.of_min 30.) )
            timing
        in
        [%test_eq: Bignum.t] Bignum.zero cliff100

      let%test_unit "after cliff, midvest" =
        let cliff100 =
          liquid
            (Time_ns.add_saturating Constants.genesis_time
               (Time_ns.Span.of_min 1507.) )
            timing
        in
        [%test_eq: Bignum.t] (Bignum.of_string "50.2") cliff100

      let%test_unit "synthesis idempotent no sends" =
        let cliff100 =
          synthesize
            (Time_ns.add_saturating Constants.genesis_time
               (Time_ns.Span.of_min 1507.) )
            timing entry100
        in
        [%test_eq: Lot_entry.t]
          { entry100 with amount = Bignum.of_string "50.2" }
          cliff100

      let%test_unit "synthesis tracks some sends" =
        let cliff100 =
          synthesize
            (Time_ns.add_saturating Constants.genesis_time
               (Time_ns.Span.of_min 1507.) )
            timing
            { entry100 with amount = Bignum.of_int 95 }
        in
        [%test_eq: Lot_entry.t]
          { entry100 with amount = Bignum.of_string "45.2" }
          cliff100

      let%test_unit "synthesis handles fully liquid with sends" =
        let cliff100 =
          synthesize
            (Time_ns.of_string_with_utc_offset "2024-01-01T00:00:00Z")
            timing
            { entry100 with amount = Bignum.of_int 95 }
        in
        [%test_eq: Lot_entry.t]
          { entry100 with amount = Bignum.of_int 95 }
          cliff100
    end )

  let view at t =
    match t with Static e -> e | Vesting (timing, e) -> synthesize at timing e

  let s e = Static e

  let v1 at (f : Lot_entry.t -> 'a) t = f (view at t)

  let v2 at (f : Lot_entry.t -> Lot_entry.t -> 'a) t1 t2 =
    f (view at t1) (view at t2)
end

module Heap = Hash_heap.Make (Id)

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

  let v' = Lot.v2 Lot_entry.dummy_date

  let high_priced_heap () =
    Heap.create (v' (fun e1 e2 -> Bignum.compare e2.price e1.price))

  let oldest_date_heap () =
    Heap.create (v' (fun e1 e2 -> Time_ns.compare e1.date e2.date))

  let create () =
    let high_priced_heap () =
      Heap.create (v' (fun e1 e2 -> Bignum.compare e1.price e2.price))
    in
    let oldest_date_heap () =
      Heap.create (v' (fun e1 e2 -> Time_ns.compare e1.date e2.date))
    in
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

let%test_module "Contexts" =
  ( module struct
    let heap_to_list h =
      let h' = Heap.copy h in
      let rec go xs =
        if Heap.length h' > 0 then go (Heap.pop_exn h' :: xs) else xs
      in
      List.rev (go [])

    let%test_unit "high priced heap does high prices first" =
      let h = Context.high_priced_heap () in
      let add e =
        match Heap.push ~key:e.Lot_entry.id ~data:(Lot.s e) h with
        | `Key_already_present ->
            failwith "unexpected key already present"
        | `Ok ->
            ()
      in
      let ez = Lot_entry.For_tests.zero in
      add ez ;
      let open Lot_entry.For_tests in
      let z = another_priced ez in
      add (z (Bignum.of_int 10)) ;
      add (z (Bignum.of_int 5)) ;
      add (z (Bignum.of_int 20)) ;
      add (z (Bignum.of_int 1)) ;
      [%test_eq: Bignum.t List.t]
        ( heap_to_list h
        |> List.map ~f:(fun s ->
               Lot.view Lot_entry.dummy_date s |> Lot_entry.price ) )
        Bignum.[ of_int 20; of_int 10; of_int 5; of_int 1; of_int 0 ]

    let%test_unit "oldest heap does oldest dates first" =
      let h = Context.oldest_date_heap () in
      let add e =
        match Heap.push ~key:e.Lot_entry.id ~data:(Lot.s e) h with
        | `Key_already_present ->
            failwith "unexpected key already present"
        | `Ok ->
            ()
      in
      let timed i =
        Time_ns.of_string_with_utc_offset
          (Core.sprintf "1970-01-01 0%d:00:00Z" i)
      in
      let timed2 i =
        Time_ns.of_string_with_utc_offset
          (Core.sprintf "1970-01-01 %d:00:00Z" i)
      in
      let ez = Lot_entry.For_tests.zero in
      add ez ;
      let open Lot_entry.For_tests in
      let z = another_dated ez in
      add (z (Time_ns.add ez.Lot_entry.date (Time_ns.Span.of_hr 10.))) ;
      add (z (Time_ns.add ez.Lot_entry.date (Time_ns.Span.of_hr 3.))) ;
      add (z (Time_ns.add ez.Lot_entry.date (Time_ns.Span.of_hr 5.))) ;
      [%test_eq: Time_ns.Alternate_sexp.t List.t]
        ( heap_to_list h
        |> List.map ~f:(fun s ->
               Lot.view Lot_entry.dummy_date s |> Lot_entry.date ) )
        Time_ns.[ epoch; timed 3; timed 5; timed2 10 ]
  end )
