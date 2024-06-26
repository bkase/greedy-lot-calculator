open Core
include Loader

module Constants = struct
  let genesis_time = Time_ns.of_string_with_utc_offset "2021-03-17T00:00:00Z"

  let slot_time_mins = 3

  let year_in_mins = 525600
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
    ; direction : [ `In of [ `Taxable | `Non_taxable of [ `Z | `Sz ] ] | `Out ]
    ; date : Time_ns.Alternate_sexp.t
    ; amount : Bignum.t
    ; price : Bignum.t
    ; metadata : string option
    }
  [@@deriving hash, sexp, compare]

  let price t = t.price

  let date t = t.date

  module Compare_earliest = struct
    let compare x x' = Time_ns.compare (date x) (date x')
  end

  exception ParseError of string

  module Parse = struct
    let of_triple ?metadata ~direction (date, amt, price) =
      { id = Id.next ()
      ; direction
      ; date =
          Time_ns.of_string_with_utc_offset (Core.sprintf "%sT00:00:00Z" date)
      ; amount = Bignum.of_string amt
      ; price = Bignum.of_string price
      ; metadata
      }

    let of_simple ss =
      match ss with
      | [ date; amt; raw_price ] ->
          let price =
            let open Bignum in
            let a = of_string amt in
            let rp = of_string raw_price in
            rp / a
          in
          of_triple ~direction:`Out (date, amt, Bignum.to_string_accurate price)
      | _ ->
          raise (ParseError "list not formatted as [date;amt;price]")

    let%test_unit "simple parses correctly" =
      let _simple = of_simple [ "2021-02-07"; "181.2777"; "1882" ] in
      (*Core.printf !"Parsed %{sexp: t}\n" simple*)
      ()

    let of_more ~taxable ss =
      match ss with
      | [ date; price; _; _; _; _; _; txn_id; _; amt; _; _ ] ->
          of_triple ~direction:(`In taxable) ~metadata:txn_id (date, amt, price)
      | _ ->
          raise (ParseError "list not formatted as [date;price;_*7;amount]")

    let%test_unit "more parses correctly" =
      let _more =
        of_more ~taxable:`Taxable
          [ "2021-07-08"
          ; "4.283872034873"
          ; ""
          ; ""
          ; ""
          ; ""
          ; ""
          ; "Ckfoo"
          ; ""
          ; "18.883723"
          ; ""
          ; ""
          ]
      in
      (*Core.printf !"Parsed %{sexp: t}\n" _more ;*)
      ()
  end

  let dummy_date = Time_ns.of_string_with_utc_offset "2022-06-06T00:00:00Z"

  module For_tests = struct
    let zero =
      { id = Id.next ()
      ; direction = `In `Taxable
      ; date = Time_ns.epoch
      ; amount = Bignum.zero
      ; price = Bignum.zero
      ; metadata = None
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

  module Parse = struct
    let parse row =
      match row with
      | [ initial_minimum_balance; cliff_time; cliff_amount; vesting_increment ]
        ->
          { initial_minimum_balance = Bignum.of_string initial_minimum_balance
          ; cliff_time = Int.of_string cliff_time
          ; cliff_amount = Bignum.of_string cliff_amount
          ; vesting_increment = Bignum.of_string vesting_increment
          }
      | _ ->
          failwith "Parse error on timings"
  end

  let initial_entry t =
    { Lot_entry.amount = t.initial_minimum_balance
    ; direction = `In (`Non_taxable `Z)
    ; price = Bignum.zero
    ; metadata = Some "synthetic"
    ; id = Id.next ()
    ; date = Constants.genesis_time
    }
end

module Lot = struct
  type t = Static of Lot_entry.t | Vesting of Timing.t * Lot_entry.t
  [@@deriving hash, sexp, compare]

  let update_entry t e =
    match t with
    | Static _ ->
        Static e
    | Vesting (timing, _) ->
        Vesting (timing, e)

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

  let view_unsafe = `Comment_why_its_safe (view Lot_entry.dummy_date)

  module Compare_earliest = struct
    let compare x x' =
      (* safe because we don't care about amounts here, only the dates *)
      let (`Comment_why_its_safe v) = view_unsafe in
      Lot_entry.Compare_earliest.compare (v x) (v x')
  end

  let s e = Static e

  let v1 at (f : Lot_entry.t -> 'a) t = f (view at t)

  let v2 at (f : Lot_entry.t -> Lot_entry.t -> 'a) t1 t2 =
    f (view at t1) (view at t2)
end

module Demux = struct
  type 'a t = 'a list list

  let demux ~compare t =
    let rec go lotss build =
      let better (i, x) best_so_far =
        match best_so_far with
        | None ->
            (i, x)
        | Some (i', x') ->
            if compare x x' <= 0 then (i, x) else (i', x')
      in

      let best : (int * 'a) option ref = ref None in
      List.iteri lotss ~f:(fun i lots ->
          match lots with
          | [] ->
              ()
          | x :: _ ->
              let best' = better (i, x) !best in
              best := Some best' ) ;

      match !best with
      | None ->
          List.rev build
      | Some (j, lot) ->
          let lotss' =
            List.filter_mapi lotss ~f:(fun i lots ->
                match lots with
                | [] ->
                    None
                | x :: xs ->
                    if Int.equal i j then Some xs else Some (x :: xs) )
          in
          go lotss' (lot :: build)
    in
    go t []

  let%test_module "Demux" =
    ( module struct
      let%test_unit "demuxes earliest lots first" =
        let a' = Lot_entry.For_tests.zero in
        let b' =
          { a' with
            Lot_entry.date =
              Time_ns.add Lot_entry.For_tests.zero.date
                (Time_ns.Span.of_min 10.)
          }
        in
        let c' =
          { b' with
            Lot_entry.date =
              Time_ns.of_string_with_utc_offset "2024-01-01T00:00:00Z"
          }
        in
        let a = Lot.Static a' in
        let b = Lot.Static b' in
        let c = Lot.Static c' in

        [%test_eq: Lot.t list]
          (demux ~compare:Lot.Compare_earliest.compare [ [ a; c ]; [ a; b ] ])
          [ a; a; b; c ]

      let%test_unit "demuxes ints properly" =
        [%test_eq: int list]
          (demux ~compare:Int.compare
             [ [ 3; 4; 10; 20 ]
             ; [ 1; 2; 10; 200; 300 ]
             ; [ 2; 4; 6; 8 ]
             ; [ 1000 ]
             ] )
          [ 1; 2; 2; 3; 4; 4; 6; 8; 10; 10; 20; 200; 300; 1000 ]
    end )
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
    ; now = Constants.genesis_time
    ; income_tax = Bignum.zero
    ; short_gains_tax = Bignum.zero
    ; long_gains_tax = Bignum.zero
    }

  let tick t x =
    let e = Lot.view t.now x in
    let now' = if Time_ns.(t.now < e.date) then e.date else t.now in

    let rec adjust_heaps () =
      match Heap.top t.short_term_dates with
      | None ->
          ()
      | Some oldest ->
          let oldest_e = Lot.view now' oldest in
          if
            Time_ns.(
              now'
              > add oldest_e.date
                  (Span.of_min (Float.of_int Constants.year_in_mins)) )
          then (
            let key, data = Heap.pop_with_key_exn t.short_term_dates in
            Heap.remove t.short_term_lots key ;
            Heap.push_exn ~key ~data t.long_term_lots ;
            adjust_heaps () )
          else ()
    in
    adjust_heaps () ;

    { t with now = now' }

  let add_lot t x =
    let t' = tick t x in
    let e = Lot.view t'.now x in

    Heap.push_exn ~key:e.id ~data:x t.mixed_lots ;
    Heap.push_exn ~key:e.id ~data:x t.short_term_dates ;
    Heap.push_exn ~key:e.id ~data:x t.short_term_lots ;
    t'

  let incr_income_tax t x =
    let e = Lot.view t.now x in
    { t with income_tax = Bignum.(t.income_tax + (e.price * e.amount)) }

  let incr_short_gains_tax t by =
    { t with short_gains_tax = Bignum.(t.short_gains_tax + by) }

  let rec keep_draining t full_x price amount_remaining =
    let open Bignum in
    if amount_remaining <= zero then t
    else
      let best_mixed' = Heap.top_exn t.mixed_lots in
      let best_mixed = Lot.view t.now best_mixed' in
      if best_mixed.price >= price then (
        let key, x = Heap.pop_with_key_exn t.mixed_lots in
        let e = Lot.view t.now x in

        Heap.remove t.short_term_dates key ;
        Heap.remove t.short_term_lots key ;
        Heap.remove t.long_term_lots key ;

        let remainder = amount_remaining - e.amount in
        if remainder < zero then (
          (* we didn't consume the full top entry *)
          let new_e =
            { e with Lot_entry.amount = e.amount - amount_remaining }
          in
          let new_x = Lot.update_entry x new_e in
          let t' = add_lot t new_x in
          let t'' = tick t' full_x in
          (* this is always a loss *)
          assert (price - e.price <= zero) ;
          incr_short_gains_tax t'' (amount_remaining * (price - e.price)) )
        else (
          (* we did consume the full top entry, so we need to keep draining *)
          assert (price - e.price <= zero) ;
          let t' =
            incr_short_gains_tax t (amount_remaining * (price - e.price))
          in
          keep_draining t' full_x price remainder ) )
      else failwith "it's a gain, undefined"

  let sell t x =
    let t' = tick t x in
    let e = Lot.view t'.now x in
    keep_draining t' x e.price e.amount
end

module Run = struct
  let run events =
    let ctx = ref (Context.create ()) in
    (* map events to a report *)
    let report =
      List.filter_map events ~f:(fun e ->
          (* TODO: Assert invariants *)
          let entry = Lot.view !ctx.now e in
          match entry.direction with
          | `In taxable ->
              let old_ctx = !ctx in
              let ctx' = Context.add_lot !ctx e in
              let ctx'' =
                match taxable with
                | `Non_taxable _ ->
                    ctx'
                | `Taxable ->
                    Context.incr_income_tax ctx' e
              in
              ctx := ctx'' ;
              let new_year_2022 =
                Time_ns.of_string_with_utc_offset "2022-01-01T00:00:00Z"
              in
              let new_year_2023 =
                Time_ns.of_string_with_utc_offset "2023-01-01T00:00:00Z"
              in
              let new_year_2024 =
                Time_ns.of_string_with_utc_offset "2024-01-01T00:00:00Z"
              in
              let latest_2024 =
                Time_ns.of_string_with_utc_offset "2024-06-01T00:00:00Z"
              in
              if
                Time_ns.(
                  old_ctx.now < new_year_2022 && ctx''.now >= new_year_2022 )
                || Time_ns.(
                     old_ctx.now < new_year_2023 && ctx''.now >= new_year_2023 )
                || Time_ns.(
                     old_ctx.now < new_year_2024 && ctx''.now >= new_year_2024 )
                || Time_ns.(ctx''.now >= latest_2024)
              then
                Some
                  (Core.sprintf "Income tax as of %s is now: %s\n"
                     (Time_ns.to_string_utc ctx''.now)
                     (Bignum.to_string_hum !ctx.income_tax) )
              else None
          | `Out ->
              let ctx' = Context.sell !ctx e in
              ctx := ctx' ;
              None )
    in
    report
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
