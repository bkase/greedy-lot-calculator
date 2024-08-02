open Core
include Loader

module Constants = struct
  let genesis_time = Time_ns.of_string_with_utc_offset "2021-03-10T00:00:00Z"

  let slot_time_mins = 3

  let year_in_mins = 525600
end

module Id : sig
  type t [@@deriving hash, sexp, compare, equal]

  val next : unit -> t

  val unsafe_of_int : int -> t
end = struct
  type t = int [@@deriving hash, sexp, compare, equal]

  let ram = ref 0

  let next () = incr ram ; !ram

  let unsafe_of_int = Fn.id
end

let or_else a = function None -> a | Some a' -> a'

module Lot_entry = struct
  type t =
    { id : Id.t
    ; direction : [ `In of [ `Taxable | `Non_taxable of [ `Z | `Sz ] ] | `Out ]
    ; date : Time_ns.Alternate_sexp.t
    ; amount : Bignum.t
    ; price : Bignum.t
    ; metadata : string option
    ; witheld : Bignum.t
    }
  [@@deriving hash, sexp, compare]

  let price t = t.price

  let date t = t.date

  module Compare_earliest = struct
    let compare x x' = Time_ns.compare (date x) (date x')
  end

  exception ParseError of string

  module Parse = struct
    let of_triple ?witheld ?metadata ~direction (date, amt, price) =
      { id = Id.next ()
      ; direction
      ; date =
          Time_ns.of_string_with_utc_offset (Core.sprintf "%sT00:00:00Z" date)
      ; amount = Bignum.of_string amt
      ; price = Bignum.of_string price
      ; metadata
      ; witheld = Option.map ~f:Bignum.of_string witheld |> or_else Bignum.zero
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
      (* minaexplorer format *)
      | [ date; price; _; _; _; _; _; txn_id; _; amt; _; _ ] ->
          of_triple ~direction:(`In taxable) ~metadata:txn_id (date, amt, price)
      (* pulley format *)
      | [ date; price; amt; witheld; txn_id ] ->
          of_triple ~direction:(`In taxable) ~witheld ~metadata:txn_id
            (date, amt, price)
      | _ ->
          raise
            (ParseError "list not formatted as [date;price;_*7;amount] (or alt)")

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
      ; witheld = Bignum.zero
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
    ; witheld = Bignum.zero
    }
end

module Lot = struct
  type t = Static of Lot_entry.t | Vesting of Timing.t * Lot_entry.t
  [@@deriving hash, sexp, compare]

  let id t = match t with Static e -> e.id | Vesting (_, e) -> e.id

  let is_synthetic t = match t with Static _ -> false | Vesting _ -> true

  let adjust_amount_by t by =
    match t with
    | Static e ->
        Static Bignum.{ e with amount = e.amount - by }
    | Vesting (timing, e) ->
        let open Bignum in
        Vesting (timing, { e with amount = e.amount - by })

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
    let amount' =
      liquid at timing - (timing.initial_minimum_balance - e.Lot_entry.amount)
    in
    assert (amount' >= zero) ;
    { e with Lot_entry.amount = amount' }

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
        let d' =
          { b' with
            Lot_entry.date =
              Time_ns.of_string_with_utc_offset "2022-04-13T00:00:00Z"
          }
        in
        let a = Lot.Static a' in
        let b = Lot.Static b' in
        let c = Lot.Static c' in
        let d = Lot.Static d' in

        [%test_eq: Lot.t list]
          (demux ~compare:Lot.Compare_earliest.compare
             [ [ a; c ]; [ a; b ]; [ d ] ] )
          [ a; a; b; d; c ]

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

module Log = struct
  module Extra = struct
    type t =
      { witheld : Bignum.t
      ; income : Bignum.t
      ; short_gains : Bignum.t
      ; long_gains : Bignum.t
      ; ids_consumed : Id.t list
      }
    [@@deriving sexp]

    let income dollars witheld =
      let open Bignum in
      { witheld
      ; income = dollars
      ; short_gains = zero
      ; long_gains = zero
      ; ids_consumed = []
      }

    let zero = income Bignum.zero Bignum.zero

    let incr_gains_tax which t by key =
      match which with
      | `Short ->
          { t with
            short_gains = Bignum.(t.short_gains + by)
          ; ids_consumed = key :: t.ids_consumed
          }
      | `Long ->
          { t with
            long_gains = Bignum.(t.long_gains + by)
          ; ids_consumed = key :: t.ids_consumed
          }
  end

  module Item = struct
    type t = Lot_entry.t * Extra.t

    let to_csv : t -> string list =
     fun (e, x) ->
      [ e.id |> Id.sexp_of_t |> Sexp.to_string_hum
      ; e.date |> Time_ns.to_string_utc
      ; e.price |> Bignum.to_string_hum
      ; e.amount |> Bignum.to_string_hum
      ; Bignum.(e.price * e.amount) |> Bignum.to_string_hum
      ; x.income |> Bignum.to_string_hum
      ; x.witheld |> Bignum.to_string_hum
      ; x.short_gains |> Bignum.to_string_hum
      ; x.long_gains |> Bignum.to_string_hum
      ; x.ids_consumed
        |> List.map ~f:(fun id -> Id.sexp_of_t id |> Sexp.to_string_hum)
        |> List.intersperse ~sep:","
        |> List.fold_left ~init:"" ~f:(fun acc x -> acc ^ x)
      ]
  end

  module Accumulator = struct
    type t =
      { by_month : string * Bignum.t
      ; by_quarter : string list * Bignum.t
      ; by_year : string * Bignum.t
      ; by_all : string * Bignum.t
      }

    let zero =
      Bignum.
        { by_month = ("2021-01", zero)
        ; by_quarter = ([ "2021-01"; "2021-02"; "2021-03" ], zero)
        ; by_year = ("2021", zero)
        ; by_all = ("", zero)
        }

    let tick t now =
      let is_within_span check_time p = p (Time_ns.to_string_utc check_time) in

      let is_within_month check_time against =
        is_within_span check_time (String.is_prefix ~prefix:against)
      in
      let is_within_quarter check_time againsts =
        is_within_span check_time (fun curr ->
            List.exists againsts ~f:(fun against ->
                String.is_prefix ~prefix:against curr ) )
      in
      let is_within_year check_time against =
        is_within_span check_time (String.is_prefix ~prefix:against)
      in

      let year_to_against time = String.prefix (Time_ns.to_string_utc time) 4 in

      let month_to_against time =
        String.prefix (Time_ns.to_string_utc time) 7
      in

      let quarter_to_against time =
        let this_month = String.prefix (Time_ns.to_string_utc time) 7 in
        match String.split this_month ~on:'-' with
        | [ yr; "01" ] | [ yr; "02" ] | [ yr; "03" ] ->
            [ yr ^ "-01"; yr ^ "-02"; yr ^ "-03" ]
        | [ yr; "04" ] | [ yr; "05" ] | [ yr; "06" ] ->
            [ yr ^ "-04"; yr ^ "-05"; yr ^ "-06" ]
        | [ yr; "07" ] | [ yr; "08" ] | [ yr; "09" ] ->
            [ yr ^ "-07"; yr ^ "-08"; yr ^ "-09" ]
        | [ yr; "10" ] | [ yr; "11" ] | [ yr; "12" ] ->
            [ yr ^ "-10"; yr ^ "-11"; yr ^ "-12" ]
        | _ ->
            failwith "improper date format"
      in

      let check_within ~p ~to_against (against, x) =
        if p now against then (against, x) else (to_against now, Bignum.zero)
      in
      { by_month =
          check_within ~p:is_within_month ~to_against:month_to_against
            t.by_month
      ; by_quarter =
          check_within ~p:is_within_quarter ~to_against:quarter_to_against
            t.by_quarter
      ; by_year =
          check_within ~p:is_within_year ~to_against:year_to_against t.by_year
      ; by_all = t.by_all
      }

    let add t y =
      let f (a, b) = (a, Bignum.(b + y)) in
      { by_month = f t.by_month
      ; by_quarter = f t.by_quarter
      ; by_year = f t.by_year
      ; by_all = f t.by_all
      }

    let to_csv t =
      [ snd t.by_month; snd t.by_quarter; snd t.by_year; snd t.by_all ]
      |> List.map ~f:Bignum.to_string_hum
  end

  let to_csv : Item.t list -> string list list =
   fun t ->
    let _, b =
      List.fold_left t
        ~init:(Accumulator.(zero, zero, zero, zero), [])
        ~f:(fun ((a1, a2, a3, a4), build) (e, x) ->
          let a1', a2', a3', a4' =
            Accumulator.
              (tick a1 e.date, tick a2 e.date, tick a3 e.date, tick a4 e.date)
          in
          let acc_income = Accumulator.add a1' x.income in
          let acc_witheld = Accumulator.add a2' x.witheld in
          let acc_short_gains = Accumulator.add a3' x.short_gains in
          let acc_long_gains = Accumulator.add a4' x.long_gains in

          let xs =
            ( Item.to_csv (e, x)
            @ Accumulator.to_csv acc_income
            @ Accumulator.to_csv acc_witheld
            @ Accumulator.to_csv acc_short_gains
            @ Accumulator.to_csv acc_long_gains )
            :: build
          in

          ((acc_income, acc_witheld, acc_short_gains, acc_long_gains), xs) )
    in
    List.rev b
end

module Context = struct
  type t =
    { mixed_lots : Lot.t Heap.t
    ; short_term_dates : Lot.t Heap.t
    ; short_term_lots : Lot.t Heap.t
    ; long_term_lots : Lot.t Heap.t
    ; synthetics : Lot.t * Lot.t
    ; now : Time_ns.Alternate_sexp.t
    }

  let v' = Lot.v2 Lot_entry.dummy_date

  let high_priced_heap () =
    Heap.create (v' (fun e1 e2 -> Bignum.compare e2.price e1.price))

  let oldest_date_heap () =
    Heap.create (v' (fun e1 e2 -> Time_ns.compare e1.date e2.date))

  let create ~synthetics () =
    { mixed_lots = high_priced_heap ()
    ; short_term_dates = oldest_date_heap ()
    ; short_term_lots = high_priced_heap ()
    ; long_term_lots = high_priced_heap ()
    ; synthetics
    ; now = Constants.genesis_time
    }

  let heap_to_list h =
    let h' = Heap.copy h in
    let rec go xs =
      if Heap.length h' > 0 then go (Heap.pop_with_key_exn h' :: xs) else xs
    in
    List.rev (go [])

  (* for now printf, later sprintf *)
  let debug_view t =
    let in_order = heap_to_list t.mixed_lots in
    Core.printf "[\n" ;
    List.iter in_order ~f:(fun (id, x) ->
        let e = Lot.view t.now x in
        let s = Bignum.to_string_hum in
        Core.printf
          !"\t%{sexp: Id.t}:(%s @ $%s = $%s)\n"
          id (s e.amount) (s e.price)
          (s Bignum.(e.amount * e.price)) ) ;
    Core.printf "]\n"

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

  let remove_lot t x =
    let t' = tick t x in
    let e = Lot.view t'.now x in

    Heap.remove t.mixed_lots e.id ;
    Heap.remove t.short_term_dates e.id ;
    Heap.remove t.short_term_lots e.id ;
    Heap.remove t.long_term_lots e.id ;
    t'

  let add_lot t x =
    let t' = tick t x in
    let e = Lot.view t'.now x in

    Heap.push_exn ~key:e.id ~data:x t.mixed_lots ;
    Heap.push_exn ~key:e.id ~data:x t.short_term_dates ;
    Heap.push_exn ~key:e.id ~data:x t.short_term_lots ;
    tick t' x

  let extra_incoming t x =
    let e = Lot.view t.now x in
    Log.Extra.income Bignum.(e.price * e.amount) Bignum.(e.price * e.witheld)

  let rec keep_draining t full_x price amount_remaining extra =
    let open Bignum in
    let use_and_lose which use loses extra =
      let key, x = Heap.pop_with_key_exn use in
      let e = Lot.view t.now x in
      let remainder = amount_remaining - e.amount in

      List.iter loses ~f:(fun l -> Heap.remove l key) ;

      if remainder <= zero then
        ( (* we didn't consume the full top entry *)
          t
        , x
        , amount_remaining
        , Log.Extra.incr_gains_tax which extra
            (amount_remaining * (price - e.price))
            key )
      else
        (* we did consume the full top entry, so we need to keep draining *)
        let extra' =
          Log.Extra.incr_gains_tax which extra
            (e.amount * (price - e.price))
            key
        in
        keep_draining t full_x price remainder extra'
    in

    assert (amount_remaining >= zero) ;
    if amount_remaining <= zero then (t, full_x, amount_remaining, extra)
    else
      let key, best_mixed' = Heap.top_with_key_exn t.mixed_lots in
      let best_mixed = Lot.view t.now best_mixed' in
      (* this is a loss, so just pick the largest one (short or long) *)
      if best_mixed.price >= price then (
        match Heap.find t.long_term_lots key with
        | Some _ ->
            use_and_lose `Long t.long_term_lots
              [ t.short_term_dates; t.short_term_lots; t.mixed_lots ]
              extra
        | None ->
            assert (Heap.find t.short_term_lots key |> Option.is_some) ;

            use_and_lose `Short t.short_term_lots
              [ t.short_term_dates; t.long_term_lots; t.mixed_lots ]
              extra )
      else if
        (* if it's a gain, choose the best lot to take from [prefer long] *)
        Int.(Heap.length t.long_term_lots > 0)
      then
        use_and_lose `Long t.long_term_lots
          [ t.short_term_dates; t.short_term_lots; t.mixed_lots ]
          extra
      else
        use_and_lose `Short t.short_term_lots
          [ t.short_term_dates; t.long_term_lots; t.mixed_lots ]
          extra

  let sell t x =
    (* prelude, add synthetics to the heap *)
    let t' =
      let s1, s2 = t.synthetics in
      add_lot (add_lot t s1) s2
    in

    let e = Lot.view t'.now x in
    let t'', last_x, amount_remaining, extra =
      keep_draining t' x e.price e.amount Log.Extra.zero
    in

    ( if Bignum.(extra.short_gains > e.price * e.amount) then
        let open Bignum in
        failwithf
          !"Short gains %s , %s * %s = %s\n"
          (to_string_hum extra.short_gains)
          (to_string_hum e.price) (to_string_hum e.amount)
          (to_string_hum (e.price * e.amount))
          () ) ;

    (* post-lude, remove synthetics from heap *)
    let t''' =
      let s1, s2 = t''.synthetics in
      remove_lot (remove_lot t'' s1) s2
    in

    (* add back the remainder, adjusted or modify the synthetic *)
    if Bignum.(amount_remaining <= zero) then (t''', extra)
    else if Lot.is_synthetic last_x then
      let s1, s2 = t'''.synthetics in
      ( { t''' with
          synthetics =
            ( ( if Id.equal (Lot.id last_x) (Lot.id s1) then
                  Lot.adjust_amount_by s1 amount_remaining
                else s1 )
            , if Id.equal (Lot.id last_x) (Lot.id s2) then
                Lot.adjust_amount_by s2 amount_remaining
              else s2 )
        }
      , extra )
    else (add_lot t''' (Lot.adjust_amount_by last_x amount_remaining), extra)
end

module Run = struct
  let run ~synthetics events =
    let ctx = ref (Context.create ~synthetics ()) in

    let prefix =
      let s1, s2 = synthetics in
      let e1, e2 = (Lot.view !ctx.now s1, Lot.view !ctx.now s2) in
      [ (e1, Log.Extra.zero); (e2, Log.Extra.zero) ]
    in
    (* map events to a log *)
    let log =
      List.filter_map events ~f:(fun e ->
          (*Context.debug_view !ctx ;*)
          (* TODO: Assert invariants *)
          let entry = Lot.view !ctx.now e in
          if Id.equal entry.id (Id.unsafe_of_int 10) then
            Core.printf !"Looking at %{sexp: Lot_entry.t}\n" entry ;

          match entry.direction with
          | `In taxable ->
              let ctx' = Context.add_lot !ctx e in
              let extra =
                match taxable with
                | `Non_taxable _ ->
                    Log.Extra.zero
                | `Taxable ->
                    Context.extra_incoming ctx' e
              in
              ctx := ctx' ;
              Some (entry, extra)
          | `Out ->
              let ctx', extra = Context.sell !ctx e in
              ctx := ctx' ;
              Some (entry, extra) )
    in
    Log.to_csv (prefix @ log)
end

let%test_module "Contexts" =
  ( module struct
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
        ( Context.heap_to_list h
        |> List.map ~f:(fun (_, s) ->
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
        ( Context.heap_to_list h
        |> List.map ~f:(fun (_, s) ->
               Lot.view Lot_entry.dummy_date s |> Lot_entry.date ) )
        Time_ns.[ epoch; timed 3; timed 5; timed2 10 ]
  end )
