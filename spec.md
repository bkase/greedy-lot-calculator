Outline:

* Sort by date
* Take as input
    Amount|Price ; [in/out boolean] ; 

* Seed with starting lots

* Any "in" amount adds to the lot collection
    * Adding affects 'income' tax burden, unless already accounted for by K1s
* Any "out" amount removes from the lot collection
    * Removing affects 'capital gains' tax burden, either short or long

* How does "out" work?
    (optimizes for losses)
    * If there exists a lot priced higher than "current price",
        * Drain that first, regardless of short or long-term
    * Else, if this "out"'s current price is "higher" than _all_ priced lots,
        * Take the 'highest' from the "long-term gains" pile
    Theorem: This 'greedy' algorithm is optimal ie. this ordering 


Lot_entry.t = { id, date : Date, amount : Double, price : Double }

```
Lot.t =
 | Static of Lot_entry.t
 | Vesting of (Curve.t * [`Adjusted of Double.t])
 ```
 invariant: view(Vesting(c,a)) > 0

view : Lot.t -> Lot_entry.t
view = fun
   | Static e -> e
   | Vesting c -> TODO


ctx:
```ocaml
    { mixed_lot_queue : Lot.t Price.PriorityQueue.t
    ; short_term_date_queue : Lot.t Date.PriorityQueue.t (* OLDEST first *)
    ; short_term_lot_queue : Lot.t Price.PriorityQueue.t
    ; long_term_lot_queue : Lot.t Price.PriorityQueue.t
    ; now : Date.t
    ; income_tax : Double.t
    ; short_gains_tax : Double.t
    ; long_gains_tax : Double.t
    } 
```

invariants:
```
    let m := mixed_lot_queue
        sd := short_term_date_queue
        s := short_term_lot_queue
        l := long_term_lot_queue
    items(m) = items(s) ++ items(l)
    \forall x \in items(s). now+1yr >= x.date
    \forall x \in items(l). now+1yr < x.date
    items(sd) = items(s)
```

logic:

```
state = init()

for e in events:
    assert_invariants()

    if e is "in":
        update income_tax on state with view(e)
    else: // e is "out"
        (adjust for "draining")
        let total = burden(e)
        while total > 0:
            best_mixed = state.mixed_lot_queue.peak()
            if best_mixed.price > e.price:
                state.mixed_lot_queue.pop()
                state.short_term_date_queue.remove(best_mixed)
                state.short_term_lot_queue.remove(best_mixed)
                state.long_term_lot_queue.remove(best_mixed)
                update short_gains_tax with view(e)
            else
                peak from long_term_lot if available
                    then adjust long_gains_tax
                if missing
                    then adjust short_gains_tax

    tick_now(state, e)
```

where
    tick_now:
    ```
        state.now = e.date
        oldest_short_term = state.short_term_date_queue.peak()
        if oldest_short_term.date > e.date + 1yr:
            state.short_term_date_queue.pop()
            state.short_term_lot_queue.remove(oldest_short_term)
            state.long_term_lot_queue.add(oldest_short_term)
    ```


