# Distributed-Food-Delivery-Platform

Ballerina microservices + Kafka event backbone + Docker Compose, implementing the
architecture in `docs/architecture.md`. Read that first — this README is build/run
instructions, this file is not a repeat of the design rationale.

## What's here

```
customer-service/       :8081  — accounts, addresses, order-history read-model (Postgres)
restaurant-service/     :8082  — menus, inventory, hours, accept/reject orders (Postgres)
order-service/          :8083  — the order state machine, source of truth (Postgres)
payment-service/        :8084  — simulated payment processing (Postgres)
delivery-service/       :8085  — driver pool, assignment, live location (MongoDB)
notification-service/   :8086  — simulated multi-channel alerts (MongoDB)
admin-service/          :8087  — event-sourced reporting (MongoDB)
infra/postgres-init/    — schema + seed data, runs automatically on first Postgres start
docker-compose.yml       — Kafka (KRaft mode, no Zookeeper needed), Postgres, Mongo, all 7 services
docs/architecture.md     — the design document (service map, event contracts, diagrams)
```

## ⚠️ Important — compiled against a real `bal build`, but Ballerina Central is unreachable here

All 7 services have actually been run through Ballerina 2201.10.2's real compiler (not
just read through) in the environment that produced this code, and every one of them
compiles clean **except** for one remaining category of error: `ballerinax/postgresql`,
`ballerinax/kafka`, and `ballerinax/mongodb` can't be resolved, because that environment's
network policy blocks Ballerina Central (`central.ballerina.io`). The core `ballerina/*`
standard library (`http`, `log`, `sql`, `time`, `os`, `uuid`, `random`) ships with the
distribution and resolves fine — it's specifically the three `ballerinax` connectors that
need Central, and Central was unreachable. This is very likely **not** a problem on your
own machine or in CI: run `bal build` in each service folder (or `docker compose build`)
and, unless your network also blocks Central, the connector packages will resolve and
everything should build straight through.

Real bugs actually caught and fixed by that compiler run (i.e. verified, not guessed):
- `sql:queryConcat(baseQuery, ...filters, ` ORDER BY...`)` in `order-service/db.bal` —
  Ballerina rejects a positional argument after a spread rest argument. Fixed by pushing
  the trailing fragment into the array before spreading.
- `string x = match y { "a" => "b", ... };` — this **match expression** form is a parse
  error in this compiler (`customer-service/kafka.bal`, `notification-service/kafka.bal`),
  even though the **match statement** form (`match y { "a" => { ...statements... } }`)
  used elsewhere in the same files compiles fine. Replaced with plain if-else helper
  functions (`statusForTopic`, `channelForRecipient`).
- `check obj->action()` cannot appear directly inside a **module-level** variable
  initializer ("action invocation as an expression not allowed here") — hit in all three
  Mongo services' `mongodb:Client`/`Database`/`Collection` setup. Fixed by moving the
  `->getDatabase()`/`->getCollection()` calls into an `initCollections()` function body
  and `check`-ing that function's own result at module level instead.
- `p.totalAmount ?: 0` and `perDriver[driverId] ?: [0, 0, 0]` in `admin-service/db.bal` —
  Ballerina infers a bare `0` as `int`, which doesn't unify with the `decimal` type on the
  other side of `+=` / the tuple slot. Fixed with explicit `0d` decimal literals.
- An `int:fromString(...) ?: default` pattern that doesn't type-check (elvis needs a
  nilable left side, not `int|error`) in every SQL-backed service's `db.bal`; two unused
  imports (Ballerina treats these as compile errors, not warnings).
- Every Kafka event type and every Mongo-document type that only needs a *subset* of its
  wire fields is declared as an **open** record (`record { ... }`, no `|` pipes). A closed
  record fails `cloneWithType`/deserialization the moment the JSON (or Mongo document) has
  a field the type didn't declare — which happens constantly here, since e.g.
  `orders.created` carries `items` but `notification-service` only wants `orderId` and
  `restaurantId`, and every Mongo document comes back with a driver-added `_id` no type
  here declares. This one wasn't caught by the compiler directly (it's a runtime failure,
  not a compile error) — verified by re-deriving it from Ballerina's closed-record
  semantics and double-checking every producer/consumer field set by hand. If you add a
  new consumer of an existing topic, keep new "partial" event types open for the same
  reason.

What's still unverified, because it needs the actual connectors resolved to type-check:
the exact `ballerinax/mongodb` method signatures used in `delivery-service`,
`notification-service`, `admin-service` (`find`, `updateOne`, the `upsert = true` named
arg, the `{"$ne": null}` raw filter syntax) — these follow the connector's documented API
as closely as could be done without the real type definitions in front of the compiler,
but are the most likely spot for a remaining real error once Central resolves. Also worth
doing once you can build: add a `Dependencies.toml` (generated automatically on first
successful `bal build`) so the connector versions are locked for the whole team, and
confirm the Postgres `::text`/`::time` casts in `db.bal` behave as written against the
seeded schema.

None of this changes the architecture or the event contracts — it's a "make it compile"
pass, which is much faster to do locally with real compiler errors in front of you than
guessing blind here.

## Running it

```bash
docker compose build     # builds all 7 service images (each does its own `bal build`)
docker compose up        # starts Kafka, Postgres, Mongo, and all services
```

First boot: `kafka-topics-init` creates the 12 topics from `docs/architecture.md` with 3
partitions each; `infra/postgres-init/001-schema.sql` creates tables and seeds two
restaurants, four menu items, and one demo customer.

Tear down: `docker compose down -v` (the `-v` also drops the Postgres/Mongo volumes, so
add it only when you want a truly clean slate for a fresh demo run).

## Manual smoke test (happy path)

```bash
# 1. Create an order (customerId 1, restaurantId 1, menuItemId 1 from the seed data)
curl -s -X POST http://localhost:8083/orders -H 'Content-Type: application/json' -d '{
  "customerId": 1, "restaurantId": 1,
  "items": [{"menuItemId": 1, "name": "Kapana Platter", "quantity": 2, "unitPrice": 85.00}],
  "deliveryAddress": "12 Independence Ave, Windhoek"
}'
# -> note the returned "id"; poll it:
curl -s http://localhost:8083/orders/<id>
# status should progress CREATED -> CONFIRMED -> PREPARING automatically within a couple
# of seconds (restaurant auto-confirms if open + in stock, payment auto-simulates ~90%
# success).

# 2. Register a driver so the Delivery Service has someone to assign
curl -s -X POST http://localhost:8085/drivers -H 'Content-Type: application/json' \
  -d '{"name": "Driver One"}'

# 3. Once the order is PREPARING, mark it ready from the restaurant side
curl -s -X POST http://localhost:8082/restaurants/1/orders/<id>/ready
# -> Delivery Service should auto-assign the driver you just created; check:
curl -s http://localhost:8083/orders/<id>          # status -> OUT_FOR_DELIVERY
curl -s http://localhost:8085/deliveries/<id>

# 4. Complete the delivery
curl -s -X POST http://localhost:8085/deliveries/<id>/complete
curl -s http://localhost:8083/orders/<id>          # status -> DELIVERED

# 5. Check the generated notifications and admin report
curl -s "http://localhost:8086/notifications?orderId=<id>"
curl -s http://localhost:8087/admin/reports/restaurants/1/stats
curl -s http://localhost:8087/admin/reports/delivery-performance
```

Rejection path: set a menu item out of stock first
(`curl -X PATCH http://localhost:8082/restaurants/1/menu/1/inventory -d '{"inStock": false}'`)
then create an order for it — it should land in `CANCELLED` via the
`orders.rejected` -> `orders.cancelled` path instead.

## Group workflow reminder (per the assignment brief)

Every member's GitHub username needs to appear in the repo's commit log or that person is
recorded as a non-contributor — split the 7 services across the team and have each person
commit their own service directly rather than one person pushing everything.

## CI (`.github/workflows/build.yml`)

Added a GitHub Actions workflow that runs `bal build` on all 7 services, validates
`docker-compose.yml`, and does a full `docker compose up` + happy-path smoke test (create
order → check it leaves `CREATED`) on every push/PR. GitHub-hosted runners have normal
internet access to Ballerina Central, so this is the fastest way to get a real green/red
build signal without depending on any one teammate's laptop network — push to `main` (or
open a PR) and check the Actions tab. It also gives the group a natural way to make sure
everyone's username shows up in commits: have each person push their own service and watch
its build run.

If this repo isn't a git repo yet: `git init`, `git remote add origin <your repo URL>`,
then have each teammate `git add <their-service>/ && git commit && git push` for their own
part.

## Verification status (2026-09-30)

Re-checked this build in a follow-up session. `central.ballerina.io` and Maven Central are
still both unreachable from every sandbox available here, so a real end-to-end
`bal build` / `docker compose up` still has to happen on your own machine or in the CI
workflow above — that part hasn't changed. What *did* change: this round had working `git
clone` access to the actual `ballerina-platform/module-ballerinax-postgresql`,
`module-ballerinax-kafka`, and `module-ballerinax-mongodb` source repos, so instead of
guessing at the connector APIs, every line of code touching them was checked against the
real, current type/function signatures. That turned up and fixed four confirmed real bugs
that static guessing alone hadn't caught:

- **`rec.topic` doesn't exist** — `kafka:BytesConsumerRecord` has no top-level `topic`
  field; the topic name is nested at `rec.offset.partition.topic` (confirmed from
  `kafka_records.bal`'s real `AnydataConsumerRecord`/`PartitionOffset`/`TopicPartition`
  definitions). This hit every consumer that dispatches on topic name:
  `admin-service/kafka.bal`, `customer-service/kafka.bal`, `notification-service/kafka.bal`,
  `order-service/kafka.bal`. Fixed by reading the topic off the nested path instead.
- **MongoDB connection config was wrapped wrong** — `ConnectionConfig.connection` is typed
  `ConnectionParameters|string` (confirmed from `types.bal`), so
  `{connection: {url: mongoUri}}` doesn't type-check; it needs the bare connection string,
  `{connection: mongoUri}`. Fixed in all three Mongo services
  (`delivery-service`, `notification-service`, `admin-service`).
- **`updateOne(..., upsert = true)` used the wrong parameter name** — `Collection.updateOne`'s
  third parameter is `UpdateOptions options`, not `upsert` (confirmed from `collection.bal`);
  Ballerina named-argument syntax has to match the declared parameter name. Fixed in
  `admin-service/db.bal` to `updateOne({orderId}, {set: fields}, {upsert: true})`.
- **`findOne` result assigned as non-optional** — `Collection.findOne` returns
  `targetType|Error?` (nilable — nil means no match), so `check` alone narrows to
  `DeliveryDoc?`, not `DeliveryDoc`; assigning that into a non-optional variable is a type
  error. Fixed in `delivery-service/db.bal`'s `getDelivery` to check for `()` and return a
  proper "not found" error instead.

All four are the kind of thing that would have shown up immediately as a real `bal build`
error, so they're fixed with the same confidence as the six bugs from the original pass —
this wasn't a guess, it was checked line-by-line against the actual connector source. The
`ballerinax/postgresql` `Client` init, `execute`/`query`/`queryRow`, and `sql:queryConcat`
usage across `customer-service`, `restaurant-service`, `order-service`, and
`payment-service` were also checked against the real `postgresql` connector source and
match its signatures — no changes needed there. Same for `Collection.find`/`insertOne` and
the `Update`/`FindOptions` record shapes used elsewhere in the Mongo services.

Bottom line: there's no longer a specific known-suspect line to check first once you get a
real `bal build` running — the connector-facing code has been checked against the actual
current API, not just written to look right. The only thing that can still surprise you is
something version-specific to whatever exact connector version Central resolves (this was
checked against each repo's latest release branch, e.g. postgresql v1.19.1), which is a much
smaller risk than a fully-unverified integration.

## Known simplifications (be ready to name these in the defence)

- Driver dispatch is "first available driver," not nearest-by-distance — the bonus route
  optimization would replace `findAvailableDriver()` in `delivery-service/db.bal`.
- Order/payment/restaurant data all share one Postgres instance for simplicity; each
  service still only touches its own tables (see `infra/postgres-init/001-schema.sql`).
- Admin Service timestamps some transitions (confirmed/rejected/preparing/ready/cancelled)
  at the moment its own consumer processes the event rather than the moment the
  originating service emitted it, since those event payloads don't carry their own
  timestamp — `delivery.assigned`/`delivery.completed` do carry real timestamps, so
  delivery-performance numbers are accurate; restaurant prep-time numbers are a close
  approximation.
- No outbox pattern: each service persists to its DB, then publishes to Kafka as a
  second step. A crash between those two steps would lose the event. Worth mentioning as
  a "what we'd add for production" point.
