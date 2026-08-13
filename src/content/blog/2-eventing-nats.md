---
title: 'From Channels to Native JetStream: Rebuilding the Knative Eventing Broker on NATS'
excerpt: The Knative Broker API is a simple promise — push events in, filter them, fan them out. We kept that promise by stacking a Broker on top of a Channel. Here is why we tore the stack down and rebuilt the broker to speak JetStream natively, and what disappeared along the way.
publishDate: 'Aug 13 2026'
tags:
  - Knative
  - Eventing
  - NATS
  - Architecture
seo:
  pageType: article
---

The Knative Eventing **Broker** makes a simple promise to application developers: send an event to one URL, declare a **Trigger** with a filter, and matching events show up at your service. You never think about the transport.

For a long time we kept that promise on NATS by stacking the broker on top of a **Channel**. It worked — but "it works" was hiding a surprising amount of machinery. This post is about replacing that stack with a broker that speaks **JetStream** natively: the same Knative API on the surface, dramatically less underneath.

The thesis up front: **identical Broker/Trigger surface, far fewer moving parts, and JetStream used for what it is actually good at** — persistence and consumer semantics — instead of being buried behind HTTP hops between pods.

## The old world: a broker balanced on a channel

The previous setup was upstream Knative's **MT Channel-Based Broker** (`MTChannelBasedBroker`) configured with a `NatsJetStreamChannel` as its channel template:

```yaml
# config-br-default-channel-jsm.yaml
data:
  channelTemplateSpec: |
    apiVersion: messaging.knative.dev/v1alpha1
    kind: NatsJetStreamChannel
```

That one line of configuration pulls in two independent subsystems that have to cooperate.

**Control plane:**

- `mt-broker-controller` — the upstream broker/trigger reconciler.
- `jetstream-ch-controller` — this project's controller for `NatsJetStreamChannel`.

**Data plane:**

- `mt-broker-ingress` — shared HTTP entry point.
- `mt-broker-filter` — shared HTTP filtering service.
- `jetstream-ch-dispatcher` — the channel's dispatcher, which owns the NATS connection, the JetStream streams, and the per-subscription consumers.

**And per broker, a small pile of API objects:** the `Broker` itself, a `NatsJetStreamChannel` (the "trigger channel"), and a `Subscription` for every `Trigger`. The relationship a user thinks of as "Trigger → Broker" is actually `Trigger → Subscription → Channel`, spread across two controllers that must agree.

Now follow a single event through it:

```
Producer
   │ HTTP POST
   ▼
mt-broker-ingress
   │ HTTP  (writes to the trigger channel)
   ▼
NatsJetStreamChannel  (addressable Service)
   │ HTTP
   ▼
jetstream-ch-dispatcher ──publish──▶ [ JetStream stream ]
   │   (one consumer per Subscription)
   │ HTTP POST
   ▼
mt-broker-filter   (evaluates the Trigger filter)
   │ HTTP POST
   ▼
Subscriber
        (replies loop back through the channel)
```

Count the hops. JetStream — the thing we actually chose NATS for — is buried in the *middle* of the path, reachable only through the channel dispatcher. Between every box is an HTTP call. Every event, matching or not, round-trips through a shared filter service just to be told whether it should be delivered. And the whole thing only holds together because two controllers keep `Channel` and `Subscription` objects in sync.

None of this was *wrong*. It was the fastest way to get a working broker by reusing the channel we already had. But it is a lot of surface area for "receive, filter, deliver."

## The new world: the broker speaks JetStream

The native broker throws away the channel and talks to JetStream directly.

**Control plane:**

- `natsjetstream-broker-controller` — reconciles Brokers.
- the Trigger controller — reconciles Triggers into JetStream consumers.

**Data plane:**

- `nats-broker-ingress` — a single shared ingress for all brokers.
- a per-broker **filter** — pulls from JetStream and delivers.

**API objects per broker:** a `Broker` and its `Trigger`s. That's it. No `Channel`. No `Subscription`.

The same event now looks like this:

```
Producer
   │ HTTP POST
   ▼
nats-broker-ingress ──publish──▶ [ JetStream stream ]
                                      │  (durable pull consumer per Trigger)
                                      ▼
                                 per-broker filter   (evaluates the Trigger filter in-process)
                                      │ HTTP POST
                                      ▼
                                  Subscriber
```

The ingress publishes **straight into the broker's JetStream stream**. Each Trigger maps directly to a **durable pull consumer** on that stream. The filter pulls only the messages its consumers are bound to, evaluates the filter *in-process*, and makes a single HTTP call to the subscriber. JetStream is now the transport, not an implementation detail hidden behind a channel service.

## Side by side

| | Channel-based broker | Native JetStream broker |
|---|---|---|
| Controllers | `mt-broker-controller` + `jetstream-ch-controller` | `natsjetstream-broker-controller` (+ trigger controller) |
| Data-plane deployments | `mt-broker-ingress`, `mt-broker-filter`, `jetstream-ch-dispatcher` | shared `nats-broker-ingress`, per-broker `filter` |
| API objects per broker | `Broker`, `NatsJetStreamChannel`, one `Subscription` per Trigger | `Broker`, `Trigger`s |
| Inter-pod HTTP hops per delivery | ingress → channel → dispatcher → filter → subscriber | ingress → (JetStream) → subscriber |
| Where JetStream sits | inside the channel dispatcher, mid-path | the transport itself |
| Where filtering happens | shared HTTP filter service, fan-in | in-process, next to the consumer |
| Trigger wiring | Trigger → Subscription → Channel (two controllers) | Trigger → JetStream consumer (one controller) |

**What went away, and why it matters:**

- **No `Channel`/`Subscription` indirection.** One controller owns the broker's world instead of two coordinating through intermediate objects. Fewer objects, fewer race windows, less to reason about when something is stuck.
- **Fewer inter-pod HTTP hops.** Every hop is latency and a failure point. Publishing directly to JetStream and pulling from it removes a service and a dispatcher from the path.
- **Filtering moved to the consumer.** Instead of every event fanning into a shared filter over HTTP, each filter pulls only what its trigger consumers match and decides in-process.

**The honest trade-offs:** the native broker runs a filter deployment *per broker* rather than one shared filter, and it assumes JetStream is doing the heavy lifting (stream retention, replication) rather than a reusable channel abstraction. For a cluster with many tiny brokers that's a different resource profile — which is exactly why the filter is created lazily (more on that below).

## How the new broker works (the short version)

Three moving parts, one paragraph each.

**Shared ingress + a contract ConfigMap.** The ingress routes by path — `/{namespace}/{name}` maps to a broker's JetStream publish subject. That mapping lives in a "contract" ConfigMap that the ingress watches through an informer, so routing hot-reloads with zero downtime and stays completely decoupled from broker reconciliation. The broker controller writes contract entries; the ingress reads them.

**Broker controller.** It reconciles one JetStream stream per broker (a deterministic name and a `publishSubject.>` subject pattern) and wires up the contract entry. Notably, it creates the per-broker filter **only when at least one Trigger exists**, and tears it down when the last Trigger is removed — an efficiency the channel model couldn't express, since there the dispatcher was always on. Crucially, the ingress and the stream are reconciled unconditionally, so events are accepted and **stored** even for a broker that has no triggers yet; a filter created later just picks up the buffered messages.

**Per-broker filter.** For each Trigger it binds a durable pull consumer, applies the trigger's filter in-process, and delivers to the subscriber with retry and dead-letter handling. Trigger delivery config maps to JetStream redelivery (retry count → `MaxDeliver`, timeout → `AckWait`).

## Making it *feel* native

Simple internals only count if the behavior is correct. A few details that took real care:

**Delivery semantics.** Both the `Broker` and the `Trigger` can specify a `Delivery` (retries, backoff, dead-letter sink). Following Knative semantics, a Trigger's delivery overrides the Broker's **wholesale** — if the Trigger sets any delivery field, its spec is used in its entirety; otherwise the Trigger inherits the Broker's. When a Trigger inherits the broker's dead-letter sink, the full resolved address is carried over — including CA certs and OIDC audience — so delivery to a TLS- or OIDC-protected sink actually works.

**Trace propagation.** The broker preserves the producer's CloudEvents distributed-tracing extension instead of stamping it with its own hop. That matches the extension spec, which requires the attribute to "carry the trace information of the starting trace of the transmission … it MUST NOT carry trace information of each individual hop." An intermediary may *add* the extension when the producer omitted one, but it must not overwrite the originating trace — so the ingress keeps an existing `traceparent`/`tracestate` untouched.

**Retention that matches expectations.** Streams default to `Limits` retention, so events published to a broker are retained even before any consumer (trigger) exists — the "store now, deliver when a subscriber shows up" behavior people expect from a durable broker.

**Observability.** Each dispatch is wrapped in a span, and the filter emits duration histograms — `dispatch` (time on the wire to the subscriber) and `process` (decode + filter evaluation before dispatch) — with explicit bucket boundaries aligned to the channel dispatcher's metric names, so channel and broker latency aggregate cleanly.

## Wrapping up

The Broker/Trigger API didn't change — user manifests are identical. What changed is everything under the line: a broker balanced on a channel, with two controllers and three data-plane deployments passing events over HTTP, became a broker that publishes to a JetStream stream and pulls from it. Fewer components, fewer hops, and NATS doing what NATS is good at.

If you're running the channel-based NATS broker today, the native broker is a drop-in at the API level. The interesting part is what you get to delete.
