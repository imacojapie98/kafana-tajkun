# Jovanica Manager v7 — messaging development branch

This is a **staging prototype**, not a deployed bot and not yet wired into
`imacojapie/jovanica_manager` production code. This folder is deliberately
kept separate from `kafana-tajkun/main` and does not contain customer data,
credentials, or copied production databases.

## Implemented

- Luna structured `{ "messages": ["bubble 1", "bubble 2"] }` and legacy
  `{ "reply": "..." }` normalization (1–3 bubbles; errors fail closed)
- Persistent SQLite outbox and unique response IDs to avoid duplicated jobs
- Nonblocking per-bubble typing indicators and calculated delays
- In-order Telegram Business messages with `business_connection_id`
- Cancellation of pending/typing bubbles after new incoming fan messages
- No blind retry of sends with uncertain Telegram delivery outcomes
- Restart recovery and pause/eligibility callback

## Integration requirements (NOT YET DONE)

1. Copy `jovanica_v7/` into a development checkout of the original bot.
2. During its database initialization, construct `BubbleOutbox(base.DB)` and
   call `.recover()` once before processing updates.
3. When an incoming user message is accepted, call `.interrupt(cid, chat)`
   immediately, before the AI request or batching logic runs.
4. In the Wiro generation path, adapt the Luna output contract to include
   `messages` while retaining existing JSON memory and sales classifications.
5. After the response is validated and the strategy/offer gates are applied,
   call `.enqueue(cid, chat, job_token, prepared_bubble_texts)`. Expand approved
   links via the existing `outgoing()` helper **before** enqueueing.
6. Instead of the existing one-shot `sendMessage` in `tick()`, call
   `bubble_outbox.tick(base.api, allowed=allowed, now=...)` after draining
   incoming events. The existing `allowed` takes `(cid,key,include_sales)`,
   so wrap it as `lambda cid,chat: allowed(cid,chat,False)`.
7. Persist assistant history, profile memory and successful sales-link state
   only after the final bubble is confirmed `sent`. Handle `uncertain` and
   interrupted delivery without falsely marking content as delivered.
8. Run integration tests using a fake Telegram API, then an opted-in test
   account. **Do not enable production auto-replies until tested.**

## Run tests

From the folder containing this README:

```bash
python -m unittest discover -s tests -v
```

## Operational caution

Telegram requires a valid Business connection and the applicable reply window.
The existing bot's `base.eligible(...)` check must remain in the allowed path.
This is a staging building block, not a full multi-process distributed queue.
Run a single sender process per database; future horizontal scaling should
use a transactional worker claim mechanism or a queue service.