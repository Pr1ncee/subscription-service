# subscriptions-service

Subscription payments and notifications service.

## Worker

`python -m subscriptions_service.worker` runs the notification scheduler and the outbox
relay. On startup it connects to RabbitMQ and declares the full topology (exchanges,
queues, bindings), so events published before any consumer has started are kept.

If RabbitMQ is unreachable **at startup**, the worker exits with a non-zero status and the
supervisor (compose `restart: unless-stopped`) restarts it. Once running, broker outages are
absorbed: a failed publish is retried with exponential backoff and the event stays pending
in the outbox until it is confirmed.
