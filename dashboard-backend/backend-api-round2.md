The goal is to allow the dashboard to avoid having to do frequent polling of the backend yet, give near-realtime updates.

To support this, the backend will support creating subscriptions and will make blocking requests which wait until a new event(s) are availible.

## Life cycle events

The following endpoints will be used for supporting subscriptions on life cycle events:

POST /subscription
Optional parameters: types

Create a subscription for being notified of any infrastructure events (ie: new jobs, task life cycle events, worker life cycle events etc), returning an ID and urls to use for polling for events.

See the section "Subscription responses" below for the format of the response payload.

POST /subscription/{subscription_id}/unsubscribe

Cancel the subscription

## Task performance/log events

POST /task/{id}/subscription
Optional parameters: types

Create a subscription for task metric and log events. These are handled seperately from life cycle events because the volume is potentially much higher. If we have 1000 tasks running, each one could potentially be publishing performance and log data, but it's such a large amount of data, it's not actually valuable to publish all of it since we have no plans to show all of it.

Instead, we have the use case were someone might one to view a feed from a single task. Instead of using the policy of always publishing all event (such as is used by life cycle events), we have a policy of requiring clients request metric/log task events start being published.

This end point should do two things:

1. Create a subscription which the caller can use to be notified of updates.
2. Tell the task to start publishing profiling and log events.

See the section "Subscription responses" below for the format of the response payload.

POST /task/{id}/subscription/{subscription_id}/unsubscribe

Cancel the subscription

## Subscription responses

When a subscription is requested, we return a json object with the following fields:

- `subscription_id`: The ID used for unsubscribing.
- `pull_url`: The URL which the client should use for retreiving messages
- `ack_url`: The URL which the client should use for acknowledging message IDs.
- `authorization_token`: The bearer token which should be used when making requests to the two urls above.

The urls returned will actually be references to GCP pubsub endpoints, so their request and response payloads are not defined here. The idea is by making the browser directly communicate with the pubsub service, we can avoid having this backend make and long-open connections. This approach will allow somewhat make the abstraction of how subscriptions work a bit leaky, but should be more efficient than trying to make the backend to act as a facade when pulling events.
