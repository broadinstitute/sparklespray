The goal is to allow the dashboard to avoid having to do frequent polling of the backend yet, give near-realtime updates.

To support this, the backend will support creating subscriptions and the client will make blocking requests (to pubsub pull) which wait until a new event(s) are availible.

## Life cycle events

The following endpoints will be used for supporting subscriptions on life cycle events:

POST /subscription
Optional query parameters: types

Create a subscription for being notified of any infrastructure events (ie: new jobs, task life cycle events, worker life cycle events etc), returning an ID and urls to use for polling for events.

See the section "Subscription responses" below for the format of the response payload.

POST /subscription/{subscription_id}/unsubscribe

Cancel the subscription

### Implementation notes:

Life cycle events should be published to the pubsub topic "sparklespray-events", so the subscriptions created should be listening for events on that topic.

## Task performance/log events

POST /task/{id}/subscription
Optional query parameters: types

Create a subscription for task metric and log events. These are handled seperately from life cycle events because the volume is potentially much higher. If we have 1000 tasks running, each one could potentially be publishing performance and log data, but it's such a large amount of data, it's not actually valuable to publish all of it since we have no plans to show all of it.

Instead, we have the use case were someone might one to view a feed from a single task. Instead of using the policy of always publishing all event (such as is used by life cycle events), we have a policy of requiring clients request metric/log task events start being published.

This end point should do two things:

1. Create a subscription which the caller can use to be notified of updates.
2. Tell the task to start publishing profiling and log events.

See the section "Subscription responses" below for the format of the response payload.

POST /task/{id}/subscription/{subscription_id}/unsubscribe

Cancel the subscription

### Implementation notes

Task log/metric events should be published to the topic "sparkles-task-out"

To request the task start publishing log and metric events we should send a "control message" to the pubsub topic "sparklespray-task-in". Currently the only control message we want to define are:

```
interface StartPublishing {
    type : "start_publishing"
    req_id: str # request ID
    task_id: str
}
```

The task's worker will also publish a message on "sparkles-task-out" with attribute "req_id" and the "type" attribute will be "command_ack" to let the client know the command was processed. (However, the backend should not wait for this ack. starting publishing is best effort)

## Subscription responses

When a subscription is requested, we return a json object with the following fields:

- `subscription_id`: The ID used for unsubscribing (and is actualy the GCP subscription name so we don't have to maintain any additional state)
- `pull_url`: The URL which the client should use for retreiving messages
- `ack_url`: The URL which the client should use for acknowledging message IDs.
- `authorization_token`: The bearer token which should be used when making requests to the two urls above.

The urls returned will actually be references to GCP pubsub endpoints, so their request and response payloads are not defined here. The idea is by making the browser directly communicate with the pubsub service, we can avoid having this backend make and long-open connections. This approach will allow somewhat make the abstraction of how subscriptions work a bit leaky, but should be more efficient than trying to make the backend to act as a facade when pulling events.

The authorization token will be a short leveed access token for service acount impersonation, where it will impersonate a service account which only has access to pull/ack the topics mentioned here.

When creating subscriptions, we should create pubsub subscriptions which only keep their contents for a day. Also, if we can make a pubsub subscription which can self-expire, we should do that. We don't want to rely on the front end always cleaning up subscriptions as the browser/user may navigate away and never give the frontend a chance to clean up.

Also, note that when this document says events will be published, they are actually just notifications of events having been written to datastore. The actual message from the pubsub topic will consist of a blank body and will have an attribute for "type". This attribute will allow the backend to create a subscription on only the types the client is interested in. (ie: the backend create the Pub/Sub subscription with a filter expression like attributes.type = "job_started" OR attributes.type = "task_claimed")
