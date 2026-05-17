Things to fix:

1. Collect a history of metrics so that when metrics are requested we can immediately see the last n minutes worth
2. Big one: revisit how we poll in the browser. Do we like the listen to a subscription model? Should we instead try leveraging firestore listeners and make mulitple pushes.
3. worker pool summary on the right is not correctly identifing workers which are running. Why?
4. potentially large: tasks and jobs should have a UUID which can be used for caching to avoid current confusion arising from resubmissions
5. Job cancellation
6. Simulate memory exhaustion
7. Migrate API to firestore
