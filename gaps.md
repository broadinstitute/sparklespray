Things to fix:

1. Collect a history of metrics so that when metrics are requested we can immediately see the last n minutes worth
2. tasks and jobs should have a UUID which can be used for caching to avoid current confusion arising from resubmissions
3. add more variance to the simload script (And random print statements to excercise logging)
4. add more variance to cpu usage in simload script
5. Big one: revisit how summaries are computed. Presently largely event driven. Switch to polling model. (Maybe store collection of "StaleSummary" which is "put" on each update. Poll can query on each poll. Would give us a quick way to determine no work to be done at the cost of extra writes.)
6. Big one: revisit how we poll
