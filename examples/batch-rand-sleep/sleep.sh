VAR_SLEEP="$1"
for i in {1..60}; do
  echo "Sleeping for $VAR_SLEEP seconds"
  sleep $VAR_SLEEP
  date
done
echo 'done'
