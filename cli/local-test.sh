docker run -v $PWD/sparklespray/bin/sparklesworker:/mnt/sparklesworker -v /Users/pmontgom/.sparkles-cache/service-keys/broad-achilles.json:/mnt/servicekey.json -e GOOGLE_APPLICATION_CREDENTIALS=/mnt/servicekey.json \
    ubuntu /mnt/sparklesworker consume --localhost \
      --cacheDir /mnt/data/cache \
      --tasksDir /mnt/data/tasks --cluster c-5cb1fd7dfae41048ad04 \
      --projectId broad-achilles \
      --timeout 10 --shutdownAfter 600

# --port 6032