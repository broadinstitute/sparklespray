TIMESTAMP=`date +'%M%m%H%S'`

cat > sample-job2.json <<EOF
{
  "name": "sample-${TIMESTAMP}",
  "resources": [{ "name": "slots", "value": 1 }],
  "filesToLocalize": [
    {
      "source": "gs://sparkles-test-0625/temp/simulate_load.py",
      "destination": "simulate_load.py"
    }
  ],
  "tasks": [
    {
      "dockerImage": "python",
      "command": [
        "python",
        "simulate_load.py",
        "--cpu_fraction=50",
        "--min_mem=20M",
        "--max_mem=4000M",
        "--run_time=600",
        "--output_size=1M",
        "--period=60"
      ]
    }
  ]
}
EOF

go run cmd/sparkles/main.go dev submit sample-job2.json sample-workpool.json --project sparkles-test-0625 --gcs-prefix gs://sparkles-test-0625/jobs
