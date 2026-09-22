
#scp bin/sprinkles-linux-amd64-v100.0.0-2-ge81c1dc-dirty container-os-test:~pmontgom/bin/sp

ssh container-os-test ./bin/sp dev test-profile-command --docker-arg -v --docker-arg /home/pmontgom:/home/pmontgom python python /home/pmontgom/simulate_load.py --shape square --period 60 --start max


