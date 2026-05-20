sparkles sub -i python -n fakeload-`date +%M%S` --label minute=`date +%M%S` --seq 5 -u simload.py 'python simload.py {index}'
