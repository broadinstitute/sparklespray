fruit_paths=`gcloud storage ls "$1/*/fruit.txt"`
for ii in $fruit_paths; do
    gcloud storage cat $ii >> merged_list.txt
done
