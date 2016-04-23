steps:
    1. support submission of single proc locally w/o docker
    2. support submission of single proc locally w docker
    3. support submission of single proc over nomad w docker
    4. restore submission of scatter/gather
    5. make cromwell submission layer?

tree:
    code
    state
        map-count
    task-completions
        scatter 
        map-1
        map-2
        ...
        gather
    shared
        ...
    map-inputs
    map-outputs
    results

code = state-scatter
shared = state-map
better names?

scatter:
```
    dl FUNC_DEFS
    run ["Rscript", r_exec_script, "scatter", scatter_func_name, FUNC_DEFS]
    ul completion_path = "task-completions/scatter"
    ul shared
    ul map-inputs
    ul results
```

mapper:
```
    dl FUNC_DEFS
    dl shared
    dl "map-inputs/"+str(task_index)+".rds"
    run ["Rscript", r_exec_script, "map", str(task_index), mapper_func_name, FUNC_DEFS]
    ul completion_path = "task-completions/map-{}".format(task_index)
    ul results
    ul map-outputs
```

gather:
```
    dl FUNC_DEFS
    dl shared
    dl map-outputs
    run ["Rscript", r_exec_script, "gather", "mapper-outputs.txt", gather_func_name, FUNC_DEFS]
    ul results
    ul completion_path = task-completions/gather
```

