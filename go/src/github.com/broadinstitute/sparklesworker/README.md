Bugs: initial timeout doesn't seem to be honored. Regardless, should be able
to do it a different way: One first poll, get job which includes the task
count. Get tasks independant of status and compare count. Repeat until
timeout or task count agrees.


TODO: 
    - add support for reserved capacity
    - add support for honoring job killed flag
    - test what happens when OOM occurs
    