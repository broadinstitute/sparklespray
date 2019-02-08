args = commandArgs(trailingOnly=TRUE)

get_function <- function(script_filename, function_name) {
    envir <- new.env()
    sys.source(script_filename, envir=envir)
    f <- envir[[function_name]]
    if(is.null(f)) {
        stop(paste0("Could not find function named ", function_name, " in ", script_filename))
    }

    f
}

scatter_cmd <- function(script_filename, function_name, package_filename, element_count_filename, script_args) {
    f <- get_function(script_filename, function_name)

    result <- do.call(f, script_args)
    stopifnot(is.list(result))
    stopifnot(!is.null(result$elements))

    saveRDS(result, package_filename)

    cat(length(result$elements), file=element_count_filename)
}

foreach_cmd <- function(script_filename, function_name, package_filename, results_filename, start_index, stop_index) {
    f <- get_function(script_filename, function_name)

    input <- readRDS(package_filename)

    results <- list()
    for(i in seq(start_index, stop_index-1)) {
        args <- c(list(input$elements[[i+1]]), input$extra_args)
        result <- do.call(f, args)
        results[[length(results)+1]] <- result
    }

    saveRDS(list(start_index=start_index, stop_index=stop_index, results=results), file=results_filename)
}

gather_cmd <- function(script_filename, function_name, results_dir, element_count, package_filename) {
    f <- get_function(script_filename, function_name)

    # read the package to get the extra_args
    input <- readRDS(package_filename)

    # read each result file and assemble the full list
    all_results <- vector(mode = "list", length = element_count)

    for(fn in list.files(results_dir)) {
        # print(fn)
        block <- readRDS(file.path(results_dir, fn))
        # str(block)
        for(i in seq(block$start_index, block$stop_index-1)) {
            # print(i)
            # print(block$start_index)
             all_results[[i+1]] <- block$results[[i-block$start_index+1]]
            #  print(paste0("updating ", i+1))
        }
    }

    # print("----")
    # str(all_results)
    # print("----")
    # str(input$extra_args)

    # invoke gather function
    args <- c(list(all_results), input$extra_args)
    # print("here")
    # str(args)
    do.call(f, args)
}

if(args[[1]] == "scatter") {
    stopifnot(length(args) >= 5)
    script_filename <- args[[2]] 
    function_name <- args[[3]]
    package_filename <- args[[4]]
    element_count_filename <- args[[5]]
    if(length(args) > 5) {
        script_args <- as.list(args[6:length(args)])
    } else {
        script_args <- list()
    }
    scatter_cmd(script_filename, function_name, package_filename, element_count_filename, script_args)
} else if (args[[1]] == "foreach") {
    script_filename <- args[[2]] 
    function_name <- args[[3]]
    package_filename <- args[[4]]
    results_filename <- args[[5]]
    start_index <- as.integer(args[[6]])
    stop_index <- as.integer(args[[7]])
    foreach_cmd(script_filename, function_name, package_filename, results_filename, start_index, stop_index)
} else if (args[[1]] == "gather") {
    script_filename <- args[[2]] 
    function_name <- args[[3]]
    results_dir <- args[[4]]
    element_count <- as.integer(args[[5]])
    package_filename <- args[[6]]
    gather_cmd(script_filename, function_name, results_dir, element_count, package_filename)
} else {
    stop(paste0("Unknown command: ", args[[1]]))
}

