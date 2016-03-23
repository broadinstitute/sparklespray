#!/usr/bin/env Rscript

execute.function.from.files.foreach.arg <- function(files.to.source, func.name, args, env) {
  for(f in files.to.source) {
    source(f, local=env)
  }

  func.to.call <- env[[func.name]]
  if(is.null(func.to.call)) {
    stop(sprintf("could not find function named '%s'", func.name))
  }
  if(!is.function(func.to.call)) {
    stop(sprintf("'%s' was not a function", func.to.call))
  }
  return ( lapply(args, function(arg) { do.call(func.to.call, arg, envir=env) } ) )
}

vargs <- commandArgs(trailingOnly=T)
operation <- vargs[1]
if(operation == "scatter") {
  func.name <- vargs[2]
  files.to.source <- vargs[3:length(vargs)]

  env <- new.env(parent=parent.frame())
  results <- execute.function.from.files.foreach.arg(files.to.source, func.name, list(list()), env)
  stopifnot(length(results) == 1)
  task.list <- results[[1]]

  dir.create("shared", showWarnings=F)
  saveRDS(task.list$shared, file="shared/state.rds")
  dir.create("map-inputs")

  inputs <- task.list$inputs
  stopifnot(!is.null(inputs))

  for(i in seq_along(inputs)) {
    out.fn <- sprintf("map-inputs/%d.rds", as.integer(i))
    task.input <- inputs[[i]]
    saveRDS(task.input, file=out.fn)
  }
} else if(operation == "map") {
  task.index <- as.integer(vargs[2])
  input.fn <- sprintf("map-inputs/%d.rds", task.index)
  func.name <- vargs[3]
  files.to.source <- vargs[4:length(vargs)]

  shared.state <- readRDS("shared/state.rds")
  task.input <- readRDS(input.fn)
  args <- list(task.input, shared.state)

  env <- new.env(parent=parent.frame())
  results <- execute.function.from.files.foreach.arg(files.to.source, func.name, list(args), env)
  stopifnot(length(results) == 1)
  output <- results[[1]]
  dir.create("map-outputs")
  saveRDS(output, sprintf("map-outputs/%d.rds", task.index))

} else if(operation == "gather") {
  output.filenames.file <- vargs[2]
  func.name <- vargs[3]
  files.to.source <- vargs[4:length(vargs)]

  output.filenames <- readLines(output.filenames.file)

  shared.state <- readRDS("shared/state.rds")
  args <- list(lapply(output.filenames, readRDS), shared.state)

  env <- new.env(parent=parent.frame())
  execute.function.from.files.foreach.arg(files.to.source, func.name, list(args), env)  
} else {
  stop(sprintf("unknown operation: %s", operation))
}
