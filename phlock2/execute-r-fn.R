execute.function.foreach.arg <- function(func.name, args) {
  env <- parent.env(environment())
  func.to.call <- env[[func.name]]
  if(is.null(func.to.call)) {
    stop(sprintf("could not find function named '%s'", func.name))
  }
  if(!is.function(func.to.call)) {
    stop(sprintf("'%s' was not a function", func.to.call))
  }
  return ( lapply(args, function(arg) { do.call(func.to.call, arg) } ) )
}

phlock.exec.scatter <- function(func.name) {
  scatter.params <- rjson::fromJSON(file="scatter-in/params.json")

  results <- execute.function.foreach.arg(func.name, list(scatter.params))

  stopifnot(length(results) == 1)
  results <- results[[1]]
  stopifnot(length(results) == 2)
  stopifnot(length(intersect(names(results), c("shared", "inputs")))==2)

  saveRDS(results$shared, "shared/state.rds")
  id.fmt.str <- sprintf("%%0%.0f.0f", ceiling(log(length(results$inputs)+1)/log(10)));
#  filenames = c()
  for( i in seq_along(results$inputs) ) {
    fn <- sprintf(id.fmt.str, i)
    saveRDS(results$inputs[[i]], paste0("map-in/", fn))
#    filenames <- c(filenames, fn)
  }
#  writeLines(filenames, "map-in/input-names.txt")
}

phlock.exec.map <- function(func.name) {
  shared <- readRDS("shared/state.rds")
  fns <- list.files("map-in", pattern='^[0-9]+$')

  args <- lapply(fns, function(fn) {
    list(input=readRDS(paste0("map-in/", fn)), shared=shared)
  })

  outputs <- execute.function.foreach.arg(func.name, args)
  for( i in seq_along(fns) ) {
    saveRDS(outputs[[i]], paste0("map-out/", fns[[i]]))
  }
}

phlock.exec.gather <- function(func.name) {
  shared <- readRDS("shared/state.rds")
  fns <- paste0("map-out/", sort(list.files("map-out", pattern='^[0-9]+$')))
  execute.function.foreach.arg(func.name, list(list(files=fns, shared=shared)))
}

