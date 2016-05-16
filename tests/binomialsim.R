b.scatter <- function() {
  list(inputs=seq(10), shared=NULL)
}

b.map <- function(input, shared) {
  return(input*2)
}

b.gather <- function(files, shared) {
  values <- sapply(files, function(fn) { readRDS(fn) } ) 
  writeLines(as.character(values), "results/final.txt")
}

