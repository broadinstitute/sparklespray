scatter <- function() { 
    print("scatter")
  list(inputs=list(1,2,3,4))
}

mapper <- function(input, shared) {
  cat("Running mapper(", input, ")\n", sep="")
  input * 2
}

gather <- function(outputs, shared) {
  cat("Running gather(", as.numeric(outputs), ")\n", sep="")
  dir.create("results")
  writeLines(sprintf("%s", sum(as.numeric(outputs))), "results/sum.txt")
}
