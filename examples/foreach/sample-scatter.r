# function for converting factors to one-hot columns
one.hot <- function(x) {
  sapply( levels(x), function(level) {
    as.numeric(x == level)
  })
}

# regress the target feature using all features except itself
fit.and.get.rmsq <- function(target.index, features) {
  model <- lm.fit(features[,-target.index], features[,target.index])
  # return RMSE
  mean(model$residuals ** 2) ** 0.5
}

# returns the list of elements to compute foreach on and additional arguments to pass to foreach
get_foreach_args <- function() {
  data(iris)

  # turn species from catagorical to one-hot encoded and get a numeric matrix of features
  features <- as.matrix(cbind(iris[,1:4], one.hot(iris[,5])))
  
  list(elements=seq(ncol(features)), extra_args=list(features))  
}

# evaluated once per element return from scatter()
foreach <- function(i, features) {
  data.frame(target=colnames(features)[[i]], rmse=fit.and.get.rmsq(i, features))
}

# evaluated on the list of values returned from all foreach calls
gather <- function(results, ...) {
 print("results")
 str(results)
  df <- do.call(rbind, results)
  write.csv(df, file="results.csv")
}
