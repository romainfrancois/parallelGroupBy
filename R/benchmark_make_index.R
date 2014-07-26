
NAMESPACE <- environment()

implementations <- ls( pattern = "^make_index_", envir = NAMESPACE )

benchmark_make_index <- function(){
  print(implementations)
}


