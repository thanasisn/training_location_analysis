#' Add a data table to a table in database
#'
#' @param con    Connection to the database
#' @param table  Name of the table to use in the database
#' @param data   Data table to add in the database
#'
#' @return
#'
append_to_table <- function(con, table, data) {

  ## -- Add new columns in the db table if not there ---------------------------
  if (dbExistsTable(con, table)) {

    ## detect data types
    tt1 <- data.table(names = colnames(tbl(con, table)),
                      types = tbl(con, table) |> head(1) |> collect() |> sapply(class))
    dd1 <- data.table(names = colnames(data),
                      types = data |> head(1) |> collect() |> sapply(class))

    ##  different methods to get data types
    # tt2 <- data.table(names = colnames(tbl(con, table)),
    #                   types = tbl(con, table) |> head(1) |> collect() |> sapply(typeof))
    # dd2 <- data.table(names = colnames(data),
    #                   types = data |> head(1) |> collect() |> sapply(typeof))

    if (!all(dd1$names %in% tt1$names)) {
      ## get new variables
      new_vars <- dd1[!names %in% tt1$names, ]
      cat("New", new_vars$names)

      for (i in 1:nrow(new_vars)) {

        ## translate data types to duckdb
        ctype <- switch(paste0(unlist(new_vars$types[i]), collapse = ""),
                        POSIXctPOSIXt = "datetime",
                        unlist(new_vars$types[i]))

        cat("\nNEW VAR:", paste(new_vars[i, ]), "\n")

        ## create new columns with a query
        qq <- paste("ALTER TABLE", table,
                    "ADD COLUMN",  new_vars$names[i], ctype, "DEFAULT null")
        dbSendQuery(con, qq)
      }
    }
  }

  dbWriteTable(
    con,
    table, data,
    append = TRUE
  )
}
