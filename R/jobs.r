#' Create a new query job.
#'
#' This is a low-level function that creates a query job. To wait until it is
#' finished and then retrieve the results, see \code{\link{query_exec}}
#'
#' @param query SQL query string
#' @param project project name
#' @param destination_table (optional) destination table for large queries,
#'   either as a string in the format used by BigQuery, or as a list with
#'   \code{project_id}, \code{dataset_id}, and \code{table_id} entries
#' @param create_disposition behavior for table creation.
#'   defaults to \code{"CREATE_IF_NEEDED"},
#'   the only other supported value is \code{"CREATE_NEVER"}; see
#'   \href{https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.createDisposition}{the API documentation}
#'   for more information
#' @param write_disposition behavior for writing data.
#'   defaults to \code{"WRITE_EMPTY"}, other possible values are
#'   \code{"WRITE_TRUNCATE"} and \code{"WRITE_APPEND"}; see
#'   \href{https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.writeDisposition}{the API documentation}
#'   for more information
#' @param default_dataset (optional) default dataset for any table references in
#'   \code{query}, either as a string in the format used by BigQuery or as a
#'   list with \code{project_id} and \code{dataset_id} entries
#' @param useLegacySql (optional) set to \code{FALSE} to enable BigQuery's standard SQL.
#' @param maximum_billing_tier (optional) customize the billing tier to
#'   allow High-Compute queries; see
#'   \href{https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.query.maximumBillingTier}{the API documentation}
#'   for more information
#' @family jobs
#' @return a job resource list, as documented at
#'   \url{https://developers.google.com/bigquery/docs/reference/v2/jobs}
#' @seealso API documentation for insert method:
#'   \url{https://developers.google.com/bigquery/docs/reference/v2/jobs/insert}
#' @export
insert_query_job <- function(query, project, destination_table = NULL,
                             default_dataset = NULL,
                             create_disposition = "CREATE_IF_NEEDED",
                             write_disposition = "WRITE_EMPTY",
                             useLegacySql = TRUE,
                             maximum_billing_tier = NULL) {
  assert_that(is.string(project), is.string(query))

  url <- sprintf("projects/%s/jobs", project)
  body <- list(
    configuration = list(
      query = list(
        query = query,
        useLegacySql = useLegacySql
      )
    )
  )

  if (!is.null(destination_table)) {
    if (is.character(destination_table)) {
      destination_table <- parse_table(destination_table, project_id = project)
    }
    assert_that(is.string(destination_table$project_id),
                is.string(destination_table$dataset_id),
                is.string(destination_table$table_id))
    body$configuration$query$allowLargeResults <- TRUE
    body$configuration$query$destinationTable <- list(
      projectId = destination_table$project_id,
      datasetId = destination_table$dataset_id,
      tableId = destination_table$table_id
    )
    body$configuration$query$createDisposition <- create_disposition
    body$configuration$query$writeDisposition <- write_disposition
  }

  if (!is.null(default_dataset)) {
    if (is.character(default_dataset)) {
      default_dataset <- parse_dataset(default_dataset, project_id = project)
    }
    assert_that(is.string(default_dataset$project_id),
                is.string(default_dataset$dataset_id))
    body$configuration$query$defaultDataset <- list(
      projectId = default_dataset$project_id,
      datasetId = default_dataset$dataset_id
    )
  }

  if (!is.null(maximum_billing_tier)) {
    body$configuration$query$maximumBillingTier <- maximum_billing_tier
  }

  bq_post(url, body)
}

#' Check status of a job.
#'
#' @param project project name
#' @param job job id
#' @return a job resource list, as documented at
#'   \url{https://developers.google.com/bigquery/docs/reference/v2/jobs}
#' @seealso API documentation for get method:
#'   \url{https://developers.google.com/bigquery/docs/reference/v2/jobs/get}
#' @seealso \code{\link{wait_for}} to wait for a job to complete
#' @family jobs
#' @export
get_job <- function(project, job) {
  assert_that(is.string(project), is.string(job))

  url <- sprintf("projects/%s/jobs/%s", project, job)
  bq_get(url)
}


#' Wait for a job to complete, optionally printing updates
#'
#' @param job job to wait for. Probably result of \code{\link{insert_query_job}}
#'   or \code{\link{insert_upload_job}}
#' @param quiet if \code{FALSE} print informative progress messages, if
#'   \code{TRUE} is silent, if \code{NA} displays messages for long-running
#'   jobs.
#' @param pause amount of time to wait between status requests
#' @family jobs
#' @export
wait_for <- function(job, quiet = getOption("bigrquery.quiet"), pause = 0.5) {
  elapsed <- timer()
  is_quiet <- function(x) isTRUE(quiet) || (is.na(quiet) && elapsed() < 2)

  while(job$status$state != "DONE") {
    if (!is_quiet()) {
      cat("\rRunning query:   ", job$status$state, " ",
        sprintf("%4.1f", elapsed()), "s", sep = "")
    }
    Sys.sleep(pause)
    job <- get_job(job$jobReference$projectId, job$jobReference$jobId)
  }
  if (!is_quiet()) cat("\n")

  err <- job$status$errorResult
  if (!is.null(err)) {
    # error message is sufficient for now, could also pull more detailed reason
    stop(err$message, call. = FALSE)
  }

  if (!is_quiet()) {
    if ("load" %in% names(job$config)) {
      in_bytes <- as.numeric(job$statistics$load$inputFileBytes)
      out_bytes <- as.numeric(job$statistics$load$outputBytes)
      message(format(size_units(in_bytes)), " input bytes")
      message(format(size_units(out_bytes)), " output bytes")
    } else if ("query" %in% names(job$config)) {
      bytes <- as.numeric(job$statistics$totalBytesProcessed)
      message(format(size_units(bytes)), " processed")
    }
  }

  job
}

size_units <- function(x) {
  i <- floor(log2(x) / 10)
  unit <- c("", "kilo", "mega", "giga", "tera", "peta", "exa", "zetta", "yotta")[i + 1]

  structure(x, i = i, unit = unit, class = "size")
}
#' @export
format.size <- function(x, ...) {
  if (x == 0) return("0 bytes")

  y <- x * 1024 ^ -attr(x, "i")
  sprintf("%.1f %sbytes", y, attr(x, "unit"))
}
