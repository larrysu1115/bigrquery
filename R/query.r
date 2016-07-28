#' Run a asynchronous query and retrieve results.
#'
#' This is a high-level function that inserts a query job
#' (with \code{\link{insert_query_job}}), repeatedly checks the status (with
#' \code{\link{get_job}}) until it is complete, then retrieves the results
#' (with \code{\link{list_tabledata}})
#'
#' @inheritParams insert_query_job
#' @inheritParams list_tabledata
#' @param job_info (optional) if pass in with a non-null variable, the variable will be job result details after function ends.
#'   the info contains billing bytes in "job_info$statistics$query$totalBytesBilled" .
#'   for job detail format; see
#'   \href{https://cloud.google.com/bigquery/docs/reference/v2/jobs}{Job format}
#'   for more information
#' @seealso Google documentation describing asynchronous queries:
#'  \url{https://developers.google.com/bigquery/docs/queries#asyncqueries}
#'
#'  Google documentation for handling large results:
#'  \url{https://developers.google.com/bigquery/querying-data#largequeryresults}
#' @export
#' @examples
#' \dontrun{
#' project <- "fantastic-voyage-389" # put your project ID here
#' sql <- "SELECT year, month, day, weight_pounds FROM [publicdata:samples.natality] LIMIT 5"
#' query_exec(sql, project = project)
#' # Put the results in a table you own (which uses project by default)
#' query_exec(sql, project = project, destination_table = "my_dataset.results")
#' # Use a default dataset for the query
#' sql <- "SELECT year, month, day, weight_pounds FROM natality LIMIT 5"
#' query_exec(sql, project = project, default_dataset = "publicdata:samples")
#' }
query_exec <- function(query, project, destination_table = NULL,
                       default_dataset = NULL,
                       page_size = 1e4, max_pages = 10,
                       warn = TRUE,
                       create_disposition = "CREATE_IF_NEEDED",
                       write_disposition = "WRITE_EMPTY",
                       useLegacySql = TRUE,
                       maximum_billing_tier = NULL,
                       job_info = NULL) {

  job_infoo <- TRUE

  dest <- run_query_job(query = query, project = project,
                        destination_table = destination_table,
                        default_dataset = default_dataset,
                        create_disposition = create_disposition,
                        write_disposition = write_disposition,
                        useLegacySql = useLegacySql,
                        maximum_billing_tier = maximum_billing_tier,
                        job_info = job_infoo)

  if (!is.null(job_info)) {
    eval.parent(substitute(job_info<-job_infoo))
  }

  list_tabledata(dest$projectId, dest$datasetId, dest$tableId,
    page_size = page_size, max_pages = max_pages, warn = warn)
}

# Submits a query job, waits for it, and returns information on the destination
# table for further consumption by the list_tabledata* functions
run_query_job <- function(query, project, destination_table, default_dataset,
                          create_disposition = "CREATE_IF_NEEDED",
                          write_disposition = "WRITE_EMPTY",
                          useLegacySql = TRUE,
                          maximum_billing_tier = NULL,
                          job_info = NULL) {
  assert_that(is.string(query), is.string(project))

  job <- insert_query_job(query, project, destination_table = destination_table,
                          default_dataset = default_dataset,
                          create_disposition = create_disposition,
                          write_disposition = write_disposition,
                          useLegacySql = useLegacySql,
                          maximum_billing_tier = maximum_billing_tier)
  job <- wait_for(job)

  if (!is.null(job_info)) {
    eval.parent(substitute(job_info<-job))
  }

  job$configuration$query$destinationTable
}
