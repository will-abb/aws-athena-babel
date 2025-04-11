;;; aws-athena-babel.el --- Run AWS Athena queries from Org Babel -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Williams Bosch-Bello

;; Author: Williams Bosch-Bello <williamsbosch@gmail.com>
;; Maintainer: Williams Bosch-Bello <williamsbosch@gmail.com>
;; Created: April 05, 2025
;; Version: 0.1.0
;; Package-Requires: ((emacs "26.1"))
;; Keywords: aws, athena, org, babel, sql, tools
;; Homepage: https://github.com/will-abb/aws-athena-babel
;; License: GPL-3.0-or-later

;; This file is part of aws-athena-babel.

;; This program is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program.  If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; This package provides an Emacs interface for running AWS Athena
;; SQL queries directly from Org Babel source blocks. Queries are
;; submitted via the AWS CLI and monitored asynchronously in a
;; dedicated buffer. Results can be displayed as CSV or JSON.

;; Features include:
;; - Live polling and console-style output
;; - CSV to JSON conversion
;; - Result reuse via Athena workgroups
;; - Integration with org-babel execution

;;; Code:
(require 'json) ; Load the built-in JSON parsing library
(require 'subr-x) ; Load common utility functions like `string-trim` and `string-empty-p`

(defvar aws-athena-babel-query-file "/tmp/athena-query.sql"
  "Path to the temporary file where the Athena SQL query is stored.") ; Define the path to save the temporary Athena SQL query

(defvar aws-athena-babel-output-location "s3://athenaqueryresults-logarchive/"
  "S3 location where Athena stores query results.
For example: \"s3://my-bucket/path/\".") ; S3 bucket path to store Athena results

(defvar aws-athena-babel-workgroup "primary"
  "Athena workgroup to use.") ; The Athena workgroup name to use in query execution

(defvar aws-athena-babel-profile "athena-log-archive"
  "AWS CLI profile to use for Athena queries.") ; AWS CLI profile name used to run queries

(defvar aws-athena-babel-database "default"
  "Athena database to query.") ; Database name to query within Athena

(defvar aws-athena-babel-poll-interval 2
  "Polling interval in seconds for checking query execution status.") ; Interval for polling the query status

(defvar aws-athena-babel-fullscreen-monitor-buffer t
  "If non-nil, display the Athena monitor buffer in fullscreen.") ; Whether to show the monitor buffer in fullscreen or not

(defvar aws-athena-babel-result-reuse-enabled t
  "If non-nil, reuse previous Athena query results when possible.") ; Whether to enable result reuse to save cost/time

(defvar aws-athena-babel-result-reuse-max-age 10080
  "Maximum age in minutes of previous Athena query results to reuse.") ; Maximum age for reused results in minutes

(defvar aws-athena-babel-console-region "us-east-2"
  "AWS region used to construct Athena Console URLs.") ; Region used in the AWS Console links

(defvar aws-athena-babel-csv-output-dir "/tmp"
  "Directory where downloaded Athena CSV result files will be saved.") ; Path to save downloaded result CSVs

(defvar aws-athena-babel-monitor-mode-map
  (let ((map (make-sparse-keymap))) ; Create a sparse keymap for the monitor buffer
    (define-key map (kbd "C-c C-k") #'aws-athena-babel-cancel-query) ; Bind C-c C-k to cancel Athena query
    (define-key map (kbd "C-c C-c") #'aws-athena-babel-show-csv-results) ; Bind C-c C-c to show raw CSV results
    (define-key map (kbd "C-c C-j") #'aws-athena-babel-show-json-results) ; Bind C-c C-j to show parsed JSON results
    (define-key map (kbd "C-c C-a") #'aws-athena-babel-open-aws-link) ; Bind C-c C-a to open the Athena console URL
    (define-key map (kbd "C-c C-l") #'aws-athena-babel-open-csv-result) ; Bind C-c C-l to open local CSV file
    map) ; Return the created keymap
  "Keymap for Athena monitor buffer.") ; Documentation for the custom keymap

(defvar aws-athena-babel-total-cost 0.0
  "Running total cost of the current Athena query in USD.
Internal use only; do not modify directly.") ; Variable to track total query cost

(defvar aws-athena-babel-query-status-timer nil
  "Timer object used internally to poll the status of the running Athena query.
Do not modify directly.") ; Timer object used for polling status

(add-to-list 'org-src-lang-modes '("athena" . sql)) ; Register "athena" language block as SQL for Org Mode source blocks

;;;###autoload
(defun org-babel-execute:athena (body _params)
  "Execute an Athena SQL query block from Org Babel using BODY and PARAMS.
Displays query progress and results in a dedicated monitor buffer." ; Main entry point for Org Babel block execution
  (aws-athena-babel-query-executor body) ; Call executor to run the query
  (format "Query submitted. See *Athena Monitor* buffer for progress and results or https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s."
          aws-athena-babel-console-region ; Use region for URL generation
          aws-athena-babel-console-region
          aws-athena-babel-query-id)) ; Provide feedback to Org block on where to find the query

(defun aws-athena-babel-query-executor (query)
  "Submit Athena QUERY and stream live status to *Athena Monitor* buffer." ; High-level query execution function
  (let ((monitor-buffer (aws-athena-babel--prepare-monitor-buffer)) ; Create monitor buffer
        (query-id nil)) ; Placeholder for query ID

    (aws-athena-babel--display-monitor-buffer monitor-buffer) ; Show monitor buffer
    (aws-athena-babel--write-query-to-file query) ; Write query text to temporary file
    (setq query-id (aws-athena-babel--start-query-execution)) ; Run the query and save ID
    (aws-athena-babel--setup-monitor-state monitor-buffer query-id) ; Update monitor buffer with query info
    (aws-athena-babel--start-status-polling query-id))) ; Begin polling for status

(defun aws-athena-babel--write-query-to-file (query)
  "Write Athena QUERY string to file." ; Save the query to the local file
  (with-temp-file aws-athena-babel-query-file ; Create and write to file
    (insert query))) ; Insert the raw query text

(defun aws-athena-babel--build-start-query-command ()
  "Return the formatted AWS CLI command string to start an Athena query." ; Compose CLI command string
  (let ((reuse-cfg (if aws-athena-babel-result-reuse-enabled ; Check if result reuse is enabled
                       (format "--result-reuse-configuration \"ResultReuseByAgeConfiguration={Enabled=true,MaxAgeInMinutes=%d}\""
                               aws-athena-babel-result-reuse-max-age) ; If so, build reuse flag string
                     ""))) ; Otherwise, leave empty
    (format "aws athena start-query-execution \
--query-string file://%s \
--work-group %s \
--query-execution-context Database=%s \
--result-configuration OutputLocation=%s \
%s \
--profile %s \
--output text --query 'QueryExecutionId'"
            aws-athena-babel-query-file ; Use saved query file
            aws-athena-babel-workgroup ; Workgroup for query
            aws-athena-babel-database ; Database context
            aws-athena-babel-output-location ; Where to store results
            reuse-cfg ; Optional reuse configuration
            aws-athena-babel-profile))) ; Use profile for execution
(defun aws-athena-babel--start-query-execution ()
  "Start the Athena query and return the QueryExecutionId or raise an error." ; Start query and return its ID
  (let* ((cmd (aws-athena-babel--build-start-query-command)) ; Build the CLI command
         (cmd-output (string-trim (shell-command-to-string cmd)))) ; Run the command and trim output
    (if (or (string-empty-p cmd-output) ; If output is empty
            (string-match-p "could not be found" cmd-output) ; or indicates a missing resource
            (string-match-p "Unable to locate credentials" cmd-output) ; or AWS credentials issue
            (not (string-match-p "^[A-Za-z0-9-]+$" cmd-output))) ; or output doesn't look like a valid ID
        (user-error "Failed to start query: %s" cmd-output) ; Raise error with message
      cmd-output))) ; Else return query ID

(defun aws-athena-babel--prepare-monitor-buffer ()
  "Create and populate the Athena monitor buffer." ; Create monitor buffer for progress
  (let ((buf (get-buffer-create "*Athena Monitor*"))) ; Create or get named buffer
    (with-current-buffer buf
      (read-only-mode -1) ; Disable read-only mode for writing
      (erase-buffer) ; Clear any previous content
      (insert (propertize "Submitting Athena query...\n" 'face 'font-lock-keyword-face)) ; Insert status message
      (insert (propertize "Press C-c C-k to cancel this query at any time.\n" 'face 'font-lock-doc-face)) ; Help text
      (insert (propertize "You can also view your query history here:\n" 'face 'font-lock-doc-face)) ; More help
      (insert (propertize
               (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history\n\n"
                       aws-athena-babel-console-region ; Region in URL
                       aws-athena-babel-console-region)
               'face 'link)) ; Insert Athena query history link
      (setq truncate-lines t) ; Prevent line wrapping
      (read-only-mode 1)) ; Re-enable read-only
    (aws-athena-babel--add-to-workspace buf) ; Add buffer to workspace
    buf)) ; Return buffer

(defun aws-athena-babel--display-monitor-buffer (buffer)
  "Display BUFFER based on fullscreen settings and add it to the current workspace." ; Show the monitor buffer
  (aws-athena-babel--add-to-workspace buffer) ; Ensure it's in current workspace
  (if aws-athena-babel-fullscreen-monitor-buffer ; If fullscreen is enabled
      (progn
        (switch-to-buffer buffer) ; Switch to the monitor buffer
        (delete-other-windows)) ; Show only the buffer
    (display-buffer buffer))) ; Else just display buffer

(defun aws-athena-babel--setup-monitor-state (buffer query-id)
  "Add QUERY-ID to BUFFER and configure interaction keys." ; Store ID and setup keys in buffer
  (with-current-buffer buffer
    (setq-local aws-athena-babel-query-id query-id) ; Save query ID as buffer-local variable
    (use-local-map aws-athena-babel-monitor-mode-map) ; Apply custom keymap
    (read-only-mode -1) ; Make buffer writable
    (goto-char (point-max)) ; Move to end
    (insert (format "Query started with ID: %s\n" query-id)) ; Show ID
    (insert (format "Polling every %d seconds...\n\n" aws-athena-babel-poll-interval)) ; Show polling interval
    (read-only-mode 1) ; Make buffer read-only again
    (goto-char (point-max)))) ; Move to end again

(defun aws-athena-babel--start-status-polling (query-id)
  "Begin polling Athena query QUERY-ID status." ; Begin polling query status
  (setq aws-athena-babel-query-status-timer
        (run-at-time 1 aws-athena-babel-poll-interval ; Start timer to run every N seconds
                     #'aws-athena-babel-monitor-query-status query-id))) ; Call polling function

(defun aws-athena-babel-monitor-query-status (query-id)
  "Poll Athena execution status for QUERY-ID and update monitor buffer." ; Check the query's current status
  (let* ((json-output (aws-athena-babel--fetch-query-json query-id)) ; Get JSON response
         (status (aws-athena-babel--extract-json-field json-output "State")) ; Extract state
         (cost (aws-athena-babel--update-total-cost-if-needed status json-output)) ; Possibly update cost
         (output (aws-athena-babel--format-monitor-status json-output status cost))) ; Format status string
    (aws-athena-babel--append-monitor-output (get-buffer-create "*Athena Monitor*") output) ; Write to monitor buffer

    (when (or (member status '("SUCCEEDED" "FAILED" "CANCELLED")) ; If query is finished
              (not status)) ; Or status is invalid/missing
      (cancel-timer aws-athena-babel-query-status-timer) ; Stop polling
      (setq aws-athena-babel-query-status-timer nil) ; Clear the timer var
      (when query-id
        (aws-athena-babel--handle-query-completion query-id (get-buffer "*Athena Monitor*")))))) ; Finalize results

(defun aws-athena-babel--fetch-query-json (query-id)
  "Return raw JSON output for Athena QUERY-ID." ; Get the Athena query's full metadata
  (shell-command-to-string
   (format "aws athena get-query-execution \
--query-execution-id %s --profile %s"
           query-id aws-athena-babel-profile))) ; Use query ID and profile to fetch data

(defun aws-athena-babel--update-total-cost-if-needed (_status json-output)
  "Update total cost and return it using current scanned bytes, if any." ; Update cost value based on bytes scanned
  (let ((bytes (aws-athena-babel--extract-json-number json-output "DataScannedInBytes"))) ; Extract byte count
    (when bytes
      (setq aws-athena-babel-total-cost (aws-athena-babel--calculate-query-cost bytes))) ; Recalculate cost
    aws-athena-babel-total-cost)) ; Return the updated cost

(defun aws-athena-babel--format-monitor-status (json-output status cost)
  "Return formatted string for monitor buffer from JSON-OUTPUT, STATUS, and COST." ; Create pretty output
  (let ((time-str (propertize (format "[%s]\n" (format-time-string "%T"))
                              'face 'font-lock-comment-face)) ; Timestamp string
        (status-line (propertize (format "Status: %s\n" status)
                                 'face (pcase status
                                         ("SUCCEEDED" 'success)
                                         ("FAILED" 'error)
                                         ("CANCELLED" 'warning)
                                         (_ 'font-lock-keyword-face))))) ; Highlight based on state
    (concat time-str
            status-line
            (aws-athena-babel--format-status-details json-output status cost) ; Details block
            "\n"))) ; Trailing newline

(defun aws-athena-babel--format-status-details (json-output status cost)
  "Return formatted detail section from JSON-OUTPUT, including reason, cost, timing, and errors." ; Expand results
  (let ((reason (aws-athena-babel--extract-json-field json-output "StateChangeReason")) ; Why the state changed
        (bytes (aws-athena-babel--extract-json-number json-output "DataScannedInBytes")) ; How much data scanned
        (error-msg (aws-athena-babel--extract-json-field json-output "ErrorMessage"))) ; Error, if any
    (concat
     (when reason
       (propertize (format "Reason: %s\n" reason) 'face 'font-lock-doc-face)) ; Show reason
     (when bytes
       (propertize (format "Data Scanned: %.2f MB\n" (/ (float bytes) 1048576))
                   'face 'font-lock-doc-face)) ; Show scanned data size
     (cond
      ((member status '("SUCCEEDED" "CANCELLED"))
       (propertize (format "Total Cost So Far: $%.4f\n\n" cost)
                   'face 'font-lock-preprocessor-face)) ; Final cost
      ((string= status "RUNNING")
       (when cost
         (propertize (format "Estimated Cost So Far: $%.4f\n\n" cost)
                     'face 'font-lock-preprocessor-face)))) ; Running estimate
     (aws-athena-babel--build-timing-section json-output) ; Add timings
     (when error-msg
       (propertize (format "Error Message: %s\n" error-msg)
                   'face 'font-lock-warning-face))))) ; Show error message if any

(defun aws-athena-babel--build-timing-section (json)
  "Build a string with formatted timing data from Athena JSON." ; Format all timing-related info
  (let* ((pre-ms   (or (aws-athena-babel--extract-json-number json "ServicePreProcessingTimeInMillis") 0)) ; Preprocessing
         (queue-ms (or (aws-athena-babel--extract-json-number json "QueryQueueTimeInMillis") 0)) ; Queued time
         (exec-ms  (or (aws-athena-babel--extract-json-number json "EngineExecutionTimeInMillis") 0)) ; Execution time
         (post-ms  (or (aws-athena-babel--extract-json-number json "ServiceProcessingTimeInMillis") 0)) ; Finalization
         (total-ms (or (aws-athena-babel--extract-json-number json "TotalExecutionTimeInMillis") 0)) ; Total duration
         (pre   (/ pre-ms 1000.0)) ; Convert ms → seconds
         (queue (/ queue-ms 1000.0))
         (exec  (/ exec-ms 1000.0))
         (post  (/ post-ms 1000.0))
         (total (/ total-ms 1000.0))
         (pct (lambda (val) (if (zerop total) "0.0%" (format "%.1f%%" (* (/ val total) 100)))))) ; % calculator
    (concat
     (propertize (format "Preprocessing Time: %.2f sec (%s)\n" pre (funcall pct pre)) 'face 'font-lock-constant-face)
     (propertize (format "Queue Time: %.2f sec (%s)\n" queue (funcall pct queue)) 'face 'font-lock-constant-face)
     (propertize (format "Execution Time: %.2f sec (%s)\n" exec (funcall pct exec)) 'face 'font-lock-constant-face)
     (propertize (format "Finalization Time: %.2f sec (%s)\n" post (funcall pct post)) 'face 'font-lock-constant-face)
     (propertize (format "Total Time: %.2f sec\n" total) 'face 'font-lock-constant-face)))) ; Concatenate the time section
(defun aws-athena-babel--append-monitor-output (buffer output)
  "Append OUTPUT to BUFFER, respecting read-only settings." ; Append text to the buffer safely
  (with-current-buffer buffer
    (read-only-mode -1) ; Temporarily disable read-only
    (goto-char (point-max)) ; Move to end of buffer
    (insert output) ; Insert the new output
    (read-only-mode 1) ; Re-enable read-only
    (goto-char (point-max)))) ; Ensure point is still at end

(defun aws-athena-babel--handle-query-completion (query-id buffer)
  "Finalize Athena QUERY-ID completion in BUFFER by downloading and displaying results." ; Cleanup and show results
  (let* ((json-output (aws-athena-babel--fetch-query-json query-id)) ; Fetch final JSON output
         (total-ms (aws-athena-babel--extract-json-number json-output "TotalExecutionTimeInMillis")) ; Extract duration
         (s3-uri (aws-athena-babel--query-result-path json-output)) ; Get S3 result location
         (csv-path (expand-file-name (format "%s.csv" query-id)
                                     aws-athena-babel-csv-output-dir))) ; Local CSV file path

    (aws-athena-babel--render-query-summary buffer total-ms) ; Show cost & time

    (when s3-uri ; If there's a result to download
      (aws-athena-babel--download-csv-result s3-uri csv-path) ; Download result
      (aws-athena-babel--insert-query-links-and-notes buffer csv-path query-id) ; Add links to buffer
      (aws-athena-babel--insert-console-style-results buffer csv-path)))) ; Insert Org-style result

(defun aws-athena-babel--query-result-path (json-output)
  "Extract S3 output location URI from Athena JSON-OUTPUT." ; Pull the S3 URI from raw JSON
  (when (string-match "\"OutputLocation\": \"\\([^\"]+\\)\"" json-output)
    (match-string 1 json-output))) ; Return matched URI

(defun aws-athena-babel--download-csv-result (s3-uri local-path)
  "Download result file from S3-URI to LOCAL-PATH using AWS CLI." ; Run AWS CLI to copy results
  (shell-command (format "aws s3 cp '%s' '%s' --profile %s"
                         s3-uri local-path aws-athena-babel-profile))) ; CLI command to copy CSV file

(defun aws-athena-babel--render-query-summary (buffer total-ms)
  "Append cost and duration summary to BUFFER using TOTAL-MS milliseconds." ; Display cost and duration
  (aws-athena-babel--append-monitor-output
   buffer
   (propertize
    (format "\nTotal Query Cost: $%.4f (Total Time: %.2f sec)\n"
            aws-athena-babel-total-cost (/ total-ms 1000.0)) ; Format cost and duration
    'face 'font-lock-warning-face))) ; Show with highlight

(defun aws-athena-babel--insert-query-links-and-notes (buffer csv-path query-id)
  "Insert messages and links into BUFFER about query results." ; Display helpful info after query finishes
  (with-current-buffer buffer
    (read-only-mode -1) ; Allow editing
    (goto-char (point-max)) ; Go to end
    (insert (propertize
             (format "\nQuery finished. Results saved to: %s\n" csv-path)
             'face 'font-lock-function-name-face)) ; File path message
    (insert (propertize
             "Press C-c C-c to view CSV results, or C-c C-j for JSON.\n"
             'face 'font-lock-doc-face)) ; Explain shortcuts
    (when query-id
      (insert (propertize
               (format "\nSpecific Query URL: https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s\n"
                       aws-athena-babel-console-region
                       aws-athena-babel-console-region
                       query-id)
               'face 'link))))) ; Console query URL

(defun aws-athena-babel--insert-console-style-results (buffer csv-path)
  "Insert Org-formatted Athena query results into BUFFER from CSV-PATH." ; Render table in org-style
  (with-current-buffer buffer
    (insert (propertize "\n--- Athena Console-style Results ---\n\n"
                        'face '(:weight bold :underline t))) ; Section header
    (insert (aws-athena-babel--format-csv-table csv-path)) ; Insert formatted table
    (read-only-mode 1) ; Lock buffer
    (goto-char (point-min)) ; Move to start
    (when (search-forward "--- Athena Console-style Results ---" nil t)
      (beginning-of-line)))) ; Jump to start of results

(defun aws-athena-babel--format-csv-table (csv-path)
  "Convert CSV at CSV-PATH into Org-style table string." ; Convert file contents to table
  (let* ((lines (aws-athena-babel--read-csv-lines csv-path)) ; Read CSV lines
         (widths (aws-athena-babel--calculate-column-widths lines))) ; Get max width of each column
    (aws-athena-babel--render-org-table lines widths))) ; Render formatted table

(defun aws-athena-babel--read-csv-lines (csv-path)
  "Read and clean CSV lines from CSV-PATH, returning list of row lists." ; Read and split lines into lists
  (with-temp-buffer
    (insert-file-contents csv-path) ; Load file
    (let ((raw-lines (split-string (buffer-string) "\n" t))) ; Split lines
      (mapcar (lambda (line)
                (split-string
                 (replace-regexp-in-string "^\"\\|\"$" ""
                                           (replace-regexp-in-string "\\\\n\\|\\\\t" " "
                                                                     (replace-regexp-in-string "\\\\\"" "\"" line)))
                 "\",\"" t)) ; Clean each field
              raw-lines)))) ; Return cleaned rows

(defun aws-athena-babel--calculate-column-widths (rows)
  "Return list of max widths per column from ROWS." ; Get max width for each column
  (apply #'cl-mapcar
         (lambda (&rest cols)
           (apply #'max (mapcar #'length cols))) ; Compute max length in each column
         rows)) ; Iterate over rows

(defun aws-athena-babel--render-org-table (rows widths)
  "Render ROWS into Org-style table using column WIDTHS." ; Format table string
  (mapconcat (lambda (row)
               (concat "| "
                       (mapconcat #'identity
                                  (cl-mapcar (lambda (cell width)
                                               (format (format "%%-%ds" width) cell)) ; Pad cells
                                             row widths)
                                  " | ")
                       " |")) ; Finalize row format
             rows ; Apply to each row
             "\n")) ; Separate rows with newlines

(defun aws-athena-babel-show-json-results ()
  "Parse the CSV output of an Athena query into JSON and display in a formatted buffer." ; Convert results to JSON
  (interactive)
  (let* ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer))) ; Get ID
         (csv-path (format "/tmp/%s.csv" query-id))) ; Build path to CSV
    (if (not (file-exists-p csv-path)) ; If file missing
        (message "CSV file not found: %s" csv-path) ; Show message
      (aws-athena-babel--render-json-buffer csv-path)))) ; Otherwise display JSON

(defun aws-athena-babel--render-json-buffer (csv-path)
  "Render a new buffer showing JSON results parsed from CSV-PATH." ; Render pretty-printed JSON
  (let ((json-buf (get-buffer-create "*Athena JSON Results*"))) ; Create buffer
    (with-current-buffer json-buf
      (aws-athena-babel--add-to-workspace json-buf) ; Add to workspace
      (erase-buffer) ; Clear buffer
      (js-mode) ; Use JavaScript mode for JSON
      (aws-athena-babel--insert-clean-json csv-path) ; Insert parsed data
      (goto-char (point-min)) ; Move to top
      (json-pretty-print-buffer) ; Pretty print JSON
      (goto-char (point-min))) ; Move to top again
    (pop-to-buffer json-buf) ; Show buffer
    (when aws-athena-babel-fullscreen-monitor-buffer
      (delete-other-windows)))) ; Fullscreen option

(defun aws-athena-babel--insert-clean-json (csv-path)
  "Insert JSON-encoded data converted from CSV at CSV-PATH into current buffer." ; Convert and insert JSON
  (let* ((csv-content (with-temp-buffer
                        (insert-file-contents csv-path)
                        (buffer-string))) ; Load CSV into string
         (lines (split-string csv-content "\n" t)) ; Split into lines
         (headers (aws-athena-babel--parse-csv-line (car lines))) ; First line is header
         (data-lines (cdr lines)) ; Rest are data rows
         (json-objects (aws-athena-babel--csv-lines-to-json lines headers))) ; Convert to JSON
    (insert (json-encode json-objects)) ; Insert encoded JSON
    (aws-athena-babel--sanitize-json-text))) ; Clean up escaping
(defun aws-athena-babel--parse-csv-line (line)
  "Parse a single CSV LINE into a list of fields.
Handles quoted fields, escaped quotes, and unquoted values." ; Parse a single line of CSV into a list of strings
  (let ((pos 0) ; Start at the beginning of the line
        (len (length line)) ; Total length of the line
        (fields '())) ; Accumulator for parsed fields
    (while (< pos len) ; Loop until end of line
      (let* ((char (aref line pos)) ; Get current character
             (result
              (if (eq char ?\") ; If it's a quoted field
                  (aws-athena-babel--parse-quoted-field line pos len) ; Use quoted parser
                (aws-athena-babel--parse-unquoted-field line pos len)))) ; Otherwise use unquoted parser
        (push (car result) fields) ; Add parsed field to list
        (setq pos (cdr result)) ; Move to next character
        (when (and (< pos len) (eq (aref line pos) ?,)) ; Skip comma separator if any
          (cl-incf pos))))
    (nreverse fields))) ; Return the list of fields in original order

(defun aws-athena-babel--parse-quoted-field (line pos len)
  "Parse quoted field from LINE starting at POS, up to LEN.
Returns a cons cell (field . new-pos)." ; Handles fields enclosed in double quotes with escaping
  (cl-incf pos) ; Skip initial quote
  (let ((start pos) ; Record start of content
        (str "")) ; Accumulator string
    (while (and (< pos len)
                (not (and (eq (aref line pos) ?\")
                          (or (>= (1+ pos) len)
                              (not (eq (aref line (1+ pos)) ?\")))))) ; Continue until closing unescaped quote
      (if (and (eq (aref line pos) ?\")
               (eq (aref line (1+ pos)) ?\")) ; Handle escaped quotes
          (progn
            (setq str (concat str (substring line start pos) "\"")) ; Add escaped quote to result
            (setq pos (+ pos 2)) ; Move past escaped quote
            (setq start pos)) ; Update start
        (cl-incf pos))) ; Otherwise, move forward
    (setq str (concat str (substring line start pos))) ; Add remaining string to result
    (cons str (1+ pos)))) ; Return field and next position

(defun aws-athena-babel--parse-unquoted-field (line pos len)
  "Parse unquoted field from LINE starting at POS, up to LEN.
Returns a cons cell (field . new-pos)." ; Parses simple fields without surrounding quotes
  (let ((start pos)) ; Start of field
    (while (and (< pos len)
                (not (eq (aref line pos) ?,))) ; Continue until comma or end
      (cl-incf pos))
    (cons (string-trim (substring line start pos)) pos))) ; Return trimmed field and next position

(defun aws-athena-babel--clean-json-values (value)
  "Remove literal tab (\\t) and newline (\\n) escape sequences from string VALUE.
Return VALUE unchanged if not a string." ; Cleans up special sequences in field values
  (if (stringp value)
      (replace-regexp-in-string "\\\\[nt]" "" value) ; Remove \t and \n
    value)) ; If not string, return as-is

(defun aws-athena-babel--csv-lines-to-json (lines headers)
  "Convert CSV LINES into a list of JSON objects using HEADERS.
Each line is parsed into a hash table mapping header names to cleaned field values." ; Transform lines into JSON array
  (cl-loop for line in (cdr lines) ; Skip header
           for fields = (aws-athena-babel--parse-csv-line line) ; Parse each line
           if (equal (length headers) (length fields)) ; Ensure correct number of fields
           collect
           (let ((obj (make-hash-table :test 'equal))) ; Create hash map
             (cl-loop for h in headers
                      for f in fields
                      do (puthash h (aws-athena-babel--clean-json-values f) obj)) ; Map header to cleaned value
             obj) ; Return object
           else do
           (message "Skipping malformed line: %s" line))) ; Warn about malformed lines

(defun aws-athena-babel-cancel-query ()
  "Cancel the running Athena query from the monitor buffer." ; Manual cancellation handler
  (interactive)
  (let ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer)))) ; Get current query ID
    (if (not query-id)
        (message "No query ID found in this buffer.") ; No active query
      (when (yes-or-no-p (format "Cancel Athena query %s? " query-id)) ; Prompt user
        (shell-command
         (format "aws athena stop-query-execution \
--query-execution-id %s \
--profile %s"
                 query-id aws-athena-babel-profile)) ; Run cancel command
        (message "Cancellation requested."))))) ; Notify user

(defun aws-athena-babel--extract-json-field (json key)
  "Extract string value for KEY from JSON string using a regex match." ; Pull a string field from JSON
  (when (string-match (format "\"%s\": \"\\([^\"]+\\)\"" key) json)
    (match-string 1 json))) ; Return match if found

(defun aws-athena-babel--extract-json-number (json key)
  "Extract numeric value for KEY from JSON string using a regex match." ; Extract a number from JSON
  (when (string-match (format "\"%s\": \\([0-9]+\\)" key) json)
    (string-to-number (match-string 1 json)))) ; Convert to number

(defun aws-athena-babel--calculate-query-cost (bytes)
  "Calculate Athena query cost from BYTES scanned." ; Calculate based on AWS pricing
  (let ((adjusted (max bytes 10485760))) ; Minimum 10MB charged
    (* (/ adjusted 1099511627776.0) 5.0))) ; Rate: $5 per TB scanned

(defun aws-athena-babel-show-csv-results ()
  "Display raw Athena CSV results in a separate buffer.
Display with tab, newline, and quote escape sequences removed." ; Opens raw CSV in buffer with cleaning
  (interactive)
  (let* ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer))) ; Get query ID
         (csv-path (format "/tmp/%s.csv" query-id))) ; Build path
    (if (not (file-exists-p csv-path)) ; If file doesn't exist
        (message "CSV file not found: %s" csv-path) ; Notify user
      (let ((buf (get-buffer-create "*Athena Raw Results*"))) ; Create new buffer
        (with-current-buffer buf
          (aws-athena-babel--add-to-workspace buf) ; Add buffer to workspace
          (erase-buffer) ; Clear old content
          (insert (with-temp-buffer
                    (insert-file-contents csv-path)
                    (let ((raw (buffer-string)))
                      (setq raw (replace-regexp-in-string "\\\\t" "" raw)) ; Remove tab escapes
                      (setq raw (replace-regexp-in-string "\\\\n" "" raw)) ; Remove newline escapes
                      (setq raw (replace-regexp-in-string "\\\\\"" "" raw)) ; Remove escaped quotes
                      raw))) ; Insert cleaned contents
          (goto-char (point-min))) ; Move to beginning
        (pop-to-buffer buf) ; Show buffer
        (when aws-athena-babel-fullscreen-monitor-buffer
          (delete-other-windows)))))) ; Optionally fullscreen

(defun aws-athena-babel--add-to-workspace (buffer)
  "Add BUFFER to current workspace if persp-mode is active." ; Add buffer to perspective workspace
  (when (and (featurep 'persp-mode)
             (bound-and-true-p persp-mode)
             (buffer-live-p buffer))
    (persp-add-buffer buffer))) ; Register buffer in workspace

(defun aws-athena-babel--sanitize-json-text ()
  "Clean up escape sequences and quoted objects in the current buffer." ; Remove unwanted escapes from JSON
  (goto-char (point-min)) ; Go to top of buffer
  (while (search-forward "\\" nil t)
    (replace-match "" nil t)) ; Remove backslashes
  (goto-char (point-min))
  (while (search-forward "\"{" nil t)
    (replace-match "{" nil t)) ; Remove opening quote
  (goto-char (point-min))
  (while (search-forward "}\"" nil t)
    (replace-match "}" nil t))) ; Remove closing quote

(defun aws-athena-babel-open-aws-link ()
  "Open the AWS Console link for the current Athena query." ; Open query in browser
  (interactive)
  (let* ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer))) ; Get query ID
         (url (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s"
                      aws-athena-babel-console-region
                      aws-athena-babel-console-region
                      query-id))) ; Build AWS Console URL
    (browse-url url))) ; Open in browser

(defun aws-athena-babel-open-csv-result ()
  "Open the local CSV result file for the current Athena query." ; Open downloaded CSV
  (interactive)
  (let* ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer))) ; Get query ID
         (csv-path (format "/tmp/%s.csv" query-id))) ; Construct CSV path
    (if (not (file-exists-p csv-path)) ; Check if file exists
        (message "CSV result not found: %s" csv-path) ; Notify user if not
      (find-file csv-path)))) ; Open file in Emacs

(provide 'aws-athena-babel) ; Make this package available to Emacs as a feature
;;; aws-athena-babel.el ends here ; End of file marker
