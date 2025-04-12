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

(require 'json)
(require 'subr-x)


(defvar aws-athena-babel-query-file "/tmp/athena-query.sql"
  "Path to the temporary file where the Athena SQL query is stored.")

(defvar aws-athena-babel-output-location "s3://my-bucket/"
  "S3 location where Athena stores query results.
For example: \"s3://my-bucket/path/\".")

(defvar aws-athena-babel-workgroup "primary"
  "Athena workgroup to use.")

(defvar aws-athena-babel-profile "athena-aws-profile"
  "AWS CLI profile to use for Athena queries.")

(defvar aws-athena-babel-database "default"
  "Athena database to query.")

(defvar aws-athena-babel-poll-interval 3
  "Polling interval in seconds for checking query execution status.")

(defvar aws-athena-babel-fullscreen-monitor-buffer t
  "If non-nil, display the Athena monitor buffer in fullscreen.")

(defvar aws-athena-babel-result-reuse-enabled t
  "If non-nil, reuse previous Athena query results when possible.")

(defvar aws-athena-babel-result-reuse-max-age 10080
  "Maximum age in minutes of previous Athena query results to reuse.")

(defvar aws-athena-babel-console-region "us-east-2"
  "AWS region used to construct Athena Console URLs.")

(defvar aws-athena-babel-csv-output-dir "/tmp"
  "Directory where downloaded Athena CSV result files will be saved.")

(defvar aws-athena-babel-monitor-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-k") #'aws-athena-babel-cancel-query)
    (define-key map (kbd "C-c C-c") #'aws-athena-babel-show-csv-results)
    (define-key map (kbd "C-c C-j") #'aws-athena-babel-show-json-results)
    (define-key map (kbd "C-c C-a") #'aws-athena-babel-open-aws-link)
    (define-key map (kbd "C-c C-l") #'aws-athena-babel-open-csv-result)
    map)
  "Keymap for Athena monitor buffer.")

(defvar aws-athena-babel-total-cost 0.0
  "Running total cost of the current Athena query in USD.
Internal use only; do not modify directly.")

(defvar aws-athena-babel-query-status-timer nil
  "Timer object used internally to poll the status of therunning Athena query.
Do not modify directly.")

(add-to-list 'org-src-lang-modes '("athena" . sql))

;;;###autoload
(defun org-babel-execute:athena (body _params)
  "Execute an Athena SQL query block from Org Babel using BODY and PARAMS.
Returns clickable Org links with full URL and file path."
  (let* ((query-id (aws-athena-babel-query-executor body))
         (console-url (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s"
                              aws-athena-babel-console-region
                              aws-athena-babel-console-region
                              query-id))
         (csv-path (format "%s/%s.csv"
                           (directory-file-name aws-athena-babel-csv-output-dir)
                           query-id)))
    (list
     "Query submitted. View:"
     (format "[[%s][%s]]" console-url console-url)
     (format "[[file:%s][%s]]" csv-path csv-path))))

(defun aws-athena-babel-query-executor (query)
  "Submit Athena QUERY and stream live status to *Athena Monitor* buffer."
  (let ((monitor-buffer (aws-athena-babel--prepare-monitor-buffer))
        (query-id nil))
    (aws-athena-babel--display-monitor-buffer monitor-buffer)
    (aws-athena-babel--write-query-to-file query)
    (setq query-id (aws-athena-babel--start-query-execution))
    (aws-athena-babel--setup-monitor-state monitor-buffer query-id)
    (aws-athena-babel--start-status-polling query-id)
    query-id))

(defun aws-athena-babel--prepare-monitor-buffer ()
  "Create and populate the Athena monitor buffer."
  (let ((buf (get-buffer-create "*Athena Monitor*")))
    (with-current-buffer buf
      (read-only-mode -1)
      (erase-buffer)
      (insert (propertize "Submitting Athena query...\n" 'face 'font-lock-keyword-face))
      (insert (propertize "Press C-c C-k to cancel this query at any time.\n" 'face 'font-lock-doc-face))
      (insert (propertize "You can also view your query history here:\n" 'face 'font-lock-doc-face))
      (insert (propertize
               (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history"
                       aws-athena-babel-console-region
                       aws-athena-babel-console-region)
               'face 'link))
      (setq truncate-lines t)
      (read-only-mode 1))
    (aws-athena-babel--add-to-workspace buf)
    buf))

(defun aws-athena-babel--add-to-workspace (buffer)
  "Add BUFFER to current workspace if `persp-mode' is active."
  (when (and (featurep 'persp-mode)
             (bound-and-true-p persp-mode)
             (buffer-live-p buffer))
    (persp-add-buffer buffer)))

(defun aws-athena-babel--display-monitor-buffer (buffer)
  "Display BUFFER based on fullscreen settings and add it to the current workspace."
  (aws-athena-babel--add-to-workspace buffer)
  (if aws-athena-babel-fullscreen-monitor-buffer
      (progn
        (switch-to-buffer buffer)
        (delete-other-windows))
    (display-buffer buffer)))

(defun aws-athena-babel--write-query-to-file (query)
  "Write Athena QUERY string to file."
  (with-temp-file aws-athena-babel-query-file
    (insert query)))

(defun aws-athena-babel--start-query-execution ()
  "Start the Athena query and return the QueryExecutionId or raise an error."
  (let* ((cmd (aws-athena-babel--build-start-query-command))
         (cmd-output (string-trim (shell-command-to-string cmd))))
    (if (or (string-empty-p cmd-output)
            (string-match-p "could not be found" cmd-output)
            (string-match-p "Unable to locate credentials" cmd-output)
            (not (string-match-p "^[A-Za-z0-9-]+$" cmd-output)))
        (user-error "Failed to start query: %s" cmd-output)
      cmd-output)))

(defun aws-athena-babel--build-start-query-command ()
  "Return the formatted AWS CLI command string to start an Athena query."
  (let ((reuse-cfg (if aws-athena-babel-result-reuse-enabled
                       (format "--result-reuse-configuration \"ResultReuseByAgeConfiguration={Enabled=true,MaxAgeInMinutes=%d}\""
                               aws-athena-babel-result-reuse-max-age)
                     "")))
    (format "aws athena start-query-execution \
--query-string file://%s \
--work-group %s \
--query-execution-context Database=%s \
--result-configuration OutputLocation=%s \
%s \
--profile %s \
--output text --query 'QueryExecutionId'"
            aws-athena-babel-query-file
            aws-athena-babel-workgroup
            aws-athena-babel-database
            aws-athena-babel-output-location
            reuse-cfg
            aws-athena-babel-profile)))

(defun aws-athena-babel--setup-monitor-state (buffer query-id)
  "Add QUERY-ID to BUFFER and configure interaction keys."
  (with-current-buffer buffer
    (setq-local aws-athena-babel-query-id query-id)
    (use-local-map aws-athena-babel-monitor-mode-map)
    (read-only-mode -1)
    (goto-char (point-max))
    (insert (format "\n\nQuery started with ID: %s\n" query-id))
    (insert (format "Polling every %d seconds...\n\n" aws-athena-babel-poll-interval))
    (read-only-mode 1)
    (goto-char (point-max))))

(defun aws-athena-babel--start-status-polling (query-id)
  "Begin polling Athena query QUERY-ID status."
  (setq aws-athena-babel-query-status-timer
        (run-at-time 0 aws-athena-babel-poll-interval
                     #'aws-athena-babel-monitor-query-status query-id)))

(defun aws-athena-babel-monitor-query-status (query-id)
  "Poll Athena execution status for QUERY-ID and update monitor buffer."
  (let* ((json-output (aws-athena-babel--fetch-query-json query-id))
         (status (aws-athena-babel--extract-json-field json-output "State"))
         (cost (aws-athena-babel--update-total-cost-if-needed status json-output))
         (output (aws-athena-babel--format-monitor-status json-output status cost)))
    (aws-athena-babel--append-monitor-output (get-buffer-create "*Athena Monitor*") output)

    (when (or (member status '("SUCCEEDED" "FAILED" "CANCELLED"))
              (not status))
      (cancel-timer aws-athena-babel-query-status-timer)
      (setq aws-athena-babel-query-status-timer nil)
      (when query-id
        (aws-athena-babel--handle-query-completion query-id (get-buffer "*Athena Monitor*"))))
    ))

(defun aws-athena-babel--fetch-query-json (query-id)
  "Return raw JSON output for Athena QUERY-ID."
  (shell-command-to-string
   (format "aws athena get-query-execution \
--query-execution-id %s --profile %s"
           query-id aws-athena-babel-profile)))

(defun aws-athena-babel--update-total-cost-if-needed (_status json-output)
  "Update total cost using JSON-OUTPUT's current scanned bytes, if any."
  (let ((bytes (aws-athena-babel--extract-json-number json-output "DataScannedInBytes")))
    (when bytes
      (setq aws-athena-babel-total-cost (aws-athena-babel--calculate-query-cost bytes)))
    aws-athena-babel-total-cost))

(defun aws-athena-babel--format-monitor-status (json-output status cost)
  "Return formatted string for monitor buffer from JSON-OUTPUT, STATUS, and COST."
  (let ((time-str (propertize (format "[%s]\n" (format-time-string "%T"))
                              'face 'font-lock-comment-face))
        (status-line (propertize (format "Status: %s\n" status)
                                 'face (pcase status
                                         ("SUCCEEDED" 'success)
                                         ("FAILED" 'error)
                                         ("CANCELLED" 'warning)
                                         (_ 'font-lock-keyword-face)))))
    (concat time-str
            status-line
            (aws-athena-babel--format-status-details json-output status cost)
            "\n")))

(defun aws-athena-babel--format-status-details (json-output status cost)
  "Return formatted detail section from JSON-OUTPUT, STATUS, and COST.
Includes the reason, scanned data size, timing breakdown, and any error messages."
  (let ((reason (aws-athena-babel--extract-json-field json-output "StateChangeReason"))
        (bytes (aws-athena-babel--extract-json-number json-output "DataScannedInBytes"))
        (error-msg (aws-athena-babel--extract-json-field json-output "ErrorMessage")))
    (concat
     (when reason
       (propertize (format "Reason: %s\n" reason) 'face 'font-lock-doc-face))
     (when bytes
       (propertize (format "Data Scanned: %.2f MB\n" (/ (float bytes) 1048576))
                   'face 'font-lock-doc-face))
     (cond
      ((member status '("SUCCEEDED" "CANCELLED"))
       (propertize (format "Total Cost So Far: $%.4f\n\n" cost)
                   'face 'font-lock-preprocessor-face))
      ((string= status "RUNNING")
       (when cost
         (propertize (format "Estimated Cost So Far: $%.4f\n\n" cost)
                     'face 'font-lock-preprocessor-face))))
     (aws-athena-babel--build-timing-section json-output)
     (when error-msg
       (propertize (format "Error Message: %s\n" error-msg)
                   'face 'font-lock-warning-face)))))

(defun aws-athena-babel--build-timing-section (json)
  "Build a string with formatted timing data from Athena JSON."
  (let* ((pre-ms   (or (aws-athena-babel--extract-json-number json "ServicePreProcessingTimeInMillis") 0))
         (queue-ms (or (aws-athena-babel--extract-json-number json "QueryQueueTimeInMillis") 0))
         (exec-ms  (or (aws-athena-babel--extract-json-number json "EngineExecutionTimeInMillis") 0))
         (post-ms  (or (aws-athena-babel--extract-json-number json "ServiceProcessingTimeInMillis") 0))
         (total-ms (or (aws-athena-babel--extract-json-number json "TotalExecutionTimeInMillis") 0))
         (pre   (/ pre-ms 1000.0))
         (queue (/ queue-ms 1000.0))
         (exec  (/ exec-ms 1000.0))
         (post  (/ post-ms 1000.0))
         (total (/ total-ms 1000.0))
         (pct (lambda (val) (if (zerop total) "0.0%" (format "%.1f%%" (* (/ val total) 100))))))
    (concat
     (propertize (format "Preprocessing Time: %.2f sec (%s)\n" pre (funcall pct pre)) 'face 'font-lock-constant-face)
     (propertize (format "Queue Time: %.2f sec (%s)\n" queue (funcall pct queue)) 'face 'font-lock-constant-face)
     (propertize (format "Execution Time: %.2f sec (%s)\n" exec (funcall pct exec)) 'face 'font-lock-constant-face)
     (propertize (format "Finalization Time: %.2f sec (%s)\n" post (funcall pct post)) 'face 'font-lock-constant-face)
     (propertize (format "Total Time: %.2f sec\n" total) 'face 'font-lock-constant-face))))

(defun aws-athena-babel--append-monitor-output (buffer output)
  "Append OUTPUT to BUFFER, respecting read-only settings."
  (with-current-buffer buffer
    (read-only-mode -1)
    (goto-char (point-max))
    (insert output)
    (read-only-mode 1)
    (goto-char (point-max))))

(defun aws-athena-babel--handle-query-completion (query-id buffer)
  "Finalize Athena QUERY-ID completion in BUFFER.
This is done by downloading and displaying results."
  (let* ((json-output (aws-athena-babel--fetch-query-json query-id))
         (total-ms (aws-athena-babel--extract-json-number json-output "TotalExecutionTimeInMillis"))
         (s3-uri (aws-athena-babel--query-result-path json-output))
         (csv-path (expand-file-name (format "%s.csv" query-id)
                                     aws-athena-babel-csv-output-dir)))

    (aws-athena-babel--render-query-summary buffer total-ms)

    (when s3-uri
      (aws-athena-babel--download-csv-result s3-uri csv-path)
      (aws-athena-babel--insert-query-links-and-notes buffer csv-path query-id)
      (aws-athena-babel--insert-console-style-results buffer csv-path)
      )))

(defun aws-athena-babel--query-result-path (json-output)
  "Extract S3 output location URI from Athena JSON-OUTPUT."
  (when (string-match "\"OutputLocation\": \"\\([^\"]+\\)\"" json-output)
    (match-string 1 json-output)))

(defun aws-athena-babel--download-csv-result (s3-uri local-path)
  "Download result file from S3-URI to LOCAL-PATH using AWS CLI."
  (shell-command (format "aws s3 cp '%s' '%s' --profile %s"
                         s3-uri local-path aws-athena-babel-profile)))

(defun aws-athena-babel--render-query-summary (buffer total-ms)
  "Append cost and duration summary to BUFFER using TOTAL-MS milliseconds."
  (aws-athena-babel--append-monitor-output
   buffer
   (propertize
    (format "Total Query Cost: $%.4f (Total Time: %.2f sec)\n"
            aws-athena-babel-total-cost (/ total-ms 1000.0))
    'face 'font-lock-warning-face)))

(defun aws-athena-babel--insert-query-links-and-notes (buffer csv-path query-id)
  "Insert messages and links into BUFFER.
Use CSV-PATH and QUERY-ID from the Athena query results."
  (with-current-buffer buffer
    (read-only-mode -1)
    (goto-char (point-max))
    (insert (propertize
             (format "Query finished. Results saved to: %s\n\n" csv-path)
             'face 'font-lock-function-name-face))
    (insert (propertize
             "Press C-c C-c to view CSV results, C-c C-j for JSON. Press C-c C-l to open local file, C-c C-a for AWS link.\n"
             'face 'font-lock-doc-face))
    (when query-id
      (insert (propertize
               (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s"
                       aws-athena-babel-console-region
                       aws-athena-babel-console-region
                       query-id)
               'face 'link)))))

(defun aws-athena-babel--insert-console-style-results (buffer csv-path)
  "Insert Org-formatted Athena query results into BUFFER from CSV-PATH."
  (with-current-buffer buffer
    (insert (propertize "\n\n--- Athena Console-style Results ---\n\n"
                        'face '(:weight bold :underline t)))
    (insert (aws-athena-babel--format-csv-table csv-path))
    (read-only-mode 1)
    (goto-char (point-min))
    (when (search-forward "--- Athena Console-style Results ---" nil t)
      (beginning-of-line))))

(defun aws-athena-babel--format-csv-table (csv-path)
  "Convert CSV at CSV-PATH into Org-style table string."
  (let* ((lines (aws-athena-babel--read-csv-lines csv-path))
         (widths (aws-athena-babel--calculate-column-widths lines)))
    (aws-athena-babel--render-org-table lines widths)))

(defun aws-athena-babel--read-csv-lines (csv-path)
  "Read and clean CSV lines from CSV-PATH, returning list of row lists."
  (with-temp-buffer
    (insert-file-contents csv-path)
    (let ((raw-lines (split-string (buffer-string) "\n" t)))
      (mapcar (lambda (line)
                (split-string
                 (replace-regexp-in-string "^\"\\|\"$" ""
                                           (replace-regexp-in-string "\\\\n\\|\\\\t" " "
                                                                     (replace-regexp-in-string "\\\\\"" "\"" line)))
                 "\",\"" t))
              raw-lines))))

(defun aws-athena-babel--calculate-column-widths (rows)
  "Return list of max widths per column from ROWS."
  (apply #'cl-mapcar
         (lambda (&rest cols)
           (apply #'max (mapcar #'length cols)))
         rows))

(defun aws-athena-babel--render-org-table (rows widths)
  "Render ROWS into Org-style table using column WIDTHS."
  (mapconcat (lambda (row)
               (concat "| "
                       (mapconcat #'identity
                                  (cl-mapcar (lambda (cell width)
                                               (format (format "%%-%ds" width) cell))
                                             row widths)
                                  " | ")
                       " |"))
             rows
             "\n"))

(defun aws-athena-babel-show-json-results ()
  "Parse the CSV output of an Athena query into JSON.
Display it in a formatted buffer."
  (interactive)
  (let* ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer)))
         (csv-path (format "/tmp/%s.csv" query-id)))
    (if (not (file-exists-p csv-path))
        (message "CSV file not found: %s" csv-path)
      (aws-athena-babel--render-json-buffer csv-path))))

(defun aws-athena-babel--render-json-buffer (csv-path)
  "Render a new buffer showing JSON results parsed from CSV-PATH."
  (let ((json-buf (get-buffer-create "*Athena JSON Results*")))
    (with-current-buffer json-buf
      (aws-athena-babel--add-to-workspace json-buf)
      (erase-buffer)
      (js-mode)
      (aws-athena-babel--insert-clean-json csv-path)
      (goto-char (point-min))
      (json-pretty-print-buffer)
      (goto-char (point-min)))
    (pop-to-buffer json-buf)
    (when aws-athena-babel-fullscreen-monitor-buffer
      (delete-other-windows))))

(defun aws-athena-babel--insert-clean-json (csv-path)
  "Insert JSON-encoded data converted from CSV at CSV-PATH into current buffer."
  (let* ((csv-content (with-temp-buffer
                        (insert-file-contents csv-path)
                        (buffer-string)))
         (lines (split-string csv-content "\n" t))
         (headers (aws-athena-babel--parse-csv-line (car lines)))
         (json-objects (aws-athena-babel--csv-lines-to-json lines headers)))
    (insert (json-encode json-objects))
    (aws-athena-babel--sanitize-json-text)))

(defun aws-athena-babel--parse-csv-line (line)
  "Parse a single CSV LINE into a list of fields.
Handles quoted fields, escaped quotes, and unquoted values."
  (let ((pos 0)
        (len (length line))
        (fields '()))
    (while (< pos len)
      (let* ((char (aref line pos))
             (result
              (if (eq char ?\")
                  (aws-athena-babel--parse-quoted-field line pos len)
                (aws-athena-babel--parse-unquoted-field line pos len))))
        (push (car result) fields)
        (setq pos (cdr result))
        (when (and (< pos len) (eq (aref line pos) ?,))
          (cl-incf pos))))
    (nreverse fields)))

(defun aws-athena-babel--parse-quoted-field (line pos len)
  "Parse quoted field from LINE starting at POS, up to LEN.
Returns a cons cell (field . new-pos)."
  (cl-incf pos)
  (let ((start pos)
        (str ""))
    (while (and (< pos len)
                (not (and (eq (aref line pos) ?\")
                          (or (>= (1+ pos) len)
                              (not (eq (aref line (1+ pos)) ?\"))))))
      (if (and (eq (aref line pos) ?\")
               (eq (aref line (1+ pos)) ?\"))
          (progn
            (setq str (concat str (substring line start pos) "\""))
            (setq pos (+ pos 2))
            (setq start pos))
        (cl-incf pos)))
    (setq str (concat str (substring line start pos)))
    (cons str (1+ pos))))

(defun aws-athena-babel--parse-unquoted-field (line pos len)
  "Parse unquoted field from LINE starting at POS, up to LEN.
Returns a cons cell (field . new-pos)."
  (let ((start pos))
    (while (and (< pos len)
                (not (eq (aref line pos) ?,)))
      (cl-incf pos))
    (cons (string-trim (substring line start pos)) pos)))

(defun aws-athena-babel--clean-json-values (value)
  "Remove literal tab (\\t) and newline (\\n) escape sequences from string VALUE.
Return VALUE unchanged if not a string."
  (if (stringp value)
      (replace-regexp-in-string "\\\\[nt]" "" value)
    value))

(defun aws-athena-babel--csv-lines-to-json (lines headers)
  "Convert CSV LINES into a list of JSON objects using HEADERS.
Each line is parsed into a hash table mapping header names.
This is to cleaned field values."
  (cl-loop for line in (cdr lines)
           for fields = (aws-athena-babel--parse-csv-line line)
           if (equal (length headers) (length fields))
           collect
           (let ((obj (make-hash-table :test 'equal)))
             (cl-loop for h in headers
                      for f in fields
                      do (puthash h (aws-athena-babel--clean-json-values f) obj))
             obj)
           else do
           (message "Skipping malformed line: %s" line)))

(defun aws-athena-babel-cancel-query ()
  "Cancel the running Athena query and change polling frequency to 0s."
  (interactive)
  (let ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer))))
    (if (not query-id)
        (message "No query ID found in this buffer.")
      (when (yes-or-no-p (format "Cancel Athena query %s? " query-id))
        (shell-command
         (format "aws athena stop-query-execution \
--query-execution-id %s \
--profile %s"
                 query-id aws-athena-babel-profile))
        (message "Cancellation requested. Polling more frequently to detect state change...")

        (when (timerp aws-athena-babel-query-status-timer)
          (cancel-timer aws-athena-babel-query-status-timer))
        (setq aws-athena-babel-query-status-timer
              (run-at-time 0 1 #'aws-athena-babel-monitor-query-status query-id))))))

(defun aws-athena-babel--extract-json-field (json key)
  "Extract string value for KEY from JSON string using a regex match."
  (when (string-match (format "\"%s\": \"\\([^\"]+\\)\"" key) json)
    (match-string 1 json)))

(defun aws-athena-babel--extract-json-number (json key)
  "Extract numeric value for KEY from JSON string using a regex match."
  (when (string-match (format "\"%s\": \\([0-9]+\\)" key) json)
    (string-to-number (match-string 1 json))))

(defun aws-athena-babel--calculate-query-cost (bytes)
  "Calculate Athena query cost from BYTES scanned."
  (let ((adjusted (max bytes 10485760)))
    (* (/ adjusted 1099511627776.0) 5.0)))

(defun aws-athena-babel-show-csv-results ()
  "Display raw Athena CSV results in a separate buffer.
Display with tab, newline, and quote escape sequences removed."
  (interactive)
  (let* ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer)))
         (csv-path (format "/tmp/%s.csv" query-id)))
    (if (not (file-exists-p csv-path))
        (message "CSV file not found: %s" csv-path)
      (let ((buf (get-buffer-create "*Athena Raw Results*")))
        (with-current-buffer buf
          (aws-athena-babel--add-to-workspace buf)
          (erase-buffer)
          (insert (with-temp-buffer
                    (insert-file-contents csv-path)
                    (let ((raw (buffer-string)))
                      (setq raw (replace-regexp-in-string "\\\\t" "" raw))
                      (setq raw (replace-regexp-in-string "\\\\n" "" raw))
                      (setq raw (replace-regexp-in-string "\\\\\"" "" raw))
                      raw)))
          (goto-char (point-min)))
        (pop-to-buffer buf)
        (when aws-athena-babel-fullscreen-monitor-buffer
          (delete-other-windows))))))

(defun aws-athena-babel--sanitize-json-text ()
  "Clean up escape sequences and quoted objects in the current buffer."
  (goto-char (point-min))
  (while (search-forward "\\" nil t)
    (replace-match "" nil t))
  (goto-char (point-min))
  (while (search-forward "\"{" nil t)
    (replace-match "{" nil t))
  (goto-char (point-min))
  (while (search-forward "}\"" nil t)
    (replace-match "}" nil t)))

(defun aws-athena-babel-open-aws-link ()
  "Open the AWS Console link for the current Athena query."
  (interactive)
  (let* ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer)))
         (url (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s"
                      aws-athena-babel-console-region
                      aws-athena-babel-console-region
                      query-id)))
    (browse-url url)))

(defun aws-athena-babel-open-csv-result ()
  "Open the local CSV result file for the current Athena query."
  (interactive)
  (let* ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer)))
         (csv-path (format "/tmp/%s.csv" query-id)))
    (if (not (file-exists-p csv-path))
        (message "CSV result not found: %s" csv-path)
      (find-file csv-path))))

(provide 'aws-athena-babel)
;;; aws-athena-babel.el ends here
