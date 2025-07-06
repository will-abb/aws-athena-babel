;;; ob-athena.el --- Run AWS Athena queries from Org Babel -*- lexical-binding: t; -*-

;; Copyright (C) 2025 Williams Bosch-Bello

;; Author: Williams Bosch-Bello <williamsbosch@gmail.com>
;; Maintainer: Williams Bosch-Bello <williamsbosch@gmail.com>
;; Created: April 05, 2025
;; Version: 2.0.3
;; Package-Version: 2.0.3
;; Package-Requires: ((emacs "26.1"))
;; Keywords: aws, athena, org, babel, sql, tools
;; URL: https://github.com/will-abb/aws-athena-babel
;; Homepage: https://github.com/will-abb/aws-athena-babel
;; SPDX-License-Identifier: GPL-3.0-or-later

;; This file is part of ob-athena.

;; This program is free software: you can redistribute it and/or modify
;; it under the terms of the GNU General Public License as published by
;; the Free Software Foundation, either version 3 of the License, or
;; (at your option) any later version.

;; This program is distributed in the hope that it will be useful,
;; but WITHOUT ANY WARRANTY; without even the implied warranty of
;; MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
;; GNU General Public License for more details.

;; You should have received a copy of the GNU General Public License
;; along with this program. If not, see <https://www.gnu.org/licenses/>.

;;; Commentary:

;; Org Babel support for AWS Athena. Provides a source block backend
;; to evaluate Athena SQL queries from Org-mode using the AWS CLI.
;; submitted via the AWS CLI and monitored asynchronously in a
;; dedicated buffer. Results can be displayed as CSV, Org tables, or JSON.

;; Features include:
;; - Asynchronous query execution and live polling
;; - Real-time execution status and cost estimation
;; - Console-style Org table rendering of query results
;; - CSV-to-JSON conversion with cleaning and pretty-printing
;; - Full AWS Console URL for the running query
;; - Local raw CSV file saved to system temporary directory
;; - Result reuse support using Athena workgroups
;; - Integrated keybindings for interacting with queries:
;;   - C-c C-k: Cancel running query
;;   - C-c C-c: Show raw CSV output
;;   - C-c C-j: Show JSON output (especially useful for CloudTrail)
;;   - C-c C-a: Open AWS Console link in browser
;;   - C-c C-l: Open downloaded local CSV file in Emacs

;; By default, result files are saved to the system temporary directory
;; returned by `temporary-file-directory`. These CSV files are downloaded
;; from S3 and can be opened directly or transformed into Org and JSON views.

;; This package has been tested primarily with CloudTrail query outputs,
;; but it supports any dataset that is queryable via Athena.

;;; Code:

(require 'org)
(require 'json)
(require 'subr-x)

(declare-function persp-add-buffer "persp-mode.el")

(defvar-local ob-athena--context nil
  "Buffer-local context alist used for Athena query execution and tracking.")

(defvar ob-athena-query-file
  (expand-file-name "athena-query.sql" (temporary-file-directory))
  "Path to the temporary file where the Athena SQL query is stored.")

(defvar ob-athena-s3-output-location "s3://athena-query-results-005343251202/"
  "S3 location where Athena stores query results.
For example: \"s3://my-bucket/path/\".")

(defvar ob-athena-workgroup "primary"
  "Athena workgroup to use.")

(defvar ob-athena-profile "williseed-athena"
  "AWS CLI profile to use for Athena queries.")

(defvar ob-athena-database "default"
  "Athena database to query.")

(defvar ob-athena-poll-interval 3
  "Polling interval in seconds for checking query execution status.")

(defvar ob-athena-fullscreen-monitor-buffer t
  "If non-nil, display the Athena monitor buffer in fullscreen.")

(defvar ob-athena-result-reuse-enabled t
  "If non-nil, reuse previous Athena query results when possible.")

(defvar ob-athena-result-reuse-max-age 10080
  "Maximum age in minutes of previous Athena query results to reuse.")

(defvar ob-athena-console-region "us-east-1"
  "AWS region used to construct Athena Console URLs.")

(defvar ob-athena-csv-output-dir
  (temporary-file-directory)
  "Directory where downloaded Athena CSV result files will be saved.")

(defvar ob-athena-monitor-mode-map
  (let ((map (make-sparse-keymap)))
    (define-key map (kbd "C-c C-k") #'ob-athena-cancel-query)
    (define-key map (kbd "C-c C-c") #'ob-athena-show-csv-results)
    (define-key map (kbd "C-c C-j") #'ob-athena-show-json-results)
    (define-key map (kbd "C-c C-a") #'ob-athena-open-aws-link)
    (define-key map (kbd "C-c C-l") #'ob-athena-open-csv-result)
    map)
  "Keymap for Athena monitor buffer.")

(defvar ob-athena-total-cost 0.0
  "Running total cost of the current Athena query in USD.
Internal use only; do not modify directly.")

(defvar ob-athena-query-status-timer nil
  "Timer object used internally to poll the status of therunning Athena query.
Do not modify directly.")

(defconst ob-athena--default-context
  `((s3-output-location . ,ob-athena-s3-output-location)
    (workgroup . ,ob-athena-workgroup)
    (aws-profile . ,ob-athena-profile)
    (database . ,ob-athena-database)
    (poll-interval . ,ob-athena-poll-interval)
    (console-region . ,ob-athena-console-region)
    (csv-output-dir . ,ob-athena-csv-output-dir)
    (result-reuse-enabled . ,ob-athena-result-reuse-enabled)
    (result-reuse-max-age . ,ob-athena-result-reuse-max-age)
    (fullscreen-monitor-buffer . ,ob-athena-fullscreen-monitor-buffer))
  "Default context for Athena execution, merged with Org Babel header arguments.")

(add-to-list 'org-src-lang-modes '("athena" . sql))

;;;###autoload
(defun org-babel-execute:athena (body params)
  "Execute an Athena SQL query block from Org Babel using BODY and PARAMS.
Returns clickable Org links with full URL and file path."
  (let* ((ctx (ob-athena--build-context params))
         (expanded-body (org-babel-expand-body:athena body params))
         (query-id (ob-athena-query-executor expanded-body ctx))
         (console-url (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s"
                              (alist-get 'console-region ctx)
                              (alist-get 'console-region ctx)
                              query-id))
         (csv-path (format "%s/%s.csv"
                           (directory-file-name (alist-get 'csv-output-dir ctx))
                           query-id)))
    (list
     "Query submitted. View:"
     (format "[[%s][%s]]" console-url console-url)
     (format "[[file:%s][%s]]" csv-path csv-path))))

(defun ob-athena--build-context (params)
  "Build execution context from PARAMS and defaults."
  (let ((ctx (copy-tree ob-athena--default-context)))
    (dolist (pair params ctx)
      (when (keywordp (car pair))
        (let ((key (intern (substring (symbol-name (car pair)) 1))))
          (setf (alist-get key ctx nil 'remove #'eq) (cdr pair)))))))

(defun org-babel-expand-body:athena (body params)
  "Expand BODY with PARAMS, replacing ${var} using Org Babel :var arguments."
  (message ">>>>> Running custom org-babel-expand-body:athena with params: %S" params)
  (let ((expanded body))
    (message ">>>>> BEFORE: %s" body)
    ;; Extract all `:var` bindings
    (dolist (param params)
      (when (and (consp param) (eq (car param) :var))
        (let* ((binding (cdr param))
               (name (symbol-name (car binding)))
               (value (cdr binding))
               (replacement (cond
                             ((stringp value) (replace-regexp-in-string "^\"\\|\"$" "" value))
                             ((symbolp value) (symbol-name value))
                             (t (format "%s" value))))
               (pattern (format "${%s}" name)))
          (setq expanded
                (replace-regexp-in-string
                 (regexp-quote pattern)
                 replacement
                 expanded nil 'literal)))))
    (message ">>>>> AFTER: %s" expanded)
    expanded))

(defun ob-athena-query-executor (query ctx)
  "Submit Athena QUERY using CTX and stream live status to *Athena Monitor* buffer."
  (let ((monitor-buffer (ob-athena--prepare-monitor-buffer ctx))
        (query-id nil))
    (ob-athena--display-monitor-buffer monitor-buffer ctx)
    (ob-athena--write-query-to-file query)
    (setq query-id (ob-athena--start-query-execution ctx))
    (ob-athena--setup-monitor-state monitor-buffer query-id ctx)
    (ob-athena--start-status-polling query-id ctx)
    query-id))

(defun ob-athena--prepare-monitor-buffer (ctx)
  "Create and populate the Athena monitor buffer using CTX."
  (let ((buf (get-buffer-create "*Athena Monitor*"))
        (region (alist-get 'console-region ctx)))
    (with-current-buffer buf
      (read-only-mode -1)
      (erase-buffer)
      (insert (propertize "Submitting Athena query...\n" 'face 'font-lock-keyword-face))
      (insert (propertize "Press C-c C-k to cancel this query at any time.\n" 'face 'font-lock-doc-face))
      (insert (propertize "You can also view your query history here:\n" 'face 'font-lock-doc-face))
      (insert (propertize
               (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history"
                       region region)
               'face 'link))
      (setq truncate-lines t)
      (read-only-mode 1))
    (ob-athena--add-to-workspace buf)
    buf))

(defun ob-athena--add-to-workspace (buffer)
  "Add BUFFER to current workspace if `persp-mode' is active."
  (when (and (featurep 'persp-mode)
             (bound-and-true-p persp-mode)
             (buffer-live-p buffer))
    (persp-add-buffer buffer)))

(defun ob-athena--display-monitor-buffer (buffer ctx)
  "Display BUFFER based on fullscreen settings in CTX.
Add buffer to the current workspace."
  (ob-athena--add-to-workspace buffer)
  (if (alist-get 'fullscreen-monitor-buffer ctx)
      (progn
        (switch-to-buffer buffer)
        (delete-other-windows))
    (display-buffer buffer)))

(defun ob-athena--write-query-to-file (query)
  "Write Athena QUERY string to file."
  (with-temp-file ob-athena-query-file
    (insert query)))

(defun ob-athena--start-query-execution (ctx)
  "Start the Athena query using CTX.
Return the QueryExecutionId or raise an error."
  (let* ((cmd (ob-athena--build-start-query-command ctx))
         (cmd-output (string-trim (shell-command-to-string cmd))))
    (if (or (string-empty-p cmd-output)
            (string-match-p "could not be found" cmd-output)
            (string-match-p "Unable to locate credentials" cmd-output)
            (not (string-match-p "^[A-Za-z0-9-]+$" cmd-output)))
        (user-error "Failed to start query: %s" cmd-output)
      cmd-output)))

(defun ob-athena--build-start-query-command (ctx)
  "Return the formatted AWS CLI command string to start an Athena query using CTX."
  (message "CTX: %S" ctx)
  (let* ((reuse-enabled (alist-get 'result-reuse-enabled ctx))
         (reuse-age (alist-get 'result-reuse-max-age ctx))
         (reuse-cfg (if reuse-enabled
                        (format "--result-reuse-configuration %s"
                                (shell-quote-argument
                                 (format "ResultReuseByAgeConfiguration={Enabled=true,MaxAgeInMinutes=%d}"
                                         reuse-age)))
                      ""))
         (workgroup (alist-get 'workgroup ctx))
         (database (alist-get 'database ctx))
         (output-location (alist-get 's3-output-location ctx))
         (profile (alist-get 'aws-profile ctx))
         (region (alist-get 'console-region ctx)))
    (format "aws athena start-query-execution \
--query-string %s \
--work-group %s \
--query-execution-context Database=%s \
--result-configuration OutputLocation=%s \
%s \
--region %s \
--profile %s \
--output text --query 'QueryExecutionId'"
            (shell-quote-argument (format "file://%s" ob-athena-query-file))
            (shell-quote-argument workgroup)
            (shell-quote-argument database)
            (shell-quote-argument output-location)
            reuse-cfg
            (shell-quote-argument region)
            (shell-quote-argument profile))))

(defun ob-athena--setup-monitor-state (buffer query-id ctx)
  "Add QUERY-ID to BUFFER and configure interaction keys using CTX."
  (with-current-buffer buffer
    (setq-local ob-athena-query-id query-id)
    (setq-local ob-athena--context ctx)
    (setq-local ob-athena-query-completed nil)
    (use-local-map ob-athena-monitor-mode-map)
    (read-only-mode -1)
    (goto-char (point-max))
    (insert (format "\n\nQuery started with ID: %s\n" query-id))
    (insert (format "Polling every %d seconds...\n\n"
                    (alist-get 'poll-interval ctx)))
    (read-only-mode 1)
    (goto-char (point-max))))

(defun ob-athena--start-status-polling (query-id ctx)
  "Begin polling Athena query QUERY-ID status using CTX."
  (setq ob-athena-query-status-timer
        (run-at-time 0 (alist-get 'poll-interval ctx)
                     (lambda () (ob-athena-monitor-query-status query-id ctx)))))

(defun ob-athena-monitor-query-status (query-id ctx)
  "Poll Athena execution status for QUERY-ID using CTX and update monitor buffer."
  (let* ((json-output (ob-athena--fetch-query-json query-id ctx))
         (status (ob-athena--extract-json-field json-output "State"))
         (cost (ob-athena--update-total-cost-if-needed status json-output))
         (output (ob-athena--format-monitor-status json-output status cost)))
    (ob-athena--append-monitor-output (get-buffer-create "*Athena Monitor*") output)
    (when (or (member status '("SUCCEEDED" "FAILED" "CANCELLED")) (not status))
      (cancel-timer ob-athena-query-status-timer)
      (setq ob-athena-query-status-timer nil)
      (ob-athena--handle-query-completion query-id (get-buffer "*Athena Monitor*") ctx))))

(defun ob-athena--fetch-query-json (query-id ctx)
  "Return raw JSON output for Athena QUERY-ID using CTX."
  (let ((profile (alist-get 'aws-profile ctx))
        (region (alist-get 'console-region ctx)))
    (shell-command-to-string
     (format "aws athena get-query-execution \
--query-execution-id %s \
--region %s \
--profile %s"
             (shell-quote-argument query-id)
             (shell-quote-argument region)
             (shell-quote-argument profile)))))

(defun ob-athena--format-monitor-status (json-output status cost)
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
            (ob-athena--format-status-details json-output status cost)
            "\n")))

(defun ob-athena--format-status-details (json-output status cost)
  "Return formatted detail section from JSON-OUTPUT, STATUS, and COST.
Includes reason, scanned data size, timing breakdown, and any error messages."
  (let ((reason (ob-athena--extract-json-field json-output "StateChangeReason"))
        (bytes (ob-athena--extract-json-number json-output "DataScannedInBytes"))
        (error-msg (ob-athena--extract-json-field json-output "ErrorMessage")))
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
     (ob-athena--build-timing-section json-output)
     (when error-msg
       (propertize (format "Error Message: %s\n" error-msg)
                   'face 'font-lock-warning-face)))))

(defun ob-athena--build-timing-section (json)
  "Build a string with formatted timing data from Athena JSON."
  (let* ((pre-ms   (or (ob-athena--extract-json-number json "ServicePreProcessingTimeInMillis") 0))
         (queue-ms (or (ob-athena--extract-json-number json "QueryQueueTimeInMillis") 0))
         (exec-ms  (or (ob-athena--extract-json-number json "EngineExecutionTimeInMillis") 0))
         (post-ms  (or (ob-athena--extract-json-number json "ServiceProcessingTimeInMillis") 0))
         (total-ms (or (ob-athena--extract-json-number json "TotalExecutionTimeInMillis") 0))
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

(defun ob-athena--append-monitor-output (buffer output)
  "Append OUTPUT to BUFFER, respecting read-only settings."
  (with-current-buffer buffer
    (read-only-mode -1)
    (goto-char (point-max))
    (insert output)
    (read-only-mode 1)
    (goto-char (point-max))))

(defun ob-athena--handle-query-completion (query-id buffer ctx)
  "Finalize Athena QUERY-ID completion in BUFFER using CTX.
This is done by downloading and displaying results."
  (let* ((json-output (ob-athena--fetch-query-json query-id ctx))
         (total-ms (ob-athena--extract-json-number json-output "TotalExecutionTimeInMillis"))
         (s3-uri (ob-athena--query-result-path json-output))
         (csv-dir (alist-get 'csv-output-dir ctx))
         (csv-path (expand-file-name (format "%s.csv" query-id) csv-dir)))
    (ob-athena--render-query-summary buffer total-ms)
    (when s3-uri
      (ob-athena--download-csv-result s3-uri csv-path ctx)
      (ob-athena--insert-query-links-and-notes buffer csv-path query-id ctx)
      (ob-athena--insert-console-style-results buffer csv-path)))
  (with-current-buffer buffer
    (setq-local ob-athena-query-completed t)))

(defun ob-athena--query-result-path (json-output)
  "Extract S3 output location URI from Athena JSON-OUTPUT."
  (when (string-match "\"OutputLocation\": \"\\([^\"]+\\)\"" json-output)
    (match-string 1 json-output)))

(defun ob-athena--download-csv-result (s3-uri local-path ctx)
  "Download result file from S3-URI to LOCAL-PATH using AWS CLI and CTX."
  (let ((profile (alist-get 'aws-profile ctx))
        (region (alist-get 'console-region ctx)))
    (shell-command
     (format "aws s3 cp %s %s --region %s --profile %s"
             (shell-quote-argument s3-uri)
             (shell-quote-argument local-path)
             (shell-quote-argument region)
             (shell-quote-argument profile)))))

(defun ob-athena--render-query-summary (buffer total-ms)
  "Append cost and duration summary to BUFFER using TOTAL-MS milliseconds."
  (ob-athena--append-monitor-output
   buffer
   (propertize
    (format "Total Query Cost: $%s (Total Time: %.2f sec)\n"
            ob-athena-total-cost (/ total-ms 1000.0))
    'face 'font-lock-warning-face)))

(defun ob-athena--insert-query-links-and-notes (buffer csv-path query-id ctx)
  "Insert messages and links into BUFFER using CSV-PATH, QUERY-ID, and CTX."
  (let ((region (alist-get 'console-region ctx)))
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
                         region region query-id)
                 'face 'link))))))

(defun ob-athena--insert-console-style-results (buffer csv-path)
  "Insert Org-formatted Athena query results into BUFFER from CSV-PATH."
  (with-current-buffer buffer
    (insert (propertize "\n\n--- Athena Console-style Results ---\n\n"
                        'face '(:weight bold :underline t)))
    (insert (ob-athena--format-csv-table csv-path))
    (read-only-mode 1)
    (goto-char (point-min))
    (when (search-forward "--- Athena Console-style Results ---" nil t)
      (beginning-of-line))))

(defun ob-athena--format-csv-table (csv-path)
  "Convert CSV at CSV-PATH into Org-style table string."
  (let* ((lines (ob-athena--read-csv-lines csv-path))
         (widths (ob-athena--calculate-column-widths lines)))
    (ob-athena--render-org-table lines widths)))

(defun ob-athena--read-csv-lines (csv-path)
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

(defun ob-athena--calculate-column-widths (rows)
  "Return list of max widths per column from ROWS."
  (if (null rows)
      '()
    (apply #'cl-mapcar
           (lambda (&rest cols)
             (apply #'max (mapcar #'length cols)))
           rows)))

(defun ob-athena--render-org-table (rows widths)
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

(defun ob-athena-show-json-results ()
  "Convert Athena CSV result to JSON using mlr and show it in a formatted buffer."
  (interactive)
  (let* ((query-id (buffer-local-value 'ob-athena-query-id (current-buffer)))
         (csv-path (expand-file-name (format "%s.csv" query-id) ob-athena-csv-output-dir))
         (json-path (expand-file-name (format "%s.json" query-id) ob-athena-csv-output-dir))
         (json-buf (get-buffer-create "*Athena JSON Results*")))
    (if (not (file-exists-p csv-path))
        (message "CSV file not found: %s" csv-path)
      (let ((mlr-exit (shell-command
                       (format "mlr --icsv --ojson cat %s > %s"
                               (shell-quote-argument csv-path)
                               (shell-quote-argument json-path)))))
        (if (/= mlr-exit 0)
            (message "Failed to convert CSV to JSON using mlr.")
          (with-current-buffer json-buf
            (ob-athena--add-to-workspace json-buf)
            (erase-buffer)
            (insert-file-contents json-path)
            (goto-char (point-min))
            (js-mode))
          (pop-to-buffer json-buf)
          (when ob-athena-fullscreen-monitor-buffer
            (delete-other-windows)))))))

(defun ob-athena-cancel-query ()
  "Cancel the running Athena query if still running."
  (interactive)
  (let ((query-id (buffer-local-value 'ob-athena-query-id (current-buffer)))
        (ctx (buffer-local-value 'ob-athena--context (current-buffer))))
    (if (or (not query-id)
            (buffer-local-value 'ob-athena-query-completed (current-buffer)))
        (message "Query is already completed or invalid.")
      (when (yes-or-no-p (format "Cancel Athena query %s? " query-id))
        (let ((profile (alist-get 'aws-profile ctx))
              (region (alist-get 'console-region ctx)))
          (shell-command
           (format "aws athena stop-query-execution \
--query-execution-id %s \
--region %s \
--profile %s"
                   (shell-quote-argument query-id)
                   (shell-quote-argument region)
                   (shell-quote-argument profile))))
        (message "Cancellation requested. Polling more frequently to detect state change...")
        (when (timerp ob-athena-query-status-timer)
          (cancel-timer ob-athena-query-status-timer))
        (setq ob-athena-query-status-timer
              (run-at-time 0 1 #'ob-athena-monitor-query-status query-id ctx))))))


(defun ob-athena--extract-json-field (json key)
  "Extract string value for KEY from JSON string using a regex match.
Returns nil if JSON is not a string, malformed, or key is not found."
  (when (and (stringp json) (stringp key))
    (when (string-match
           (format "\"%s\"[ \t]*:[ \t]*\"\\([^\"]*\\)\"" (regexp-quote key))
           json)
      (match-string 1 json))))

(defun ob-athena--extract-json-number (json key)
  "Extract numeric value for KEY from JSON string using a regex match."
  (when (string-match (format "\"%s\": \\([0-9]+\\)" key) json)
    (string-to-number (match-string 1 json))))

(defun ob-athena--calculate-query-cost (bytes)
  "Calculate Athena query cost (per-query billing model).
Rounds BYTES to nearest megabyte with a 10MB minimum charge."
  (let* ((rounded-up-mb (ceiling (/ bytes 1048576.0)))
         (billable-mb (max rounded-up-mb 10)))
    (* (/ (* billable-mb 1048576.0) 1099511627776.0) 5.0)))

(defun ob-athena-show-csv-results ()
  "Display raw Athena CSV results in a separate buffer.
Display with tab, newline, and quote escape sequences removed."
  (interactive)
  (let* ((query-id (buffer-local-value 'ob-athena-query-id (current-buffer)))
         (csv-path (expand-file-name (format "%s.csv" query-id) ob-athena-csv-output-dir)))
    (if (not (file-exists-p csv-path))
        (message "CSV file not found: %s" csv-path)
      (let ((buf (get-buffer-create "*Athena Raw Results*")))
        (with-current-buffer buf
          (ob-athena--add-to-workspace buf)
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
        (when ob-athena-fullscreen-monitor-buffer
          (delete-other-windows))))))

(defun ob-athena-open-aws-link ()
  "Open the AWS Console link for the current Athena query."
  (interactive)
  (let* ((query-id (buffer-local-value 'ob-athena-query-id (current-buffer)))
         (url (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s"
                      ob-athena-console-region
                      ob-athena-console-region
                      query-id)))
    (browse-url url)))

(defun ob-athena-open-csv-result ()
  "Open the local CSV result file for the current Athena query."
  (interactive)
  (let* ((query-id (buffer-local-value 'ob-athena-query-id (current-buffer)))
         (csv-path (expand-file-name (format "%s.csv" query-id) ob-athena-csv-output-dir)))
    (if (not (file-exists-p csv-path))
        (message "CSV result not found: %s" csv-path)
      (find-file csv-path))))

(defun ob-athena--update-total-cost-if-needed (_status json-output)
  "Update total cost using JSON-OUTPUT's current scanned bytes, if any."
  (let ((bytes (ob-athena--extract-json-number json-output "DataScannedInBytes")))
    (when bytes
      (setq ob-athena-total-cost (ob-athena--calculate-query-cost bytes)))
    ob-athena-total-cost))

(provide 'ob-athena)
;;; ob-athena.el ends here
