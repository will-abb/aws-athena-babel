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

(defvar aws-athena-babel-output-location nil
  "S3 location where Athena stores query results.
For example: \"s3://my-bucket/path/\".")

(defvar aws-athena-babel-workgroup "primary"
  "Athena workgroup to use.")

(defvar aws-athena-babel-profile "aws-athena-profile"
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
    map)
  "Keymap for Athena monitor buffer.")

(defvar aws-athena-babel-total-cost 0.0
  "Running total cost of the current Athena query in USD.
Internal use only; do not modify directly.")

(defvar aws-athena-babel-query-status-timer nil
  "Timer object used internally to poll the status of therunning Athena query.
Do not modify directly.")

(defun aws-athena-babel--parse-csv-line (line)
  "Parse a single CSV LINE into a list of fields.
Will correctly handle quoted fields and escaped double quotes."
  (let ((pos 0)
        (len (length line))
        (fields '()))
    (while (< pos len)
      (let ((char (aref line pos)))
        (cond
         ;; Quoted field
         ((eq char ?\")
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
            (cl-incf pos)
            (push str fields)
            (when (and (< pos len) (eq (aref line pos) ?,))
              (cl-incf pos))))
         ;; Unquoted field
         (t
          (let ((start pos))
            (while (and (< pos len)
                        (not (eq (aref line pos) ?,)))
              (cl-incf pos))
            (push (string-trim (substring line start pos)) fields)
            (when (and (< pos len) (eq (aref line pos) ?,))
              (cl-incf pos)))))))
    (nreverse fields)))


(defun aws-athena-babel--clean-json-values (value)
  "Remove literal tab (\\t) and newline (\\n) escape sequences from string VALUE.
Return VALUE unchanged if not a string."
  (if (stringp value)
      (replace-regexp-in-string "\\\\[nt]" "" value)
    value))


(defun aws-athena-babel-query-executor (query)
  "Submit Athena QUERY and stream live status to *Athena Monitor* buffer."
  (let ((monitor-buffer (get-buffer-create "*Athena Monitor*"))
        (query-id nil))

    ;; Setup monitor buffer
    (with-current-buffer monitor-buffer
      (read-only-mode -1)
      (erase-buffer)
      (insert (propertize "Submitting Athena query...\n" 'face 'font-lock-keyword-face))
      (insert (propertize "Press C-c C-k to cancel this query at any time.\n" 'face 'font-lock-doc-face))
      (insert (propertize "You can also view your query history here:\n"
                          'face 'font-lock-doc-face))
(insert (propertize
         (format "https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history\n\n"
                 aws-athena-babel-console-region
                 aws-athena-babel-console-region)
         'face 'link))

      (setq truncate-lines t)
      (read-only-mode 1))


    ;; Show buffer based on fullscreen setting
    (if aws-athena-babel-fullscreen-monitor-buffer
        (progn
          (switch-to-buffer monitor-buffer)
          (delete-other-windows))
      (display-buffer monitor-buffer))

    ;; Write query to file
    (with-temp-file aws-athena-babel-query-file
      (insert query))

    ;; Start the query
    (let* ((cmd-output
            (shell-command-to-string
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
                     (if aws-athena-babel-result-reuse-enabled
                         (format "--result-reuse-configuration \"ResultReuseByAgeConfiguration={Enabled=true,MaxAgeInMinutes=%d}\""
                                 aws-athena-babel-result-reuse-max-age)
                       "")
                     aws-athena-babel-profile)))
           (trimmed (string-trim cmd-output)))
      (if (or (string-empty-p trimmed)
              (string-match-p "could not be found" trimmed)
              (string-match-p "Unable to locate credentials" trimmed)
              (not (string-match-p "^[A-Za-z0-9-]+$" trimmed)))
          (user-error "Failed to start query: %s" (string-trim cmd-output))
        (setq query-id trimmed)))

    ;; Add query ID and keymap to monitor buffer
    (with-current-buffer monitor-buffer
      (setq-local aws-athena-babel-query-id query-id)
      (use-local-map aws-athena-babel-monitor-mode-map))

    ;; Display initial status
    (with-current-buffer monitor-buffer
      (read-only-mode -1)
      (goto-char (point-max))
      (insert (format "Query started with ID: %s\n" query-id))
      (insert (format "Polling every %d seconds...\n\n" aws-athena-babel-poll-interval))
      (read-only-mode 1)
      (goto-char (point-max)))

    ;; Start polling via timer
    (setq aws-athena-babel-query-status-timer
          (run-at-time 1 aws-athena-babel-poll-interval
                       #'aws-athena-babel-monitor-query-status query-id))))


(defun aws-athena-babel-cancel-query ()
  "Cancel the running Athena query from the monitor buffer."
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
        (message "Cancellation requested.")))))


(defun aws-athena-babel-monitor-query-status (query-id)
  "Poll Athena execution status for QUERY-ID.
Update the monitor buffer with progress and cost metrics."
  (let ((monitor-buffer (get-buffer-create "*Athena Monitor*")))
    (let* ((info
            (shell-command-to-string
             (format "aws athena get-query-execution \
--query-execution-id %s --profile %s"
                     query-id aws-athena-babel-profile)))
           (query-status nil)
           (poll-output "")
           (cost-this-scan 0.0))
      (setq query-status
            (progn
              (string-match "\"State\": \"\\([A-Z]+\\)\"" info)
              (match-string 1 info)))

      ;; Extract data scanned and metrics
      (let* ((timestamp (propertize (format "[%s]" (format-time-string "%T"))
                                    'face 'font-lock-comment-face))
             (status-line (format "Status: %s\n" query-status))
             (state-reason (when (string-match "\"StateChangeReason\": \"\\([^\"]+\\)\"" info)
                             (match-string 1 info)))
             (bytes-scanned (when (string-match "\"DataScannedInBytes\": \\([0-9]+\\)" info)
                              (string-to-number (match-string 1 info))))
             (adjusted-bytes (when bytes-scanned (max bytes-scanned 10485760)))
             (cost-this-scan (if adjusted-bytes
                                 (* (/ adjusted-bytes 1099511627776.0) 5.0)
                               0.0))
             (manifest (when (string-match "\"DataManifestLocation\": \"\\([^\"]+\\)\"" info)
                         (match-string 1 info)))
             (reused-result (when (string-match "\"ReusedPreviousResult\": \\(true\\|false\\)" info)
                              (format "Reused Previous Result: %s\n" (match-string 1 info))))
             (workgroup (when (string-match "\"WorkGroup\": \"\\([^\"]+\\)\"" info)
                          (format "WorkGroup: %s\n" (match-string 1 info))))
             (error-category (when (string-match "\"ErrorCategory\": \\([0-9]+\\)" info)
                               (match-string 1 info)))
             (error-type (when (string-match "\"ErrorType\": \\([0-9]+\\)" info)
                           (match-string 1 info)))
             (error-message (when (string-match "\"ErrorMessage\": \"\\([^\"]+\\)\"" info)
                              (match-string 1 info)))
             (retryable (when (string-match "\"Retryable\": \\(true\\|false\\)" info)
                          (match-string 1 info))))

        (when (and cost-this-scan (string= query-status "SUCCEEDED"))
          (setq aws-athena-babel-total-cost cost-this-scan))

        ;; Compute durations
        (let* ((pre (/ (string-to-number (or (and (string-match "\"ServicePreProcessingTimeInMillis\": \\([0-9]+\\)" info)
                                                  (match-string 1 info)) "0")) 1000.0))
               (queue (/ (string-to-number (or (and (string-match "\"QueryQueueTimeInMillis\": \\([0-9]+\\)" info)
                                                    (match-string 1 info)) "0")) 1000.0))
               (exec (/ (string-to-number (or (and (string-match "\"EngineExecutionTimeInMillis\": \\([0-9]+\\)" info)
                                                   (match-string 1 info)) "0")) 1000.0))
               (post (/ (string-to-number (or (and (string-match "\"ServiceProcessingTimeInMillis\": \\([0-9]+\\)" info)
                                                   (match-string 1 info)) "0")) 1000.0))
               (total (/ (string-to-number (or (and (string-match "\"TotalExecutionTimeInMillis\": \\([0-9]+\\)" info)
                                                    (match-string 1 info)) "0")) 1000.0))
               (pct (lambda (val) (if (zerop total) "0.0%" (format "%.1f%%" (* (/ val total) 100))))))

          (setq poll-output
                (concat timestamp "\n"
                        (propertize status-line 'face
                                    (cond
                                     ((string= query-status "SUCCEEDED") 'success)
                                     ((string= query-status "FAILED") 'error)
                                     ((string= query-status "CANCELLED") 'warning)
                                     (t 'font-lock-keyword-face)))
                        (when state-reason
                          (propertize (format "Reason: %s\n" state-reason)
                                      'face 'font-lock-doc-face))
                        (when bytes-scanned
                          (propertize (format "Data Scanned: %.2f MB\n"
                                              (/ adjusted-bytes 1048576.0))
                                      'face 'font-lock-doc-face))
                        (when (and cost-this-scan (> cost-this-scan 0.0))
                          (propertize (format "Total Cost So Far: $%.4f\n\n" aws-athena-babel-total-cost)
                                      'face 'font-lock-preprocessor-face))
                        (propertize (format "Preprocessing Time: %.2f sec (%s)\n" pre (funcall pct pre))
                                    'face 'font-lock-constant-face)
                        (propertize (format "Queue Time: %.2f sec (%s)\n" queue (funcall pct queue))
                                    'face 'font-lock-constant-face)
                        (propertize (format "Execution Time: %.2f sec (%s)\n" exec (funcall pct exec))
                                    'face 'font-lock-constant-face)
                        (propertize (format "Finalization Time: %.2f sec (%s)\n" post (funcall pct post))
                                    'face 'font-lock-constant-face)
                        (propertize (format "Total Time: %.2f sec\n" total)
                                    'face 'font-lock-constant-face)
                        (when manifest (propertize (format "Data Manifest: %s\n" manifest)
                                                   'face 'font-lock-string-face))
                        (when reused-result (propertize reused-result 'face 'font-lock-type-face))
                        (when workgroup (propertize workgroup 'face 'font-lock-variable-name-face))
                        (when error-category
                          (propertize (format "Error Category: %s (1=System, 2=User, 3=Other)\n"
                                              error-category)
                                      'face 'error))
                        (when error-type
                          (propertize (format "Error Type: %s\n" error-type)
                                      'face 'error))
                        (when error-message
                          (propertize (format "Error Message: %s\n" error-message)
                                      'face 'font-lock-warning-face))
                        (when retryable
                          (propertize (format "Retryable: %s\n" retryable)
                                      'face 'font-lock-type-face))
                        "\n"))))

      ;; Append to monitor buffer
      (with-current-buffer monitor-buffer
        (read-only-mode -1)
        (goto-char (point-max))
        (insert poll-output)
        (read-only-mode 1)
        (goto-char (point-max)))

      ;; On completion
      (when (member query-status '("SUCCEEDED" "FAILED" "CANCELLED"))
        (cancel-timer aws-athena-babel-query-status-timer)
        (setq aws-athena-babel-query-status-timer nil)

        (let ((total-ms (string-to-number
                         (or (and (string-match "\"TotalExecutionTimeInMillis\": \\([0-9]+\\)" info)
                                  (match-string 1 info)) "0"))))
          (with-current-buffer monitor-buffer
            (read-only-mode -1)
            (goto-char (point-max))
            (insert (propertize
                     (format "\nTotal Query Cost: $%.4f (Total Time: %.2f sec)\n"
                             aws-athena-babel-total-cost
                             (/ total-ms 1000.0))
                     'face 'font-lock-warning-face))
            (read-only-mode 1)))

        (let* ((result-json
                (shell-command-to-string
                 (format "aws athena get-query-execution \
--query-execution-id %s --profile %s"
                         query-id aws-athena-babel-profile)))
               (s3-uri
                (when (string-match "\"OutputLocation\": \"\\([^\"]+\\)\"" result-json)
                  (match-string 1 result-json)))
               (csv-file (expand-file-name (format "%s.csv" query-id)
                                           aws-athena-babel-csv-output-dir)))
          (when s3-uri
            (shell-command (format "aws s3 cp '%s' '%s' --profile %s"
                                   s3-uri csv-file aws-athena-babel-profile))
            (with-current-buffer monitor-buffer
              (read-only-mode -1)
              (goto-char (point-max))
              (insert (propertize
                       (format "\nQuery finished. Results saved to: %s\n" csv-file)
                       'face 'font-lock-function-name-face))
              (insert (propertize
                       "Press C-c C-c to view csv results, or C-c C-j for JSON.\n"
                       'face 'font-lock-doc-face))
;; Insert specific query link before result rendering
(when query-id
  (insert (propertize
           (format "\nSpecific Query URL: https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s\n"
                   aws-athena-babel-console-region
                   aws-athena-babel-console-region
                   query-id)
           'face 'link)))

(insert (propertize "\n--- Athena Console-style Results ---\n\n"
                    'face '(:weight bold :underline t)))
              (insert (with-temp-buffer
                        (insert-file-contents csv-file)
                        (goto-char (point-min))
                        (let* ((lines (split-string (buffer-string) "\n" t))
                               (rows (mapcar (lambda (line)
                                               (split-string line "\",\"" t "[ \t]*\"?\\(?:\\\\\"\\)*"))
                                             (mapcar (lambda (s)
                                                       (replace-regexp-in-string "^\"\\|\"$" ""
                                                                                 (replace-regexp-in-string "\\\\n" " "
                                                                                                           (replace-regexp-in-string "\\\\t" " "
                                                                                                                                     (replace-regexp-in-string "\\\\\"" "\"" s)))))
                                                     lines)))
                               (col-widths (apply #'cl-mapcar (lambda (&rest cols)
                                                                (apply #'max (mapcar #'length cols)))
                                                  rows)))
                          (mapconcat (lambda (row)
                                       (concat "| "
                                               (let ((cells (cl-mapcar (lambda (cell width)
                                                                         (format (format "%%-%ds" width) cell))
                                                                       row col-widths)))
                                                 (mapconcat #'identity cells " | "))
                                               " |"))
                                     rows
                                     "\n"))))
              (read-only-mode 1)
              (goto-char (point-min))
              (when (search-forward "--- Athena Console-style Results ---" nil t)
                (beginning-of-line)))))))))


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


(defun aws-athena-babel-show-json-results ()
  "Parse the CSV output of an Athena query into JSON objects.
Display the result in a formatted JSON buffer.
 Attempts to clean escape sequences and fix quoted structures before formatting."
  (interactive)
  (let* ((query-id (buffer-local-value 'aws-athena-babel-query-id (current-buffer)))
         (csv-path (format "/tmp/%s.csv" query-id)))
    (if (not (file-exists-p csv-path))
        (message "CSV file not found: %s" csv-path)
      (let ((json-buf (get-buffer-create "*Athena JSON Results*")))
        (with-current-buffer json-buf
          (erase-buffer)
          (js-mode) ;; Optional: for basic JSON syntax highlighting
          (let* ((csv-content (with-temp-buffer
                                (insert-file-contents csv-path)
                                (buffer-string)))
                 (lines (split-string csv-content "\n" t))
                 (headers (aws-athena-babel--parse-csv-line (car lines)))
                 (data-lines (cdr lines))
                 (json-objects
                  (cl-loop for line in data-lines
                           for fields = (aws-athena-babel--parse-csv-line line)
                           unless (equal (length headers) (length fields))
                           collect (message "Skipping malformed line: %s" line)
                           else collect
                           (let ((obj (make-hash-table :test 'equal)))
                             (cl-loop for h in headers
                                      for f in fields do
                                      (puthash h (aws-athena-babel--clean-json-values f) obj))
                             obj))))
            (insert (json-encode json-objects))
            (goto-char (point-min))

            ;; Remove literal backslashes
            (while (search-forward "\\" nil t)
              (replace-match "" nil t))

            ;; Remove quoted curly braces
            (goto-char (point-min))
            (while (search-forward "\"{" nil t)
              (replace-match "{" nil t))
            (goto-char (point-min))
            (while (search-forward "}\"" nil t)
              (replace-match "}" nil t))

            (json-pretty-print-buffer)
            (goto-char (point-min)))
          (pop-to-buffer json-buf)
          (when aws-athena-babel-fullscreen-monitor-buffer
            (delete-other-windows)))))))

;;;###autoload
(defun org-babel-execute:athena (body _params)
  "Execute an Athena SQL query block from Org Babel using BODY and PARAMS.
Displays query progress and results in a dedicated monitor buffer."
  (aws-athena-babel-query-executor body)
  (format "Query submitted. See *Athena Monitor* buffer for progress and results or https://%s.console.aws.amazon.com/athena/home?region=%s#/query-editor/history/%s."
          aws-athena-babel-console-region
          aws-athena-babel-console-region
          aws-athena-babel-query-id))

  (add-to-list 'org-src-lang-modes '("athena" . sql))

  (provide 'aws-athena-babel)
;;; aws-athena-babel.el ends here

