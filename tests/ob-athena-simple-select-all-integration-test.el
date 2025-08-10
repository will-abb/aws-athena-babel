;;; ob-athena-simple-select-all-integration-test.el --- Isolated integration test -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(setq org-confirm-babel-evaluate nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defun ob-athena--wait-for-file (file-path &optional timeout)
  "Wait up to TIMEOUT seconds for FILE-PATH to exist, checking each second.
If TIMEOUT is nil, defaults to 10 seconds."
  (let ((retries (or timeout 10)))
    (while (and (not (file-exists-p file-path)) (> retries 0))
      (sleep-for 1)
      (setq retries (1- retries)))))

(defun ob-athena--extract-csv-path (result)
  "Extract local file path from Org link RESULT."
  (when (stringp result)
    (if (string-match "\\[\\[file:\\(.*?\\)\\]\\[" result)
        (match-string 1 result)
      result)))

(ert-deftest ob-athena-user-profiles-simple-select-all ()
  "Run a simple SELECT * query on test_user_profiles without any variables or interpolation."
  ;; Pre/post cleanup for any leaked global state
  (when (and (boundp 'ob-athena-query-status-timer)
             (timerp ob-athena-query-status-timer))
    (cancel-timer ob-athena-query-status-timer)
    (setq ob-athena-query-status-timer nil))
  (when (get-buffer "*Athena Monitor*")
    (kill-buffer "*Athena Monitor*"))
  (let ((org-src
         "#+begin_src athena :s3-output-location \"s3://athena-query-results-005343251202/\"
SELECT * FROM test_user_profiles;
#+end_src"))
    (with-temp-buffer
      (insert org-src)
      (goto-char (point-min))
      (let* ((result (org-babel-execute-src-block))
             (csv-path (ob-athena--extract-csv-path (car (last result)))))
        (ob-athena--wait-for-file csv-path)
        (should
         (and
          (file-exists-p csv-path)
          (with-temp-buffer
            (insert-file-contents csv-path)
            (let ((lines (split-string (buffer-string) "\n" t)))
              (> (length lines) 1)))))))))

(provide 'ob-athena-simple-select-all-integration-test)
;;; ob-athena-simple-select-all-integration-test.el ends here
