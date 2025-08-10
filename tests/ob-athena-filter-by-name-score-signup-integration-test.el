;;; ob-athena-filter-by-name-score-signup-integration-test.el --- Isolated integration test -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'cl-lib)
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

(ert-deftest ob-athena-user-profiles-filter-by-name-score-signup ()
  "Query test_user_profiles using :var name, score, and signup_date, filtering by inequality. Validate output content and character length."
  ;; Pre-clean any leaked globals from previous runs
  (when (and (boundp 'ob-athena-query-status-timer)
             (timerp ob-athena-query-status-timer))
    (cancel-timer ob-athena-query-status-timer)
    (setq ob-athena-query-status-timer nil))
  (when (get-buffer "*Athena Monitor*")
    (kill-buffer "*Athena Monitor*"))
  ;; Make cancel-timer nil-safe only within this test to guard against an
  ;; instant-completion double-fire race inside ob-athena.el.
  (cl-letf* ((orig-cancel (symbol-function 'cancel-timer))
             ((symbol-function 'cancel-timer)
              (lambda (tm &rest args)
                (when (timerp tm)
                  (apply orig-cancel tm args)))))
    (let ((org-src
           "#+begin_src athena :aws-profile \"williseed-athena\" :database \"default\" :s3-output-location \"s3://athen-query-test-queries-005343251202/test-data/filters-test/\" :workgroup \"primary\" :poll-interval 3 :fullscreen t :result-reuse-enabled nil :result-reuse-max-age 0 :console-region \"us-east-1\" :var name=\"Alice\" score=90 signup_date=\"2024-01-01\"
SELECT id, name, score, signup_date FROM test_user_profiles
WHERE name != '${name}'
AND score >= ${score}
AND signup_date != '${signup_date}';
#+end_src"))
      (with-temp-buffer
        (insert org-src)
        (goto-char (point-min))
        (let* ((result (org-babel-execute-src-block))
               (csv-path (ob-athena--extract-csv-path (car (last result))))
               (expected-csv "\"id\",\"name\",\"score\",\"signup_date\"\n\
\"3\",\"Charlie\",\"95.9\",\"2024-01-03\"\n\
\"8\",\"Hank\",\"91.4\",\"2024-01-08\"\n\
\"16\",\"Paul\",\"93.4\",\"2024-01-16\"\n\
\"18\",\"Ruby\",\"92.0\",\"2024-01-18\"\n"))
          (ob-athena--wait-for-file csv-path)
          (should
           (and
            (file-exists-p csv-path)
            (with-temp-buffer
              (insert-file-contents csv-path)
              (let ((actual (buffer-string)))
                (and (equal actual expected-csv)
                     (= (length actual) 163)))))))))))
