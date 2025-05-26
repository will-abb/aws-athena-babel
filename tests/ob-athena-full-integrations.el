;;; ob-athena-full-integrations.el --- Integration tests for ob-athena -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(defun ob-athena--poll-until-finished (path)
  "Poll a status JSON file at PATH until state becomes SUCCEEDED.
This is a blocking function used for testing only."
  (let ((state ""))
    (while (not (string= state "SUCCEEDED"))
      (sleep-for ob-athena-poll-interval)
      (when (file-exists-p path)
        (with-temp-buffer
          (insert-file-contents path)
          (goto-char (point-min))
          (when (re-search-forward "\"State\"[[:space:]]*:[[:space:]]*\"\\([^\"]+\\)\"" nil t)
            (setq state (match-string 1))))))
    (with-temp-buffer
      (insert-file-contents path)
      (buffer-string))))

(ert-deftest ob-athena--poll-until-finished-stops-at-succeeded ()
  "Ensure polling loop terminates when query state becomes SUCCEEDED."
  (let* ((tmp-json (make-temp-file "ob-athena-test" nil ".json"))
         (query-id "fake-query-id")
         (ob-athena-poll-interval 0.05)
         (updates `(
                    ,(json-encode `(:Status (:State "RUNNING")))
                    ,(json-encode `(:Status (:State "QUEUED")))
                    ,(json-encode `(:Status (:State "SUCCEEDED")))))
         (i 0)
         (write-next-state
          (lambda ()
            (when (< i (length updates))
              (with-temp-file tmp-json
                (insert (nth i updates)))
              (cl-incf i)))))
    (funcall write-next-state)
    (cl-letf (((symbol-function 'sleep-for)
               (lambda (&rest _) (funcall write-next-state))))
      (let ((status-json (ob-athena--poll-until-finished tmp-json)))
        (should (string-match-p "\"SUCCEEDED\"" status-json))))))

(provide 'ob-athena-full-integrations)
;;; ob-athena-full-integrations.el ends here
