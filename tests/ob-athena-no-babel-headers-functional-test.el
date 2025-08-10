;;; ob-athena-no-babel-headers-functional-test.el --- Description -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(setq org-confirm-babel-evaluate nil)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(ert-deftest ob-athena-default-only-profile-test ()
  "Runs a minimal query and verifies that an Org-results block with
   a console link and a CSV link is inserted."
  (let ((org-src
         "#+begin_src athena :results output
SELECT * FROM test_user_profiles;
#+end_src"))
    (with-temp-buffer
      (insert org-src)
      (goto-char (point-min))
      (org-mode)

      ;; run the query
      (org-babel-execute-src-block)

      (let ((buf (substring-no-properties (buffer-string))))
        (should (string-match-p "#\\+RESULTS:" buf))
        (should (string-match-p "Query submitted. View:" buf))
        (should (string-match-p "console\\.aws\\.amazon\\.com" buf))
        (should (string-match-p "\\[\\[file:.+\\.csv" buf))))))

(provide 'ob-athena-no-babel-headers-functional-test)
;;; ob-athena-no-babel-headers-functional-test.el ends here
