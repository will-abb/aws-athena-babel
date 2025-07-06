;;; ob-athena-no-babel-headers.el --- Description -*- lexical-binding: t; -*-

(require 'ert)
(require 'org)
(require 'ob-athena)

(defvar auto-save-list-file-prefix nil)

(setq default-directory
      (or (file-name-directory load-file-name)
          (file-name-directory buffer-file-name)
          default-directory))

(ert-deftest ob-athena-default-only-profile-test ()
  "Ensure query executes successfully with only :aws-profile specified; others use default context."
  (let* ((org-src
          "#+begin_src athena
SELECT * FROM test_user_profiles;
#+end_src"))
    (with-temp-buffer
      (insert org-src)
      (goto-char (point-min))
      (org-mode)
      (let ((results (org-babel-execute-src-block)))
        (should (and (listp results)
                     (= (length results) 3)
                     (string-match-p "https://.*athena.*amazonaws.com" (nth 1 results))
                     (string-match-p "^\\[\\[file:.+\\.csv\\]\\[" (nth 2 results))))))))
