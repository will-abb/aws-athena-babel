;;; ob-athena-melpa-style-tests.el --- MELPA compliance tests for ob-athena.el -*- lexical-binding: t; -*-

(require 'ert)
(require 'checkdoc)
(require 'package-lint)

(defconst ob-athena--file "ob-athena.el")

(defun ob-athena--visit ()
  (find-file-noselect (expand-file-name ob-athena--file default-directory)))

(ert-deftest ob-athena-melpa:lexical-binding ()
  "Ensure `lexical-binding: t` is declared."
  (with-current-buffer (ob-athena--visit)
    (goto-char (point-min))
    (should (re-search-forward "lexical-binding: t" (line-end-position) t))))

(ert-deftest ob-athena-melpa:checkdoc ()
  "Run checkdoc and ensure no violations are found."
  (with-current-buffer (ob-athena--visit)
    (let ((checkdoc-violations nil))
      (checkdoc-eval-current-buffer)
      (should (null checkdoc-violations)))))

(ert-deftest ob-athena-melpa:package-lint ()
  "Ensure package-lint reports no warnings or errors."
  (with-current-buffer (ob-athena--visit)
    (let ((results (package-lint-buffer)))
      (dolist (issue results)
        (message "package-lint: %s" issue))
      (should (null results)))))

(ert-deftest ob-athena-melpa:namespacing ()
  "Ensure all functions are prefixed with `ob-athena-` or `org-babel-execute:`."
  (with-current-buffer (ob-athena--visit)
    (goto-char (point-min))
    (while (re-search-forward "^(def\\(un\\|subst\\|macro\\|var\\|const\\)\\s-+(?\\([^ \n()]+\\)" nil t)
      (let ((name (match-string 2)))
        (unless (or (string-prefix-p "ob-athena-" name)
                    (string-prefix-p "org-babel-execute:" name))
          (message "Unprefixed public symbol: %s" name)
          (should nil))))))

(ert-deftest ob-athena-melpa:long-functions ()
  "Warn if any function is longer than 80 lines."
  (with-current-buffer (ob-athena--visit)
    (goto-char (point-min))
    (let ((threshold 80))
      (while (re-search-forward "^(defun\\s-+\\(ob-athena-[^ \n(]+\\)" nil t)
        (let* ((name (match-string 1))
               (start (point))
               (end (save-excursion (end-of-defun) (point)))
               (lines (count-lines start end)))
          (when (> lines threshold)
            (message "Function %s is %d lines long" name lines)))))))

(provide 'ob-athena-melpa-style-tests)
;;; ob-athena-melpa-style-tests.el ends here
