;;; ob-athena-test-helpers.el --- Shared helpers for ob-athena tests -*- lexical-binding: t; -*-

(require 'cl-lib)

(defconst ob-athena-test-helper-dir
  (file-name-directory (or load-file-name buffer-file-name default-directory))
  "Directory containing shared test helpers for ob-athena.")

(defun ob-athena-test-file-path (relative-path)
  "Return absolute path for RELATIVE-PATH under the tests directory."
  (expand-file-name relative-path ob-athena-test-helper-dir))

(defun ob-athena-test-fixture-path (filename)
  "Return absolute path for fixture FILENAME under tests/fixtures."
  (ob-athena-test-file-path (expand-file-name filename "fixtures")))

(defun ob-athena-test-reset-state ()
  "Reset global monitor state that can leak between interactive tests."
  (when (and (boundp 'ob-athena-query-status-timer)
             (timerp ob-athena-query-status-timer))
    (cancel-timer ob-athena-query-status-timer))
  (setq ob-athena-query-status-timer nil)
  (when (boundp 'ob-athena-total-cost)
    (setq ob-athena-total-cost 0.0))
  (dolist (buffer-name '("*Athena Monitor*"
                         "*Athena Raw Results*"
                         "*Athena JSON Results*"))
    (when (get-buffer buffer-name)
      (kill-buffer buffer-name))))

(defmacro ob-athena-test-with-clean-state (&rest body)
  "Run BODY with isolated timer and buffer state."
  (declare (indent 0) (debug t))
  `(let ((orig-cancel (symbol-function 'cancel-timer)))
     (cl-letf (((symbol-function 'cancel-timer)
                (lambda (timer &rest args)
                  (when (timerp timer)
                    (apply orig-cancel timer args)))))
       (unwind-protect
           (progn
             (ob-athena-test-reset-state)
             ,@body)
         (ob-athena-test-reset-state)))))

(defmacro ob-athena-test-with-temp-output-dir (dir-var &rest body)
  "Bind DIR-VAR to a temporary directory while running BODY."
  (declare (indent 1) (debug (symbolp body)))
  `(let ((,dir-var (make-temp-file "ob-athena-test-output-" t)))
     (unwind-protect
         (progn ,@body)
       (when (file-directory-p ,dir-var)
         (delete-directory ,dir-var t)))))

(provide 'ob-athena-test-helpers)
;;; ob-athena-test-helpers.el ends here
