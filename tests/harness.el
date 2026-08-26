;;; harness.el --- shared scaffolding for gossip.el e2e tests  -*- lexical-binding: t -*-

(add-to-list 'load-path (expand-file-name "." default-directory))
(require 'gossip)
(require 'cl-lib)

(setq gossip-daemon-command
      (list "python3"
            (expand-file-name "mock/gossipd-mock.py" default-directory))
      gossip-dial-backoff-initial 0.2
      gossip-dial-backoff-multiplier 2.0
      gossip-dial-backoff-jitter 0.0)

(defvar gossip-test-events nil
  "Chronological (tag . params) pairs captured from hooks.")

(defun gossip-test--record (tag)
  (lambda (params) (push (cons tag params) gossip-test-events)))

(add-hook 'gossip-queue-functions    (gossip-test--record 'queue))
(add-hook 'gossip-delivery-functions (gossip-test--record 'delivered))
(add-hook 'gossip-presence-functions (gossip-test--record 'presence))
(add-hook 'gossip-message-functions  (gossip-test--record 'received))

(defun gossip-test-pump (seconds)
  "Run the event loop for SECONDS so async notifications arrive."
  (let ((deadline (+ (float-time) seconds)))
    (while (< (float-time) deadline)
      (accept-process-output nil 0.2))))

(defun gossip-test-find (tag predicate)
  "Return the first captured event with TAG whose params satisfy PREDICATE."
  (cl-find-if (lambda (event)
                (and (eq (car event) tag)
                     (funcall predicate (cdr event))))
              gossip-test-events))

(defun gossip-test-count (tag predicate)
  "Count captured events with TAG whose params satisfy PREDICATE."
  (cl-count-if (lambda (event)
                 (and (eq (car event) tag)
                      (funcall predicate (cdr event))))
               gossip-test-events))

(defun gossip-test-assert (ok format &rest args)
  "Fail loudly (non-zero batch exit) unless OK."
  (unless ok
    (message "EVENTS: %S" (reverse gossip-test-events))
    (apply #'error (concat "FAIL: " format) args)))

(provide 'harness)
