;;; gossip-tor.el --- tor-aware messaging over gossip  -*- lexical-binding: t -*-

(require 'gossip)

(defvar gossip-tor-bootstrap nil
  "Latest tor bootstrap progress plist (:state :percent), nil before any.")

(defvar gossip-tor-ready-hook nil
  "Normal hook run once when tor bootstrap reaches 100%.")

(add-hook 'gossip-tor-status-functions
          (lambda (status)
            (let ((was-ready (gossip-tor-ready-p)))
              (setq gossip-tor-bootstrap status)
              (when (and (not was-ready) (gossip-tor-ready-p))
                (run-hooks 'gossip-tor-ready-hook)))))

(defun gossip-tor-ready-p ()
  "Non-nil once the tor transport has bootstrapped."
  (eql (plist-get gossip-tor-bootstrap :percent) 100))

(defun gossip-tor-onion ()
  "Return this node's onion address, or nil, messaging it when interactive."
  (interactive)
  (let* ((tor (plist-get (gossip--request 'status nil) :tor))
         (onion (and (stringp tor) (string-suffix-p ".onion" tor) tor)))
    (when (called-interactively-p 'any)
      (message "gossip-tor: %s" (or onion (format "no onion (tor is %s)" tor))))
    onion))

(defun gossip-tor-send (contact body)
  "Send BODY to CONTACT and report which transport delivered it."
  (interactive (list (gossip--read-contact) (read-string "Message: ")))
  (unless (gossip-tor-ready-p)
    (message "gossip-tor: tor not bootstrapped yet - a tor-only peer will queue"))
  (let (sent-id watcher)
    (setq watcher
          (lambda (delivery)
            (when (and sent-id (equal (plist-get delivery :msg-id) sent-id))
              (remove-hook 'gossip-delivery-functions watcher)
              (message "gossip-tor: delivered to %s via %s"
                       (plist-get contact :name)
                       (or (plist-get delivery :path) "direct")))))
    (add-hook 'gossip-delivery-functions watcher)
    (gossip-send (plist-get contact :id) body
                 :on-result (lambda (reply)
                              (setq sent-id (plist-get reply :msg-id))))))

(provide 'gossip-tor)
