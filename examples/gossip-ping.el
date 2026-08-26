;;; gossip-ping.el --- example gossip.el library consumer  -*- lexical-binding: t -*-

(require 'gossip)

(defvar gossip-ping-received-functions nil
  "Abnormal hook run with (peer-id rtt-seconds) on each pong.")

(gossip-register-kind "ping/v1"
  (lambda (msg)
    (gossip-send (plist-get msg :from) (plist-get msg :body)
                 :kind "pong/v1")))

(gossip-register-kind "pong/v1"
  (lambda (msg)
    (let ((rtt (- (float-time)
                  (string-to-number (plist-get msg :body)))))
      (run-hook-with-args 'gossip-ping-received-functions
                          (plist-get msg :from) rtt)
      (message "gossip-ping: reply from %s in %.2fs"
               (plist-get msg :from-name) rtt))))

(defun gossip-ping (contact)
  "Ping CONTACT through gossip and report the round-trip time when it lands."
  (interactive (list (gossip--read-contact)))
  (gossip-send (plist-get contact :id)
               (number-to-string (float-time))
               :kind "ping/v1"
               :on-result
               (lambda (reply)
                 (when (equal (plist-get reply :status) "queued")
                   (message "gossip-ping: %s is offline - pong will arrive when they return"
                            (plist-get contact :name))))))

(provide 'gossip-ping)
