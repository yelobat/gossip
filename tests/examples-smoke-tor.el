;;; examples-smoke-tor.el --- exercise examples/gossip-tor.el  -*- lexical-binding: t -*-

(setq gossip-enable-tor t)

(require 'gossip)
(require 'gossip-tor)

(defvar smoke-deliveries nil)
(defvar smoke-replies nil)

(defun smoke-await (predicate timeout what)
  (let ((deadline (+ (float-time) timeout)))
    (while (not (funcall predicate))
      (when (> (float-time) deadline)
        (error "timed out waiting for %s" what))
      (accept-process-output nil 0.2))))

(setq gossip-daemon-command
      (list "python3" (expand-file-name "mock/gossipd-mock.py")))
(add-hook 'gossip-delivery-functions (lambda (d) (push d smoke-deliveries)))
(add-hook 'gossip-message-functions (lambda (m) (push m smoke-replies)))

(gossip-daemon-start)

(when (gossip-tor-onion)
  (error "onion address before bootstrap?"))

(smoke-await #'gossip-tor-ready-p 30 "tor bootstrap")
(let ((onion (gossip-tor-onion)))
  (unless (and onion (string-suffix-p ".onion" onion))
    (error "no onion address after bootstrap: %S" onion))
  (message "[tor] bootstrapped, onion %s" onion))

(let ((dave (seq-find (lambda (c) (equal (plist-get c :name) "dave"))
                      (gossip-contacts))))
  (gossip-tor-send dave "hello over the onion")
  (smoke-await (lambda ()
                 (seq-find (lambda (d) (equal (plist-get d :path) "tor"))
                           smoke-deliveries))
               30 "delivery via tor")
  (smoke-await (lambda ()
                 (seq-find (lambda (m) (equal (plist-get m :from)
                                              (plist-get dave :id)))
                           smoke-replies))
               30 "dave's reply"))

(gossip-daemon-stop)
(message "[tor] TOR EXAMPLE OK")
