;;; real-smoke.el --- two-Emacs smoke against the real gossipd  -*- lexical-binding: t -*-

(require 'gossip)

(defvar smoke-role (getenv "GOSSIP_ROLE"))
(defvar smoke-dir (getenv "GOSSIP_DIR"))
(defvar smoke-received nil)

(defun smoke-file (name) (expand-file-name name smoke-dir))

(defun smoke-write (name text)
  (with-temp-file (smoke-file (concat name ".tmp"))
    (insert text))
  (rename-file (smoke-file (concat name ".tmp")) (smoke-file name) t))

(defun smoke-await-file (name timeout)
  (let ((deadline (+ (float-time) timeout)))
    (while (not (file-exists-p (smoke-file name)))
      (when (> (float-time) deadline)
        (error "timed out waiting for %s" name))
      (sleep-for 0.2))
    (with-temp-buffer
      (insert-file-contents (smoke-file name))
      (string-trim (buffer-string)))))

(defun smoke-await (predicate timeout what)
  (let ((deadline (+ (float-time) timeout)))
    (while (not (funcall predicate))
      (when (> (float-time) deadline)
        (error "timed out waiting for %s" what))
      (accept-process-output nil 0.2))))

(let* ((port (if (equal smoke-role "a") "47301" "47302"))
       (gossip-data-directory (expand-file-name smoke-role smoke-dir)))
  (setq gossip-daemon-command
        (list "env" "GOSSIPD_NO_DISCOVERY=1"
              (concat "GOSSIPD_BIND=127.0.0.1:" port)
              (getenv "GOSSIPD_BIN")))
  (setq gossip-display-name (if (equal smoke-role "a") "alice" "bob"))
  (add-hook 'gossip-message-functions
            (lambda (msg) (push (plist-get msg :body) smoke-received)))
  (gossip-daemon-start)
  (message "[%s] node %s" smoke-role gossip--node-id)

  (smoke-write (concat "ticket-" smoke-role) (gossip-my-ticket))
  (let* ((other (if (equal smoke-role "a") "b" "a"))
         (ticket (smoke-await-file (concat "ticket-" other) 30))
         (contact (gossip-add-contact ticket)))
    (message "[%s] added %s" smoke-role (plist-get contact :name))

    (if (equal smoke-role "a")
        (progn
          (gossip-send (plist-get contact :id) "hello from alice")
          (smoke-await (lambda () (member "hello from bob" smoke-received))
                       60 "bob's reply")
          (message "[a] got bob's reply, chat history: %S"
                   (mapcar (lambda (m) (plist-get m :body))
                           (gossip--request 'msg/history
                                            (list :peer-id (plist-get contact :id)
                                                  :limit 10))))
          (smoke-write "done-a" "ok"))
      (smoke-await (lambda () (member "hello from alice" smoke-received))
                   60 "alice's message")
      (gossip-send (plist-get contact :id) "hello from bob")
      (smoke-await-file "done-a" 60)
      (smoke-write "done-b" "ok")))

  (gossip-daemon-stop)
  (message "[%s] SMOKE OK" smoke-role))
