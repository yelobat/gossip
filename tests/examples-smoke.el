;;; examples-smoke.el --- exercise examples/gossip-ping.el for real  -*- lexical-binding: t -*-

(require 'gossip)
(require 'gossip-ping)
(require 'gossip-dired-send)

(defvar smoke-role (getenv "GOSSIP_ROLE"))
(defvar smoke-dir (getenv "GOSSIP_DIR"))
(defvar smoke-pong nil)

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

(let* ((port (if (equal smoke-role "a") "47311" "47312"))
       (gossip-data-directory (expand-file-name smoke-role smoke-dir)))
  (setq gossip-daemon-command
        (list "env" "GOSSIPD_NO_DISCOVERY=1"
              (concat "GOSSIPD_BIND=127.0.0.1:" port)
              (getenv "GOSSIPD_BIN")))
  (setq gossip-display-name smoke-role)
  (add-hook 'gossip-ping-received-functions
            (lambda (peer rtt) (setq smoke-pong (list peer rtt))))
  (gossip-daemon-start)

  (smoke-write (concat "ticket-" smoke-role) (gossip-my-ticket))
  (let* ((other (if (equal smoke-role "a") "b" "a"))
         (ticket (smoke-await-file (concat "ticket-" other) 30))
         (contact (gossip-add-contact ticket)))
    (if (equal smoke-role "a")
        (progn
          (gossip-ping contact)
          (smoke-await (lambda () smoke-pong) 60 "pong from b")
          (unless (and (equal (car smoke-pong) (plist-get contact :id))
                       (numberp (cadr smoke-pong))
                       (>= (cadr smoke-pong) 0))
            (error "bad pong: %S" smoke-pong))
          (message "[a] pong from %s in %.2fs"
                   (plist-get contact :name) (cadr smoke-pong))
          (with-current-buffer (dired (expand-file-name "outbox" smoke-dir))
            (dired-mark-files-regexp ".")
            (gossip-dired-send contact))
          (smoke-await-file "files-ok" 120)
          (message "[a] file transfers verified by b")
          (smoke-write "done-a" "ok"))
      (dolist (name '("blob.bin" "héllo wörld.txt"))
        (let ((got (expand-file-name (concat "downloads/" name)
                                     gossip-data-directory))
              (want (expand-file-name (concat "outbox/" name) smoke-dir)))
          (smoke-await (lambda ()
                         (and (file-exists-p got)
                              (= (file-attribute-size (file-attributes got))
                                 (file-attribute-size (file-attributes want)))))
                       90 (format "download of %s" name))
          (unless (equal (with-temp-buffer
                           (set-buffer-multibyte nil)
                           (insert-file-contents-literally got)
                           (buffer-string))
                         (with-temp-buffer
                           (set-buffer-multibyte nil)
                           (insert-file-contents-literally want)
                           (buffer-string)))
            (error "downloaded %s differs from the original" name))))
      (smoke-write "files-ok" "ok")
      (smoke-await-file "done-a" 60)))

  (gossip-daemon-stop)
  (message "[%s] PING SMOKE OK" smoke-role))
