;;; gossip-dired-send.el --- send marked Dired files to a contact  -*- lexical-binding: t -*-

(require 'gossip)
(require 'dired)

(defun gossip-dired-send (contact)
  "Send the marked Dired files (or the file at point) to CONTACT."
  (interactive (list (gossip--read-contact)))
  (let ((files (seq-remove #'file-directory-p (dired-get-marked-files))))
    (unless files
      (user-error "gossip-dired-send: no files marked (directories are skipped)"))
    (dolist (file files)
      (gossip-send-file file contact))
    (message "gossip: sending %d file%s to %s"
             (length files) (if (cdr files) "s" "")
             (plist-get contact :name))))

(provide 'gossip-dired-send)
