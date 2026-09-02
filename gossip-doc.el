;;; gossip-doc.el --- Collaborative buffer editing over gossip -*- lexical-binding: t -*-
;;
;; Author: Luke Holland
;; Package-Requires: ((emacs "28.1"))
;;
;;; Commentary:
;;
;;; Code:

(require 'gossip)

(defgroup gossip-doc nil
  "Collaborative buffer editing over gossip."
  :group 'gossip
  :prefix "gossip-doc-")

(defcustom gossip-doc-sync-timeout 0.5
  "Seconds to wait for the daemon on each sync before going degraded."
  :type 'number)

(defcustom gossip-doc-idle-interval 5
  "Seconds of idle time between keep-in-step syncs of attached buffers."
  :type 'number)

(defcustom gossip-doc-max-chars (* 2 1024 1024)
  "Refuse to share buffers longer than this many characters."
  :type 'natnum)

(defcustom gossip-doc-cursor-colors
  '("#d95f02" "#1b9e77" "#7570b3" "#e7298a" "#66a61e" "#e6ab02")
  "Background colors for peer cursors, picked by contact id."
  :type '(repeat color))

(defvar-local gossip-doc--id nil "Document id this buffer is attached to.")
(defvar-local gossip-doc--name nil "Document name.")
(defvar-local gossip-doc--batch nil "Unsent edits, newest first, as [POS DEL INS].")
(defvar-local gossip-doc--dirty nil "Non-nil when the daemon has remote edits for us.")
(defvar-local gossip-doc--degraded nil "Non-nil while waiting to re-attach.")
(defvar-local gossip-doc--in-sync nil "Non-nil while a sync request is in flight.")
(defvar-local gossip-doc--applying nil "Non-nil while applying remote edits.")
(defvar-local gossip-doc--peers nil "Last peer list from the daemon.")
(defvar-local gossip-doc--overlays nil "Alist of peer id to cursor overlay.")
(dolist (v '(gossip-doc--id gossip-doc--name gossip-doc--batch gossip-doc--dirty
             gossip-doc--degraded gossip-doc--peers gossip-doc--overlays))
  (put v 'permanent-local t))

(defvar gossip-doc--buffers (make-hash-table :test #'equal)
  "Map of document id to attached buffer.")

(defvar gossip-doc--idle-timer nil)

(defun gossip-doc--request (method params &optional timeout)
  "Call METHOD with PARAMS."
  (condition-case err
      (jsonrpc-request (gossip-ensure) method params
                       :timeout (or timeout gossip-doc-sync-timeout))
    (jsonrpc-error
     (signal 'gossip-doc-error
             (list (alist-get 'jsonrpc-error-message (cdr err)))))
    (error (signal 'gossip-doc-error (list (error-message-string err))))))

(define-error 'gossip-doc-error "gossip-doc")

(defun gossip-doc--after-change (beg end old-len)
  "Record the change BEG END OLD-LEN unless it came from the daemon.
Must never signal: Emacs drops the whole hook variable if it does."
  (unless gossip-doc--applying
    (condition-case nil
        (push (vector (1- beg) old-len
                      (save-restriction
                        (widen)
                        (buffer-substring-no-properties beg end)))
              gossip-doc--batch)
      (error (setq gossip-doc--degraded t)))))

(defun gossip-doc--apply-ops (ops)
  "Apply OPS (vector of [POS DEL INS], 0-based chars) to the buffer."
  (let ((gossip-doc--applying t)
        (inhibit-read-only t))
    (save-excursion
      (save-restriction
        (widen)
        (seq-doseq (op ops)
          (let ((pos (1+ (aref op 0)))
                (del (aref op 1))
                (ins (aref op 2)))
            (when (> del 0)
              (delete-region pos (+ pos del)))
            (unless (string-empty-p ins)
              (goto-char pos)
              (insert ins))))))))

(defun gossip-doc--diff-ops (text)
  "Return ops that turn TEXT into this buffer's text."
  (let* ((mine (save-restriction
                 (widen)
                 (buffer-substring-no-properties (point-min) (point-max))))
         (n (length text))
         (m (length mine))
         (prefix (or (cl-mismatch text mine) n))
         (suffix 0))
    (while (and (< suffix (- n prefix)) (< suffix (- m prefix))
                (eq (aref text (- n suffix 1)) (aref mine (- m suffix 1))))
      (setq suffix (1+ suffix)))
    (unless (and (= n m) (= prefix n))
      (list (vector prefix (- n prefix suffix) (substring mine prefix (- m suffix)))))))

(defun gossip-doc--sync ()
  "Flush pending edits and apply what came back. Degrade on any doubt."
  (when (and gossip-doc--id (not gossip-doc--in-sync) (not gossip-doc--degraded))
    (setq gossip-doc--in-sync t)
    (unwind-protect
        (condition-case err
            (let ((rounds 0))
              (while (and (or gossip-doc--batch gossip-doc--dirty (= rounds 0))
                          (< rounds 4))
                (setq rounds (1+ rounds))
                (let* ((ops (vconcat (nreverse gossip-doc--batch)))
                       (_ (setq gossip-doc--batch nil gossip-doc--dirty nil))
                       (reply (gossip-doc--request
                               'doc/sync
                               (list :id gossip-doc--id :ops ops
                                     :cursor (1- (point))))))
                  (gossip-doc--apply-ops (plist-get reply :ops))
                  (unless (= (plist-get reply :size) (buffer-size))
                    (signal 'gossip-doc-error
                            (list (format "size mismatch (%d here, %d in daemon)"
                                          (buffer-size) (plist-get reply :size)))))
                  (gossip-doc--show-peers (plist-get reply :peers)))))
          (gossip-doc-error (gossip-doc--degrade (cadr err))))
      (setq gossip-doc--in-sync nil))))

(defun gossip-doc--degrade (why)
  "Enter degraded mode because of WHY and schedule a re-attach."
  (setq gossip-doc--degraded t
        gossip-doc--batch nil
        gossip-doc--dirty nil)
  (message "gossip-doc: %s: %s; re-syncing shortly" gossip-doc--name why)
  (force-mode-line-update)
  (run-with-timer 2 nil #'gossip-doc--reattach (current-buffer)))

(defun gossip-doc--reattach (buffer)
  "Re-attach BUFFER by diffing it against the daemon's copy."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (when gossip-doc--id
        (condition-case err
            (progn
              (gossip-doc--attach nil)
              (message "gossip-doc: %s back in sync" gossip-doc--name))
          (gossip-doc-error
           (setq gossip-doc--degraded t)
           (run-with-timer 10 nil #'gossip-doc--reattach buffer)
           (message "gossip-doc: %s still out of sync: %s" gossip-doc--name (cadr err))))))))

(defun gossip-doc--attach (take-remote)
  "Attach this buffer to `gossip-doc--id'.
With TAKE-REMOTE replace the buffer text with the daemon's.
Otherwise send the difference as our edits, keeping anything typed meanwhile."
  (let* ((reply (gossip-doc--request 'doc/attach (list :id gossip-doc--id) 10))
         (text (plist-get reply :text)))
    (setq gossip-doc--batch nil
          gossip-doc--dirty nil
          gossip-doc--degraded nil)
    (if take-remote
        (let ((gossip-doc--applying t)
              (inhibit-read-only t)
              (src (current-buffer)))
          (save-restriction
            (widen)
            (with-temp-buffer
              (insert text)
              (let ((tmp (current-buffer)))
                (with-current-buffer src
                  (with-suppressed-warnings ((obsolete replace-buffer-contents))
                    (replace-buffer-contents tmp 2.0)))))))
      (setq gossip-doc--batch (nreverse (gossip-doc--diff-ops text))))
    (puthash gossip-doc--id (current-buffer) gossip-doc--buffers)
    (gossip-doc--sync)))

(defun gossip-doc--post-command ()
  "Flush edits made by the command that just ran."
  (when (and gossip-doc--id (or gossip-doc--batch gossip-doc--dirty))
    (gossip-doc--sync)))

(defun gossip-doc--on-dirty (id)
  "The daemon has remote edits for document ID."
  (when-let* ((buffer (gethash id gossip-doc--buffers)))
    (when (buffer-live-p buffer)
      (with-current-buffer buffer
        (setq gossip-doc--dirty t)
        (run-at-time 0 nil #'gossip-doc--idle-sync buffer)))))

(defun gossip-doc--idle-sync (buffer)
  "Sync BUFFER now, flushing edits made outside any command."
  (when (buffer-live-p buffer)
    (with-current-buffer buffer
      (when gossip-doc--id
        (gossip-doc--sync)))))

(defun gossip-doc--idle-all ()
  "Keep every attached buffer in step."
  (maphash (lambda (_id buffer)
             (when (buffer-live-p buffer)
               (with-current-buffer buffer
                 (setq gossip-doc--dirty t))
               (gossip-doc--idle-sync buffer)))
           gossip-doc--buffers))

(defun gossip-doc--peer-color (id)
  "Pick a stable color for peer ID."
  (nth (mod (sxhash-equal id) (length gossip-doc-cursor-colors))
       gossip-doc-cursor-colors))

(defun gossip-doc--show-peers (peers)
  "Draw a cursor overlay for each of PEERS."
  (setq gossip-doc--peers (append peers nil))
  (let ((seen nil))
    (save-restriction
      (widen)
      (dolist (peer gossip-doc--peers)
        (let* ((id (plist-get peer :id))
               (pos (min (1+ (plist-get peer :pos)) (point-max)))
               (ov (or (alist-get id gossip-doc--overlays nil nil #'equal)
                       (let ((o (make-overlay pos pos)))
                         (push (cons id o) gossip-doc--overlays)
                         o)))
               (color (gossip-doc--peer-color id))
               (name (plist-get peer :name)))
          (push id seen)
          (if (< pos (point-max))
              (progn
                (move-overlay ov pos (1+ pos))
                (overlay-put ov 'after-string nil)
                (overlay-put ov 'face `(:background ,color :foreground "white")))
            (move-overlay ov pos pos)
            (overlay-put ov 'face nil)
            (overlay-put ov 'after-string
                         (propertize "▏" 'face `(:foreground ,color :weight bold))))
          (overlay-put ov 'help-echo name)
          (overlay-put ov 'priority 100))))
    (dolist (entry gossip-doc--overlays)
      (unless (member (car entry) seen)
        (delete-overlay (cdr entry))))
    (setq gossip-doc--overlays
          (seq-filter (lambda (e) (member (car e) seen)) gossip-doc--overlays)))
  (force-mode-line-update))

(defun gossip-doc--lighter ()
  "Mode-line lighter: shared doc, peers online, and a bang when desynced."
  (format " ⇄%s%s" (length gossip-doc--peers) (if gossip-doc--degraded "!" "")))

(defun gossip-doc--install-hooks ()
  "Add the buffer-local hooks."
  (add-hook 'after-change-functions #'gossip-doc--after-change nil t)
  (add-hook 'post-command-hook #'gossip-doc--post-command nil t)
  (add-hook 'kill-buffer-hook #'gossip-doc--kill nil t))

(defun gossip-doc--remove-hooks ()
  "Remove the buffer-local hooks."
  (remove-hook 'after-change-functions #'gossip-doc--after-change t)
  (remove-hook 'post-command-hook #'gossip-doc--post-command t)
  (remove-hook 'kill-buffer-hook #'gossip-doc--kill t))

(defun gossip-doc--after-major-mode ()
  "Re-install hooks after `kill-all-local-variables' wiped them."
  (when gossip-doc--id
    (gossip-doc-mode 1)))

(defun gossip-doc--kill ()
  "Detach when the buffer is killed."
  (when gossip-doc--id
    (ignore-errors (gossip-doc--request 'doc/detach (list :id gossip-doc--id) 2))
    (remhash gossip-doc--id gossip-doc--buffers)))

(define-minor-mode gossip-doc-mode
  "Keep this buffer in sync with a shared gossip document."
  :lighter (:eval (gossip-doc--lighter))
  (if gossip-doc-mode
      (progn
        (gossip-doc--install-hooks)
        (add-hook 'after-change-major-mode-hook #'gossip-doc--after-major-mode)
        (unless gossip-doc--idle-timer
          (setq gossip-doc--idle-timer
                (run-with-idle-timer gossip-doc-idle-interval t #'gossip-doc--idle-all))))
    (gossip-doc--remove-hooks)
    (dolist (e gossip-doc--overlays) (delete-overlay (cdr e)))
    (setq gossip-doc--overlays nil gossip-doc--peers nil)))

(defun gossip-doc--read-contacts ()
  "Prompt for one or more contacts."
  (let* ((contacts (gossip-contacts))
         (names (mapcar (lambda (c) (plist-get c :name)) contacts))
         (chosen (completing-read-multiple "Share with (comma-separated): " names nil t)))
    (seq-filter (lambda (c) (member (plist-get c :name) chosen)) contacts)))

;;;###autoload
(defun gossip-doc-share (contacts)
  "Share the current buffer with CONTACTS and start editing together."
  (interactive (list (gossip-doc--read-contacts)))
  (when gossip-doc--id
    (user-error "This buffer is already shared as %s" gossip-doc--name))
  (when (> (buffer-size) gossip-doc-max-chars)
    (user-error "Buffer is larger than gossip-doc-max-chars (%d)" gossip-doc-max-chars))
  (unless enable-multibyte-characters
    (user-error "Only multibyte (text) buffers can be shared"))
  (let* ((name (buffer-name))
         (created (gossip-doc--request 'doc/create (list :name name) 10)))
    (setq gossip-doc--id (plist-get created :id)
          gossip-doc--name name)
    (gossip-doc-mode 1)
    (gossip-doc--attach nil)
    (when contacts
      (gossip-doc--request 'doc/invite
                           (list :id gossip-doc--id
                                 :to (vconcat (mapcar (lambda (c) (plist-get c :id)) contacts)))
                           10))
    (message "gossip-doc: sharing %s with %s" name
             (mapconcat (lambda (c) (plist-get c :name)) contacts ", "))))

(defun gossip-doc--read-doc ()
  "Prompt for a shared document."
  (let* ((docs (append (gossip-doc--request 'doc/list nil 10) nil))
         (labels (mapcar (lambda (d)
                           (format "%s [%s%s]" (plist-get d :name) (plist-get d :state)
                                   (if-let* ((buf (gethash (plist-get d :id) gossip-doc--buffers)))
                                       (format ", open in %s" (buffer-name buf))
                                     "")))
                         docs)))
    (when (null docs)
      (user-error "No shared documents. Wait for an invitation or share a buffer"))
    (nth (cl-position (completing-read "Document: " labels nil t) labels :test #'equal) docs)))

;;;###autoload
(defun gossip-doc-join (doc &optional merge)
  "Open shared DOC in a buffer and start editing.
With prefix argument MERGE, attach the current buffer instead and send its
text as edits over the shared copy (use with care: a stale file would
overwrite everyone's work)."
  (interactive (list (gossip-doc--read-doc) current-prefix-arg))
  (let ((id (plist-get doc :id))
        (name (plist-get doc :name)))
    (when-let* ((existing (gethash id gossip-doc--buffers)))
      (when (buffer-live-p existing)
        (pop-to-buffer existing)
        (user-error "%s is already open here" name)))
    (gossip-doc--request 'doc/join (list :id id) 10)
    (unless merge
      (pop-to-buffer (generate-new-buffer (format "*gossip doc: %s*" name)))
      (let ((mode (assoc-default name auto-mode-alist #'string-match)))
        (when (and mode (symbolp mode)) (funcall mode))))
    (when gossip-doc--id
      (user-error "This buffer is already shared as %s" gossip-doc--name))
    (setq gossip-doc--id id gossip-doc--name name)
    (gossip-doc-mode 1)
    (gossip-doc--attach (not merge))
    (message "gossip-doc: joined %s" name)))

;;;###autoload
(defun gossip-doc-leave ()
  "Stop sharing the current buffer's document and forget it."
  (interactive)
  (unless gossip-doc--id (user-error "This buffer is not shared"))
  (let ((id gossip-doc--id) (name gossip-doc--name))
    (gossip-doc-mode -1)
    (remhash id gossip-doc--buffers)
    (setq gossip-doc--id nil gossip-doc--name nil)
    (gossip-doc--request 'doc/leave (list :id id) 10)
    (message "gossip-doc: left %s" name)))

;;;###autoload
(defun gossip-doc-resync ()
  "Force a re-attach: diff this buffer against the daemon's copy."
  (interactive)
  (unless gossip-doc--id (user-error "This buffer is not shared"))
  (gossip-doc--attach nil)
  (message "gossip-doc: %s re-synced" gossip-doc--name))

;;;###autoload
(defun gossip-doc-list ()
  "Echo the shared documents and their members."
  (interactive)
  (let ((docs (append (gossip-doc--request 'doc/list nil 10) nil)))
    (if (null docs)
        (message "gossip-doc: no shared documents")
      (message "%s"
               (mapconcat
                (lambda (d)
                  (format "%s [%s] %s" (plist-get d :name) (plist-get d :state)
                          (mapconcat (lambda (m)
                                       (format "%s%s" (if (eq (plist-get m :online) t) "●" "○")
                                               (plist-get m :name)))
                                     (plist-get d :members) " ")))
                docs "\n")))))

;;;; Notifications

(defun gossip-doc--on-notification (method params)
  "Handle doc notifications METHOD PARAMS.
Returns non-nil when handled."
  (pcase method
    ('doc/dirty (gossip-doc--on-dirty (plist-get params :id)) t)
    ('doc/invited
     (let ((from (plist-get params :from-name)) (name (plist-get params :name)))
       (gossip--chat-system (plist-get params :from)
                            (format "invited you to edit %s (M-x gossip-doc-join)" name))
       (message "gossip-doc: %s invited you to edit %s (M-x gossip-doc-join)" from name))
     t)))

(add-hook 'gossip-notification-functions #'gossip-doc--on-notification)
(gossip-register-kind "doc-invite"
                      (lambda (msg)
                        (gossip--chat-system (plist-get msg :from)
                                             "shared a document with you")))

(with-eval-after-load 'gossip
  (transient-append-suffix 'gossip-menu '(1 -1)
    ["Shared docs"
     ("S" "share this buffer" gossip-doc-share)
     ("J" "join a document" gossip-doc-join)
     ("L" "leave document" gossip-doc-leave)
     ("D" "list documents" gossip-doc-list)]))

(provide 'gossip-doc)
;;; gossip-doc.el ends here
