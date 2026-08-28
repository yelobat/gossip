;;; gossip.el --- Serverless P2P messaging for Emacs -*- lexical-binding: t -*-
;;
;; Copyright (C) 2025 Luke Holland
;;
;; Author: Luke Holland
;; Maintainer: Luke Holland
;; Created: September 07, 2025
;; Modified: September 30, 2025
;; Version: 0.1.0
;; Keywords: lisp tools
;; Homepage: https://github.com/yelobat/gossip
;; Package-Requires: ((emacs "28.1"))
;;
;; This file is not part of GNU Emacs.
;;
;;; Commentary:
;;
;; # Overview
;;
;;; Code:

(require 'jsonrpc)
(require 'transient)
(require 'cl-lib)
(require 'subr-x)

(defgroup gossip nil
  "Serverless P2P messaging for Emacs."
  :group 'comm
  :prefix "gossip-")

(defcustom gossip-daemon-command '("gossipd")
  "Command to launch the gossip daemon."
  :type '(repeat string))

(defcustom gossip-data-directory
  (expand-file-name "gossip/" user-emacs-directory)
  "Directory the daemon uses for keys and messages."
  :type 'directory)

(defcustom gossip-display-name (user-login-name)
  "Display name announced to contacts."
  :type 'string)

(defcustom gossip-bind-address nil
  "Fixed IP:PORT to bind, or nil for an ephemeral mDNS-resolved port."
  :type '(choice (const :tag "Ephemeral (default)" nil) string))

(defcustom gossip-dial-backoff-initial 1.0
  "Initial delay in seconds before redialing an unreachable peer."
  :type 'number)

(defcustom gossip-dial-backoff-max 300.0
  "Maximum amount of seconds for the redial backoff."
  :type 'number)

(defcustom gossip-dial-backoff-multiplier 2.0
  "Factor applied to the redial delay after each failed attempt."
  :type 'number)

(defcustom gossip-dial-backoff-jitter 0.2
  "Random jitter applied to each redial delay."
  :type 'number)

(defcustom gossip-dial-backoff-max-attempts 12
  "Stop actively redialing a peer after this many failed attempts."
  :type 'natnum)

(defcustom gossip-allow-relays nil
  "If non-nil, permit relayed transport as a fallback."
  :type 'boolean)

(defcustom gossip-relay-urls nil
  "Relay URLs used when `gossip-allow-relays' is non-nil."
  :type '(repeat string))

(defcustom gossip-enable-tor nil
  "If non-nil, run the Tor transport alongside direct."
  :type 'boolean)

(defcustom gossip-advertised-addresses nil
  "Extra static addresses to publish as directly reachable."
  :type '(repeat string))

(defface gossip-self-face
  '((t :inherit font-lock-keyword-face :weight bold))
  "Face for your own name in chat buffers.")

(defface gossip-peer-face
  '((t :inherit font-lock-function-name-face :weight bold))
  "Face for peer names in chat buffers.")

(defface gossip-system-face
  '((t :inherit shadow :slant italic))
  "Face for system lines in chat buffers.")

(defvar gossip--connection nil
  "Connection to gossipd, or nil.")

(defvar gossip--node-id nil
  "This node's public key id, as reported by the daemon.")

(defvar gossip--kind-handlers (make-hash-table :test #'equal)
  "Map of payload kind to handler function.")

(defvar gossip--chat-buffers (make-hash-table :test #'equal)
  "Map of peer id to chat buffer.")

(defvar gossip-message-functions nil
  "Message hook run with each incoming message.")

(defvar gossip-delivery-functions nil
  "Message hook run with a plist when a queued message is delivered.")

(defvar gossip-presence-functions nil
  "Message hook run with a plist when a peer's presence changes.")

(defvar gossip-queue-functions nil
  "Message hook run with a plist on each redelivery attempt.")

(defvar gossip-tor-status-functions nil
  "Message hook run with a plist on tor bootstrap progress.")

(defun gossip--live-p ()
  "Return non-nil if the daemon connection is usable."
  (and gossip--connection (jsonrpc-running-p gossip--connection)))

(defun gossip--on-shutdown (_conn)
  "Handle daemon shutdown."
  (setq gossip--connection nil
        gossip--node-id nil)
  (message "gossip: daemon stopped"))

(defun gossip--control-file ()
  "Path to this profile's daemon control file (loopback port + token)."
  (expand-file-name "gossip.port" (expand-file-name gossip-data-directory)))

(defun gossip--read-control (file)
  "Return (PORT . TOKEN) parsed from control FILE, or nil if unreadable."
  (ignore-errors
    (with-temp-buffer
      (insert-file-contents file)
      (let ((lines (split-string (buffer-string) "\n" t)))
        (when (>= (length lines) 2)
          (cons (string-to-number (car lines)) (string-trim (nth 1 lines))))))))

(defun gossip--send-auth (process token)
  "Send the framed control-port auth message carrying TOKEN to PROCESS.
TOKEN is daemon-generated lowercase hex, so no JSON escaping is needed."
  (let ((body (format "{\"auth\":\"%s\"}" token)))
    (process-send-string
     process
     (format "Content-Length: %d\r\n\r\n%s" (string-bytes body) body))))

(defun gossip--daemon-process ()
  "Return a process talking to gossipd.
Attach to a daemon already listening on the profile control port if
there is one, otherwise spawn a daemon that also listens on it so a
terminal or Bevy client can attach to the same identity."
  (let* ((control (gossip--control-file))
         (info (and (file-exists-p control) (gossip--read-control control)))
         (process
          (and info
               (ignore-errors
                 (let ((proc (make-network-process
                              :name "gossipd" :host "127.0.0.1"
                              :service (car info)
                              :coding 'utf-8-emacs-unix :noquery t)))
                   (gossip--send-auth proc (cdr info))
                   proc)))))
    (or process
        (let* ((stderr-buffer (get-buffer-create " *gossipd stderr*"))
               (process-environment
                (append (list (concat "GOSSIPD_CONTROL=" control))
                        (and gossip-bind-address
                             (list (concat "GOSSIPD_BIND=" gossip-bind-address)))
                        process-environment))
               (proc (make-process
                      :name "gossipd"
                      :command gossip-daemon-command
                      :connection-type 'pipe
                      :coding 'utf-8-emacs-unix
                      :noquery t
                      :stderr stderr-buffer)))
          (when-let* ((stderr-process (get-buffer-process stderr-buffer)))
            (set-process-query-on-exit-flag stderr-process nil))
          proc))))

(defun gossip--make-connection ()
  "Return a jsonrpc connection to gossipd, sharing a running daemon if any."
  (make-instance 'jsonrpc-process-connection
                 :name "gossip"
                 :process (gossip--daemon-process)
                 :notification-dispatcher #'gossip--dispatch-notification
                 :request-dispatcher #'gossip--dispatch-request
                 :on-shutdown #'gossip--on-shutdown))

;;;###autoload
(defun gossip-daemon-start ()
  "Start the gossip daemon and initialize the session."
  (interactive)
  (if (gossip--live-p)
      (message "gossip: daemon already running (node %s)" gossip--node-id)
    (setq gossip--connection (gossip--make-connection))
    (let ((reply (jsonrpc-request
                  gossip--connection 'init
                  (list :data-dir (expand-file-name gossip-data-directory)
                        :display-name gossip-display-name
                        :transport (list :allow-relays (if gossip-allow-relays
                                                           t :json-false)
                                         :relay-urls (vconcat gossip-relay-urls)
                                         :tor (list :enabled
                                                    (if gossip-enable-tor
                                                        t :json-false))
                                         :advertised-addrs
                                         (vconcat gossip-advertised-addresses))
                        :backoff (list :initial-seconds gossip-dial-backoff-initial
                                       :max-seconds gossip-dial-backoff-max
                                       :multiplier gossip-dial-backoff-multiplier
                                       :jitter gossip-dial-backoff-jitter
                                       :max-attempts gossip-dial-backoff-max-attempts)))))
      (setq gossip--node-id (plist-get reply :node-id))
      (message "gossip: up as %s" gossip--node-id))))

(defun gossip-daemon-stop ()
  "Stop the gossip daemon."
  (interactive)
  (when (gossip--live-p)
    (ignore-errors (jsonrpc-request gossip--connection 'shutdown nil))
    (jsonrpc-shutdown gossip--connection))
  (setq gossip--connection nil gossip--node-id nil))

(defun gossip-ensure ()
  "Return a live daemon connection, starting one if necessary."
  (unless (gossip--live-p)
    (gossip-daemon-start))
  gossip--connection)

(defun gossip--request (method &optional params)
  "Send synchronous METHOD with PARAMS, with friendly errors."
  (condition-case err
      (jsonrpc-request (gossip-ensure) method params)
    (jsonrpc-error
     (user-error "gossip: %s" (alist-get 'jsonrpc-error-message (cdr err))))))

(defun gossip-node-id ()
  "Return this node's id, ensuring the daemon is running."
  (gossip-ensure)
  gossip--node-id)

(defun gossip-contacts ()
  "Return the contact list as a list of plists."
  (append (gossip--request 'contact/list) nil))

(defun gossip-my-ticket ()
  "Return (and show) an invite ticket for this node."
  (interactive)
  (let ((ticket (plist-get (gossip--request 'contact/makeTicket) :ticket)))
    (kill-new ticket)
    (message "gossip: ticket copied to kill ring: %s" ticket)
    ticket))

(defun gossip-add-contact (ticket &optional name)
  "Add a contact from TICKET, optionally naming them NAME."
  (interactive (list (read-string "Ticket: ")
                     (read-string "Name (optional): " nil nil nil)))
  (let ((contact (gossip--request
                  'contact/addTicket
                  (append (list :ticket ticket)
                          (and name (not (string-empty-p name))
                               (list :name name))))))
    (message "gossip: added %s (%s)"
             (plist-get contact :name) (plist-get contact :id))
    contact))

(cl-defun gossip-send (recipient body &key (kind "chat") on-result)
  "Send BODY to RECIPIENT under KIND, passing the reply plist to ON-RESULT."
  (jsonrpc-async-request
   (gossip-ensure) 'msg/send
   (list :to recipient :kind kind :body body)
   :success-fn (lambda (reply)
                 (when on-result (funcall on-result reply)))
   :error-fn (lambda (err)
               (message "gossip: send failed: %s"
                        (plist-get err :message)))))

(defun gossip-register-kind (kind handler)
  "Register HANDLER to run on each incoming message of KIND."
  (puthash kind handler gossip--kind-handlers))

(defun gossip-unregister-kind (kind)
  "Remove the handler for KIND."
  (remhash kind gossip--kind-handlers))

(defun gossip--dispatch-request (_conn method _params)
  "Daemon-to-Emacs requests are not part of the protocol yet."
  (jsonrpc-error "Unknown method: %s" method))

(defun gossip--dispatch-notification (_conn method params)
  "Route daemon notification METHOD with PARAMS."
  (pcase method
    ('msg/received (gossip--handle-incoming params))
    ('msg/sent (gossip--handle-sent params))
    ('file/incoming (run-at-time 0 nil #'gossip--prompt-file params))
    ('file/declined (gossip--handle-file-declined params))
    ('msg/delivered
     (run-hook-with-args 'gossip-delivery-functions params)
     (gossip--chat-system (plist-get params :to)
                          (format "delivered ✓%s (%s)"
                                  (if-let* ((path (plist-get params :path)))
                                      (format " via %s" path)
                                    "")
                                  (plist-get params :msg-id))))
    ('tor/status
     (run-hook-with-args 'gossip-tor-status-functions params)
     (message "gossip: tor %s (%d%%)"
              (plist-get params :state) (plist-get params :percent)))
    ('peer/presence
     (run-hook-with-args 'gossip-presence-functions params)
     (gossip--chat-system (plist-get params :peer-id)
                          (if (eq (plist-get params :online) t)
                              (format "peer is online%s"
                                      (if-let* ((path (plist-get params :path)))
                                          (format " via %s" path)
                                        ""))
                            "peer went offline")))
    ('queue/update
     (run-hook-with-args 'gossip-queue-functions params)
     (gossip--chat-system (plist-get params :to)
                          (format "queued - attempt %d failed, retrying in %.1fs"
                                  (plist-get params :attempts)
                                  (plist-get params :delay-seconds))))
    ('transfer/progress
     (message "gossip: transfer %s %d%%"
              (plist-get params :transfer-id) (plist-get params :percent)))
    ('log
     (message "gossipd[%s]: %s"
              (plist-get params :level) (plist-get params :message)))
    (_ (message "gossip: unhandled notification %S" method))))

(defun gossip--handle-incoming (msg)
  "Handle an incoming message plist MSG: hooks, then kind dispatch."
  (run-hook-with-args 'gossip-message-functions msg)
  (let* ((kind (or (plist-get msg :kind) "chat"))
         (handler (gethash kind gossip--kind-handlers)))
    (cond
     ((member kind '("chat" "file"))
      (gossip--chat-render-incoming msg))
     (handler (funcall handler msg))
     (t (message "gossip: message of unregistered kind %S from %s"
                 kind (plist-get msg :from-name))))))

(defun gossip--handle-sent (msg)
  "Render an outgoing MSG (from any client on this identity) into its buffer."
  (let* ((peer-id (plist-get msg :to))
         (peer-name (or (plist-get msg :to-name) peer-id))
         (buffer (gossip--chat-buffer peer-id peer-name)))
    (gossip--chat-append buffer "you" (gossip--chat-body msg)
                         'gossip-self-face (plist-get msg :ts))))

(defvar-local gossip--chat-peer-id nil)
(defvar-local gossip--chat-peer-name nil)

(defvar gossip-chat-mode-map
  (let ((map (make-sparse-keymap)))
    (set-keymap-parent map special-mode-map)
    (define-key map (kbd "RET") #'gossip-chat-send)
    (define-key map (kbd "g") #'gossip-chat-refresh)
    map)
  "Keymap for `gossip-chat-mode'.")

(define-derived-mode gossip-chat-mode special-mode "Gossip-Chat"
  "Major mode for gossip.el chat buffers."
  (setq-local truncate-lines nil))

(defun gossip--chat-buffer (peer-id peer-name)
  "Return (creating if needed) the chat buffer for PEER-ID / PEER-NAME."
  (or (let ((buffer (gethash peer-id gossip--chat-buffers)))
        (and (buffer-live-p buffer) buffer))
      (let ((buffer (get-buffer-create (format "*gossip: %s*" peer-name))))
        (with-current-buffer buffer
          (gossip-chat-mode)
          (setq gossip--chat-peer-id peer-id
                gossip--chat-peer-name peer-name))
        (puthash peer-id buffer gossip--chat-buffers)
        buffer)))

(defun gossip--chat-append (buffer sender body face &optional ts)
  "Append a chat line from SENDER with BODY to BUFFER using FACE."
  (with-current-buffer buffer
    (let ((inhibit-read-only t)
          (at-end (eobp)))
      (save-excursion
        (goto-char (point-max))
        (insert (format-time-string "[%H:%M] " ts)
                (propertize (format "%s: " sender) 'face face)
                body "\n"))
      (when at-end (goto-char (point-max))))))

(defun gossip--chat-system (peer-id text)
  "Append system TEXT to PEER-ID's chat buffer, if it exists."
  (when peer-id
    (when-let* ((buffer (gethash peer-id gossip--chat-buffers)))
      (when (buffer-live-p buffer)
        (with-current-buffer buffer
          (let ((inhibit-read-only t))
            (save-excursion
              (goto-char (point-max))
              (insert (propertize (concat "· " text) 'face 'gossip-system-face)
                      "\n"))))))))

(defun gossip--chat-render-incoming (msg)
  "Render incoming chat MSG into the right buffer."
  (let* ((peer-id (plist-get msg :from))
         (peer-name (or (plist-get msg :from-name) peer-id))
         (buffer (gossip--chat-buffer peer-id peer-name)))
    (gossip--chat-append buffer peer-name (gossip--chat-body msg)
                         'gossip-peer-face (plist-get msg :ts))
    (unless (get-buffer-window buffer)
      (message "gossip: %s: %s" peer-name (plist-get msg :body)))))

(defun gossip--read-contact ()
  "Prompt for a contact and return its plist."
  (let* ((contacts (gossip-contacts))
         (names (mapcar (lambda (c)
                          (format "%s%s"
                                  (plist-get c :name)
                                  (if (eq (plist-get c :online) t)
                                      "" " (offline)")))
                        contacts))
         (choice (completing-read "Contact: " names nil t)))
    (nth (cl-position choice names :test #'equal) contacts)))

;;;###autoload
(defun gossip-chat (contact)
  "Open a chat buffer with CONTACT (prompted interactively)."
  (interactive (list (gossip--read-contact)))
  (let* ((peer-id (plist-get contact :id))
         (buffer (gossip--chat-buffer peer-id (plist-get contact :name))))
    (gossip-chat-refresh buffer)
    (pop-to-buffer buffer)))

(defun gossip-chat-refresh (&optional buffer)
  "Reload history into BUFFER (or the current chat buffer)."
  (interactive)
  (with-current-buffer (or buffer (current-buffer))
    (unless (derived-mode-p 'gossip-chat-mode)
      (user-error "Not a gossip chat buffer"))
    (let ((history (gossip--request 'msg/history
                                    (list :peer-id gossip--chat-peer-id
                                          :limit 100)))
          (inhibit-read-only t))
      (erase-buffer)
      (seq-do (lambda (msg)
                (let ((mine (equal (plist-get msg :from) gossip--node-id)))
                  (gossip--chat-append
                   (current-buffer)
                   (if mine "you" (or (plist-get msg :from-name)
                                      gossip--chat-peer-name))
                   (gossip--chat-body msg)
                   (if mine 'gossip-self-face 'gossip-peer-face)
                   (plist-get msg :ts))))
              history))))

(defun gossip--chat-body (msg)
  "Return the display text for chat MSG, labelling file transfers."
  (if (equal (plist-get msg :kind) "file")
      (format "[file] %s" (plist-get msg :body))
    (plist-get msg :body)))

(defun gossip-chat-send (text)
  "Prompt for TEXT and send it to the current chat buffer's peer."
  (interactive (list (read-string
                      (format "To %s: " gossip--chat-peer-name))))
  (unless (derived-mode-p 'gossip-chat-mode)
    (user-error "Not a gossip chat buffer"))
  (let ((peer-id gossip--chat-peer-id))
    (gossip-send peer-id text
                 :on-result
                 (lambda (reply)
                   (when (equal (plist-get reply :status) "queued")
                     (gossip--chat-system
                      peer-id
                      (format "peer unreachable - %s queued, backoff engaged"
                              (plist-get reply :msg-id))))))))

;;;###autoload
(defun gossip-send-region (start end)
  "Send the region between START and END to a chosen contact."
  (interactive "r")
  (let ((contact (gossip--read-contact))
        (text (buffer-substring-no-properties start end)))
    (gossip-send (plist-get contact :id) text)
    (message "gossip: region sent to %s" (plist-get contact :name))))

;;;###autoload
(defun gossip-send-file (file &optional contact)
  "Send FILE to CONTACT (prompted if nil) via the blob transfer channel."
  (interactive "fSend file: ")
  (let ((contact (or contact (gossip--read-contact))))
    (gossip--request 'blob/send
                     (list :to (plist-get contact :id)
                           :path (expand-file-name file)))
    (message "gossip: transferring %s to %s"
             (file-name-nondirectory file) (plist-get contact :name))))

;;;###autoload
(defun gossip-downloads-directory ()
  "Show where received files are saved."
  (interactive)
  (message "gossip: received files are saved to %s"
           (plist-get (gossip--request 'status) :downloads-dir)))

;;;###autoload
(defun gossip-set-downloads-directory (dir)
  "Set DIR as the folder where received files are saved."
  (interactive "DSave received files to: ")
  (message "gossip: received files now saved to %s"
           (plist-get (gossip--request 'config/setDownloadsDir
                                       (list :path (expand-file-name dir)))
                      :downloads-dir)))

(defun gossip--handle-file-declined (params)
  "Note in PARAMS's chat buffer that an offered file was declined."
  (let ((name (plist-get params :name)))
    (gossip--chat-system (plist-get params :from) (format "declined file %s" name))
    (message "gossip: declined file %s from %s" name (plist-get params :from-name))))

(defun gossip--prompt-file (params)
  "Ask whether to accept the offered file described by PARAMS."
  (let ((accept (yes-or-no-p
                 (format "gossip: accept file %s (%s bytes) from %s? "
                         (plist-get params :name)
                         (plist-get params :size)
                         (plist-get params :from-name)))))
    (gossip--request 'file/respond
                     (list :id (plist-get params :id)
                           :accept (if accept t :json-false)))
    (message "gossip: %s %s"
             (if accept "accepted" "declined") (plist-get params :name))))

;;;###autoload
(defun gossip-file-policy ()
  "Show the current default policy for incoming files."
  (interactive)
  (message "gossip: incoming files default=%s"
           (plist-get (plist-get (gossip--request 'status) :files) :default)))

;;;###autoload
(defun gossip-set-file-policy (policy)
  "Set the default POLICY for incoming files: accept, reject, or ask."
  (interactive (list (completing-read "Default for incoming files: "
                                      '("accept" "reject" "ask") nil t)))
  (gossip--request 'config/setFilePolicy (list :default policy))
  (message "gossip: incoming files default=%s" policy))

;;;###autoload
(defun gossip-set-contact-file-policy (contact policy)
  "Set file POLICY (accept, reject, ask, or default) for CONTACT."
  (interactive
   (list (gossip--read-contact)
         (completing-read "Files from this contact: "
                          '("accept" "reject" "ask" "default") nil t)))
  (gossip--request 'config/setContactFilePolicy
                   (list :id (plist-get contact :id) :policy policy))
  (message "gossip: files from %s: %s" (plist-get contact :name) policy))

;;;###autoload
(defun gossip-export-profile (file)
  "Export this profile (identity, contacts, chat history) to FILE."
  (interactive "FExport profile to file: ")
  (message "gossip: exported profile to %s"
           (plist-get (gossip--request 'profile/export
                                       (list :path (expand-file-name file)))
                      :path)))

;;;###autoload
(defun gossip-import-profile (file dir)
  "Import a profile archive FILE into data directory DIR.
DIR must differ from the running profile."
  (interactive "fProfile archive: \nDImport into data directory: ")
  (let ((res (gossip--request 'profile/import
                              (list :path (expand-file-name file)
                                    :data-dir (expand-file-name dir)))))
    (message "gossip: imported %s into %s - set gossip-data-directory there and restart"
             (plist-get res :node-id) (plist-get res :data-dir))))

(defun gossip-show-node-id ()
  "Show and copy this node's id."
  (interactive)
  (let ((id (gossip-node-id)))
    (kill-new id)
    (message "gossip: node id copied: %s" id)))

(defun gossip-set-name (name)
  "Set the display NAME announced to contacts."
  (interactive "sDisplay name: ")
  (gossip--request 'identity/setName (list :name name))
  (setq gossip-display-name name)
  (message "gossip: display name set to %s" name))

(defun gossip-list-contacts ()
  "Echo the contact list with presence."
  (interactive)
  (message "%s"
           (mapconcat (lambda (c)
                        (format "%s %s"
                                (if (eq (plist-get c :online) t) "●" "○")
                                (plist-get c :name)))
                      (gossip-contacts) "  ")))

(defun gossip-net-check ()
  "Probe inbound direct reachability via a connected friend (dial-back)."
  (interactive)
  (let ((result (gossip--request 'net/check)))
    (message "gossip: inbound direct: %s (checked via %s)"
             (if (eq (plist-get result :inbound-direct) t)
                 "reachable"
               "NOT reachable, dial-out-only mode (VPN/NAT), peers sync when you dial them")
             (plist-get result :checked-via))
    result))

(defun gossip-status ()
  "Pop a dashboard buffer with daemon status and the redelivery queue."
  (interactive)
  (let ((status (gossip--request 'status))
        (buffer (get-buffer-create "*gossip status*")))
    (with-current-buffer buffer
      (let ((inhibit-read-only t))
        (erase-buffer)
        (special-mode)
        (insert (format "node      %s\nrelay     %s\ntor       %s\ninbound   %s\n"
                        (plist-get status :node-id)
                        (plist-get status :relay)
                        (plist-get status :tor)
                        (let ((inbound (plist-get status :inbound-direct)))
                          (cond ((eq inbound t) "direct ok")
                                ((eq inbound :json-false)
                                 "dial-out only (VPN/NAT)")
                                (t inbound)))))
        (let ((advertised (plist-get status :advertised-addrs)))
          (unless (seq-empty-p advertised)
            (insert (format "adverts   %s\n"
                            (mapconcat #'identity advertised ", ")))))
        (insert "\ncontacts\n")
        (seq-do (lambda (c)
                  (insert (format "  %s %-10s %s\n"
                                  (if (eq (plist-get c :online) t) "●" "○")
                                  (plist-get c :name)
                                  (plist-get c :id))))
                (plist-get status :contacts))
        (insert "\nredelivery queue (exponential backoff)\n")
        (let ((queue (plist-get status :queue)))
          (if (seq-empty-p queue)
              (insert "  empty\n")
            (seq-do (lambda (q)
                      (insert (format "  %s -> %s  attempt %d, next in %.1fs\n"
                                      (plist-get q :msg-id)
                                      (plist-get q :to-name)
                                      (plist-get q :attempts)
                                      (plist-get q :next-in-seconds))))
                    queue)))))
    (pop-to-buffer buffer)))

;;;###autoload (autoload 'gossip-menu "gossip" nil t)
(transient-define-prefix gossip-menu ()
  "Serverless P2P messaging."
  [["Daemon"
    ("s" "start" gossip-daemon-start)
    ("k" "stop" gossip-daemon-stop)
    ("d" "status dashboard" gossip-status)
    ("p" "probe reachability" gossip-net-check)]
   ["Identity"
    ("i" "show node id" gossip-show-node-id)
    ("n" "set display name" gossip-set-name)
    ("t" "my invite ticket" gossip-my-ticket)]]
  [["Contacts"
    ("a" "add from ticket" gossip-add-contact)
    ("l" "list with presence" gossip-list-contacts)]
   ["Send"
    ("c" "chat buffer" gossip-chat)
    ("r" "send region" gossip-send-region)
    ("f" "send file" gossip-send-file)]])

(provide 'gossip)
;;; gossip.el ends here
