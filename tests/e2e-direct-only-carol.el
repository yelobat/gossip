;;; e2e-direct-only-carol.el --- hard-NAT peer: outbound fails, inbound flushes  -*- lexical-binding: t -*-
(load (expand-file-name "tests/harness.el" default-directory))
(gossip-daemon-start)
(let ((status (jsonrpc-request gossip--connection 'status nil)))
  (gossip-test-assert (string-prefix-p "disabled" (plist-get status :relay))
                      "relay policy should be direct-only, got %S" (plist-get status :relay)))
(gossip-send "gsp1-carol-9e1f" "can you hear me carol?")
(gossip-test-pump 12.0)
(gossip-test-assert (>= (gossip-test-count 'queue
                                           (lambda (p) (equal (plist-get p :to-name) "carol")))
                        5)
                    "expected >=5 failed dial attempts to carol")
(gossip-test-assert (gossip-test-find 'presence
                                      (lambda (p) (equal (plist-get p :path) "direct-inbound")))
                    "carol never connected inbound")
(gossip-test-assert (gossip-test-find 'received
                                      (lambda (p) (equal (plist-get p :from-name) "carol")))
                    "no reply from carol after inbound connect")
(message "PASS e2e-direct-only-carol")
(gossip-daemon-stop)
