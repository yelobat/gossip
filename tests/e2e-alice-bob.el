;;; e2e-alice-bob.el --- online delivery + offline peer backoff  -*- lexical-binding: t -*-
(load (expand-file-name "tests/harness.el" default-directory))
(gossip-daemon-start)
(gossip-test-assert (equal (mapcar (lambda (c) (plist-get c :name)) (gossip-contacts))
                           '("alice" "bob" "carol" "dave"))
                    "unexpected contact roster")
(gossip-send "gsp1-alice-77aa" "hello alice")
(gossip-send "gsp1-bob-42dd" "hello bob, wake up")
(gossip-test-pump 8.0)
(gossip-test-assert (>= (gossip-test-count 'queue (lambda (_) t)) 3)
                    "expected backoff queue/update events for bob")
(gossip-test-assert (gossip-test-find 'delivered
                                      (lambda (p) (equal (plist-get p :to) "gsp1-alice-77aa")))
                    "no delivery ack from alice")
(gossip-test-assert (gossip-test-find 'received
                                      (lambda (p) (equal (plist-get p :from-name) "bob")))
                    "no reply from bob after he came back")
(message "PASS e2e-alice-bob")
(gossip-daemon-stop)
