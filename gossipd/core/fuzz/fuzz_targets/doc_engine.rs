#![no_main]

use arbitrary::Arbitrary;
use gossipd_core::doc::{apply_ops, Op, SharedDoc, CORRUPT};
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug)]
enum Step {
    Edit { who: bool, pos: u8, del: u8, ins: String },
    Sync { who: bool, cursor: u8 },
    Exchange,
    Garbage(Vec<u8>),
    Reattach(bool),
}

fuzz_target!(|steps: Vec<Step>| {
    let mut docs = [SharedDoc::new(), SharedDoc::new()];
    let mut bufs = [String::new(), String::new()];
    let mut pending: [Vec<Op>; 2] = [vec![], vec![]];
    let mut logs: [Vec<Vec<u8>>; 2] = [vec![], vec![]];
    for (i, d) in docs.iter_mut().enumerate() {
        d.attach(i as u64);
    }

    fn apply(docs: &mut [SharedDoc; 2], logs: &mut [Vec<Vec<u8>>; 2], w: usize, u: &[u8]) -> bool {
        match docs[w].apply_remote(u) {
            Ok(_) => {
                logs[w].push(u.to_vec());
                true
            }
            Err(e) if e.starts_with(CORRUPT) => {
                docs[w] = SharedDoc::load(None, logs[w].iter().map(Vec::as_slice)).unwrap();
                false
            }
            Err(_) => false,
        }
    }
    fn exchange(docs: &mut [SharedDoc; 2], logs: &mut [Vec<Vec<u8>>; 2]) {
        let d01 = docs[0].diff(&docs[1].state_vector()).unwrap();
        let d10 = docs[1].diff(&docs[0].state_vector()).unwrap();
        apply(docs, logs, 1, &d01);
        apply(docs, logs, 0, &d10);
    }
    fn reattach(docs: &mut [SharedDoc; 2], bufs: &[String; 2], pending: &mut [Vec<Op>; 2], w: usize) {
        let text = docs[w].attach(w as u64);
        pending[w].clear();
        if text != bufs[w] {
            pending[w].push(Op(0, text.chars().count(), bufs[w].clone()));
        }
    }

    for st in steps {
        match st {
            Step::Edit { who, pos, del, ins } => {
                let w = who as usize;
                let n = bufs[w].chars().count();
                let pos = (pos as usize).min(n);
                let del = (del as usize).min(n - pos);
                let op = Op(pos, del, ins);
                apply_ops(&mut bufs[w], std::slice::from_ref(&op)).unwrap();
                pending[w].push(op);
            }
            Step::Sync { who, cursor } => {
                let w = who as usize;
                if !docs[w].is_attached(w as u64) {
                    reattach(&mut docs, &bufs, &mut pending, w);
                }
                let ops = std::mem::take(&mut pending[w]);
                let c = (cursor as usize).min(bufs[w].chars().count());
                let out = docs[w].sync(w as u64, &ops, Some(c)).unwrap();
                apply_ops(&mut bufs[w], &out.ops).unwrap();
                assert_eq!(bufs[w].chars().count(), out.size);
                if let Some(u) = &out.update {
                    logs[w].push(u.clone());
                }
                if let Some(cur) = out.cursor {
                    docs[1 - w].set_peer_cursor("peer", "p", &cur).unwrap();
                }
            }
            Step::Exchange => exchange(&mut docs, &mut logs),
            Step::Garbage(bytes) => {
                apply(&mut docs, &mut logs, 0, &bytes);
                let _ = docs[0].diff(&bytes);
                let _ = docs[0].set_peer_cursor("x", "y", &bytes);
                let _ = SharedDoc::load(Some(&bytes), [bytes.as_slice()]);
            }
            Step::Reattach(who) => reattach(&mut docs, &bufs, &mut pending, who as usize),
        }
    }

    for round in 0..2 {
        for w in 0..2 {
            if !docs[w].is_attached(w as u64) {
                reattach(&mut docs, &bufs, &mut pending, w);
            }
            let ops = std::mem::take(&mut pending[w]);
            let out = docs[w].sync(w as u64, &ops, None).unwrap();
            apply_ops(&mut bufs[w], &out.ops).unwrap();
            if let Some(u) = &out.update {
                logs[w].push(u.clone());
            }
        }
        if round == 0 {
            exchange(&mut docs, &mut logs);
        }
    }
    assert_eq!(docs[0].text(), docs[1].text());
    assert_eq!(bufs[0], docs[0].text());
    assert_eq!(bufs[1], docs[1].text());
});
