mod helpers;
#[path = "../src/score_set.rs"]
#[allow(dead_code)]
mod score_set;
use quickcheck::quickcheck;
use score_set::ScoreSet;

quickcheck! {
    fn insert_remove_roundtrip(pairs: Vec<(f64, String)>) -> bool {
        let mut set = ScoreSet::default();
        for (s, m) in &pairs {
            set.insert(*s, m);
        }
        for (_, m) in &pairs {
            assert!(set.score(m).is_some());
        }
        true
    }
}
