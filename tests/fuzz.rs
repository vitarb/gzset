mod helpers;
use gzset::ScoreSet;
use quickcheck::quickcheck;

quickcheck! {
    fn insert_remove_roundtrip(pairs: Vec<(f64, String)>) -> bool {
        let mut set = ScoreSet::default();
        for (s, m) in &pairs {
            if !s.is_finite() {
                continue;
            }
            set.insert(*s, m);
        }
        for (s, m) in &pairs {
            if !s.is_finite() {
                continue;
            }
            assert!(set.score(m).is_some());
        }
        true
    }
}
