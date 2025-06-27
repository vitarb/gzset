use gzset::ScoreSet;
use quickcheck::quickcheck;

quickcheck! {
    fn insert_remove_roundtrip(pairs: Vec<(f64, String)>) -> bool {
        let mut set = ScoreSet::default();
        for (s, m) in &pairs {
            set.insert(*s, m.clone());
        }
        for (_, m) in &pairs {
            assert!(set.score(m).is_some());
        }
        true
    }
}
