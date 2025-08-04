use gzset::StringPool;

#[test]
fn pool_roundtrip_dedup() {
    let mut pool = StringPool::default();
    let a = pool.intern("foo");
    let b = pool.intern("foo");
    assert_eq!(a, b);
    assert_eq!(pool.get(a), "foo");
}
