use ryu::Buffer;
use std::cell::RefCell;

#[inline]
pub fn fmt_f64(buf: &mut Buffer, score: f64) -> &str {
    debug_assert!(score.is_finite());
    let formatted = buf.format_finite(score);
    formatted.strip_suffix(".0").unwrap_or(formatted)
}

thread_local! {
    static FMT_BUF: RefCell<Buffer> = RefCell::new(Buffer::new());
}

#[inline]
pub fn with_fmt_buf<F, R>(f: F) -> R
where
    F: FnOnce(&mut Buffer) -> R,
{
    FMT_BUF.with(|b| f(&mut b.borrow_mut()))
}
