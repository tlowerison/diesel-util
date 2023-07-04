pub fn pagination_max_count() -> &'static Option<u32> {
    use std::sync::OnceLock;
    static PAGINATION_MAX_COUNT: OnceLock<Option<u32>> = OnceLock::new();
    PAGINATION_MAX_COUNT.get_or_init(|| {
        std::env::var("PAGINATION_MAX_COUNT").ok().map(|count| {
            count
                .parse::<u32>()
                .expect("PAGINATION_MAX_COUNT environment variable must be a semi-positive integer")
        })
    })
}
