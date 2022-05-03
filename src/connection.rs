use crate::driver::Driver;

struct Connection {
    dsn: String,
    driver: Option<Driver>,
}
