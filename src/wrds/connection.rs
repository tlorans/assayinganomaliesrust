use dotenv::dotenv;
use std::env;

#[derive(Debug)]
pub struct WrdsConfig {
    pub user: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub dbname: String,
}

impl WrdsConfig {
    pub fn from_env() -> Self {
        dotenv().ok();
        WrdsConfig {
            user: env::var("WRDS_USER").expect("WRDS_USER must be set"),
            password: env::var("WRDS_PASSWORD").expect("WRDS_PASSWORD must be set"),
            host: env::var("WRDS_HOST")
                .unwrap_or_else(|_| "wrds-pgdata.wharton.upenn.edu".to_string()),
            port: env::var("WRDS_PORT")
                .unwrap_or_else(|_| "9737".to_string())
                .parse()
                .expect("WRDS_PORT must be a number"),
            dbname: env::var("WRDS_DBNAME").unwrap_or_else(|_| "wrds".to_string()),
        }
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.dbname
        )
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn test_wrds_config() {
//         let config = WrdsConfig::from_env();
//         dbg!(&config);
//     }
// }
