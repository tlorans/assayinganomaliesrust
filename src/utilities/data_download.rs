use anyhow::Result;
use dotenv::dotenv;
use log::info;
use native_tls::TlsConnector;
use polars::prelude::*;
use postgres_native_tls::MakeTlsConnector;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use tokio_postgres::Client;

pub async fn download_crsp_monthly(
    config: &WrdsConfig,
    start_date: &str,
    end_date: &str,
) -> Result<DataFrame> {
    let client = establish_connection(config).await?;
    let query = format!(
        "SELECT msf.permno, date_trunc('month', msf.mthcaldt)::date AS date, \
    msf.mthret::double precision AS ret, \
    msf.shrout::double precision AS shrout, \
    msf.mthprc::double precision AS altprc, \
    ssih.primaryexch, ssih.siccd \
    FROM crsp.msf_v2 AS msf \
    INNER JOIN crsp.stksecurityinfohist AS ssih \
    ON msf.permno = ssih.permno AND \
       ssih.secinfostartdt <= msf.mthcaldt AND \
       msf.mthcaldt <= ssih.secinfoenddt \
    WHERE msf.mthcaldt BETWEEN '{}' AND '{}' \
          AND ssih.sharetype = 'NS' \
          AND ssih.securitytype = 'EQTY' \
          AND ssih.securitysubtype = 'COM' \
          AND ssih.usincflg = 'Y' \
          AND ssih.issuertype in ('ACOR', 'CORP') \
          AND ssih.primaryexch in ('N', 'A', 'Q') \
          AND ssih.conditionaltype in ('RW', 'NW') \
          AND ssih.tradingstatusflg = 'A'",
        start_date, end_date
    );
    info!("Downloading CRSP monthly data");
    let rows = client.query(query.as_str(), &[]).await?;
    info!("Fetched {} rows from CRSP monthly data", rows.len());
    // Ok(rows)

    let mut permno: Vec<i32> = Vec::with_capacity(rows.len());
    let mut date: Vec<chrono::NaiveDate> = Vec::with_capacity(rows.len());
    let mut ret: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut shrout: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut altprc: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut primaryexch: Vec<Option<String>> = Vec::with_capacity(rows.len());
    let mut siccd: Vec<Option<i32>> = Vec::with_capacity(rows.len());

    for row in rows {
        permno.push(row.get("permno"));
        date.push(row.get("date"));
        ret.push(row.get("ret"));
        shrout.push(row.get("shrout"));
        altprc.push(row.get("altprc"));
        primaryexch.push(row.get("primaryexch"));
        siccd.push(row.get("siccd"));
    }

    let mut df: DataFrame = df![
        "permno" => permno,
        "date" => date,
        "ret" => ret,
        "shrout" => shrout,
        "altprc" => altprc,
        "primaryexch" => primaryexch,
        "siccd" => siccd,
    ]?;

    df = df
        .lazy()
        .with_column(
            when((col("shrout") * col("altprc") / lit(1_000_000)).eq(lit(0.0)))
                .then(lit(f64::NAN))
                .otherwise(col("shrout") * col("altprc") / lit(1_000_000))
                .alias("mktcap"),
        )
        .collect()?;

    // Additional transformations (exchange mapping, industry mapping)
    df = map_industry(df)?;

    Ok(df)
}

fn map_industry(mut df: DataFrame) -> Result<DataFrame> {
    let industry_map = |siccd: i32| -> &str {
        match siccd {
            1..=999 => "Agriculture",
            1000..=1499 => "Mining",
            1500..=1799 => "Construction",
            2000..=3999 => "Manufacturing",
            4000..=4899 => "Transportation",
            4900..=4999 => "Utilities",
            5000..=5199 => "Wholesale",
            5200..=5999 => "Retail",
            6000..=6799 => "Finance",
            7000..=8999 => "Services",
            9000..=9999 => "Public",
            _ => "Missing",
        }
    };

    let industry: Vec<&str> = df
        .column("siccd")?
        .i32()?
        .into_iter()
        .map(|opt| opt.map(industry_map).unwrap_or("Missing"))
        .collect();

    df.with_column(Series::new("industry".into(), &industry))?;
    Ok(df)
}

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

/// Establishes a connection to the WRDS PostgreSQL database using the provided configuration.
/// Utilizes SSL/TLS for secure communication.
///
/// # Arguments
///
/// * `config` - A reference to `WrdsConfig` containing connection details.
///
/// # Returns
///
/// * `Result<Client>` - Ok containing the PostgreSQL client or an error.
pub async fn establish_connection(config: &WrdsConfig) -> Result<Client> {
    // Create a TLS connector
    let native_tls_connector = TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()?;
    let tls_connector = MakeTlsConnector::new(native_tls_connector);

    let connection_string = config.connection_string();
    let (client, connection) = tokio_postgres::connect(&connection_string, tls_connector).await?;

    // Spawn the connection to run in the background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_download_crsp_monthly() {
        let config = WrdsConfig::from_env();
        let start_date = "2020-01-01";
        let end_date = "2020-01-31";
        let mut df: DataFrame = download_crsp_monthly(&config, start_date, end_date)
            .await
            .unwrap();

        let mut file = std::fs::File::create("test.parquet").unwrap();
        ParquetWriter::new(&mut file).finish(&mut df).unwrap();

        // Read the parquet file
        let mut readFile = std::fs::File::open("test.parquet").unwrap();
        let read_df = ParquetReader::new(&mut readFile).finish().unwrap();

        dbg!(&read_df);
    }
}
