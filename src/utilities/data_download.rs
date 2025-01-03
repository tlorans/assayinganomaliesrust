use crate::database::sqlite::SqliteDB;
use crate::wrds::connection::WrdsConfig;
use crate::wrds::queries::establish_connection;
use anyhow::{anyhow, Result};
use indicatif::{ProgressBar, ProgressStyle};
use log::{error, info};
use polars::prelude::*;
use std::collections::HashMap;
use tokio_postgres::{Client, Row};

pub async fn download_crsp_monthly(
    config: &WrdsConfig,
    db: &SqliteDB,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<Row>> {
    let client = establish_connection(config).await?;
    let query = format!(
        "SELECT msf.permno, date_trunc('month', msf.mthcaldt)::date AS date, \
        msf.mthret AS ret, msf.shrout, msf.mthprc AS altprc, \
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
    Ok(rows)

    //     // Convert rows to Polars DataFrame
    //     let mut permno: Vec<i32> = Vec::with_capacity(rows.len());
    //     let mut date: Vec<chrono::NaiveDate> = Vec::with_capacity(rows.len());
    //     let mut ret: Vec<f64> = Vec::with_capacity(rows.len());
    //     let mut shrout: Vec<f64> = Vec::with_capacity(rows.len());
    //     let mut altprc: Vec<f64> = Vec::with_capacity(rows.len());
    //     let mut primaryexch: Vec<String> = Vec::with_capacity(rows.len());
    //     let mut siccd: Vec<i32> = Vec::with_capacity(rows.len());

    //     for row in rows {
    //         permno.push(row.get("permno"));
    //         date.push(row.get("date"));
    //         ret.push(row.get("ret"));
    //         shrout.push(row.get("shrout"));
    //         altprc.push(row.get("altprc"));
    //         primaryexch.push(row.get("primaryexch"));
    //         siccd.push(row.get("siccd"));
    //     }

    //     let df = df![
    //         "permno" => permno,
    //         "date" => date,
    //         "ret" => ret,
    //         "shrout" => shrout,
    //         "altprc" => altprc,
    //         "primaryexch" => primaryexch,
    //         "siccd" => siccd,
    //     ]?;

    //     // Data processing steps (e.g., calculating mktcap)
    //     let mut df = df
    //         .lazy()
    //         .with_column((col("shrout") * col("altprc")).alias("mktcap"))
    //         .with_column(
    //             col("mktcap")
    //                 .divide(lit(1_000_000))
    //                 .alias("mktcap_millions"),
    //         )
    //         .with_column(
    //             when(col("mktcap_millions").eq(lit(0.0)))
    //                 .then(lit(f64::NAN))
    //                 .otherwise(col("mktcap_millions"))
    //                 .alias("mktcap_clean"),
    //         )
    //         .collect()?;

    //     // Additional transformations (exchange mapping, industry mapping)
    //     df = map_primary_exchange(df)?;
    //     df = map_industry(df)?;

    //     // Store DataFrame into SQLite
    //     store_dataframe_to_sqlite(&df, db, "crsp_monthly").await?;

    //     Ok(())
    // }

    // fn map_primary_exchange(mut df: DataFrame) -> Result<DataFrame> {
    //     let exchange_map: HashMap<&str, &str> =
    //         HashMap::from([("N", "NYSE"), ("A", "AMEX"), ("Q", "NASDAQ")]);

    //     let exchange: Vec<String> = df
    //         .column("primaryexch")?
    //         .utf8()?
    //         .into_iter()
    //         .map(|opt| {
    //             opt.and_then(|exch| exchange_map.get(exch).map(|&v| v.to_string()))
    //                 .unwrap_or_else(|| "Other".to_string())
    //         })
    //         .collect();

    //     df.with_column(Series::new("exchange", &exchange))?;
    //     Ok(df)
    // }

    // fn map_industry(mut df: DataFrame) -> Result<DataFrame> {
    //     let industry_map = |siccd: i32| -> &str {
    //         match siccd {
    //             1..=999 => "Agriculture",
    //             1000..=1499 => "Mining",
    //             1500..=1799 => "Construction",
    //             2000..=3999 => "Manufacturing",
    //             4000..=4899 => "Transportation",
    //             4900..=4999 => "Utilities",
    //             5000..=5199 => "Wholesale",
    //             5200..=5999 => "Retail",
    //             6000..=6799 => "Finance",
    //             7000..=8999 => "Services",
    //             9000..=9999 => "Public",
    //             _ => "Missing",
    //         }
    //     };

    //     let industry: Vec<&str> = df
    //         .column("siccd")?
    //         .i32()?
    //         .into_iter()
    //         .map(|opt| opt.map(industry_map).unwrap_or("Missing"))
    //         .collect();

    //     df.with_column(Series::new("industry", &industry))?;
    //     Ok(df)
    // }

    // async fn store_dataframe_to_sqlite(df: &DataFrame, db: &SqliteDB, table_name: &str) -> Result<()> {
    //     // Convert Polars DataFrame to CSV in-memory
    //     let csv_data = df.to_csv()?;
    //     let mut stmt = db.conn.prepare(&format!("CREATE TABLE IF NOT EXISTS {} (permno INTEGER, date TEXT, ret REAL, shrout REAL, altprc REAL, primaryexch TEXT, siccd INTEGER, mktcap REAL, exchange TEXT, industry TEXT)", table_name))?;
    //     stmt.execute([])?;

    //     // Insert data
    //     for row in df.iter_rows() {
    //         db.conn.execute(
    //             &format!(
    //                 "INSERT INTO {} (permno, date, ret, shrout, altprc, primaryexch, siccd, mktcap, exchange, industry) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
    //                 table_name
    //             ),
    //             row.as_slice(),
    //         )?;
    //     }

    //     Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_download_crsp_monthly() {
        let config = WrdsConfig::from_env();
        let db = SqliteDB::new(":memory:").unwrap();
        let start_date = "2020-01-01";
        let end_date = "2020-01-31";
        let rows = download_crsp_monthly(&config, &db, start_date, end_date)
            .await
            .unwrap();
        dbg!(&rows);
    }
}
