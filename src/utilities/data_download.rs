use anyhow::anyhow;
use anyhow::Result;
use dotenv::dotenv;
use log::info;
use native_tls::TlsConnector;
use polars::prelude::*;
use postgres_native_tls::MakeTlsConnector;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::File;
use tokio_postgres::Client;
use tokio_postgres::Row;

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
/// Downloads a table from the WRDS PostgreSQL database and saves it to disk in the specified format.
///
/// # Arguments
/// * `client` - A reference to the PostgreSQL client.
/// * `libname` - WRDS library name (e.g., "CRSP").
/// * `memname` - WRDS table name (e.g., "MSF").
/// * `dir_path` - Directory path to save the downloaded table.
/// * `custom_query` - Optional custom SQL query to execute.
/// * `output_format` - Output format for the saved table ("csv" or "parquet").
///
/// # Returns
/// * `Result<()>` - Ok if the table was successfully downloaded and saved, or an error.
///
/// # Example
/// ```rust
/// use anyhow::Result;
/// use tokio_postgres::Client;
/// use wrds::utilities::data_download::get_wrds_table;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///    let config = WrdsConfig::from_env();
///   let client = establish_connection(&config).await?;
///  get_wrds_table(&client, "CRSP", "MSF", "data/crsp", None, "parquet").await.unwrap();
/// Ok(())
/// }
/// ```
///
pub async fn get_wrds_table(
    client: &Client,
    libname: &str,
    memname: &str,
    dir_path: &str,
    custom_query: Option<&str>,
    output_format: &str,
) -> Result<()> {
    fs::create_dir_all(dir_path).expect("Failed to create directory");

    // Construct table name and SQL query
    let table_name = format!("{}.{}", libname, memname);
    let query = if let Some(custom_query) = custom_query {
        custom_query.to_string() // Convert to owned `String` if custom query is provided
    } else {
        format!("SELECT * FROM {}", table_name) // Format a new query string
    };

    // Execute query
    let rows = client.query(query.as_str(), &[]).await?;
    if rows.is_empty() {
        return Err(anyhow!("No data found for table: {}", table_name));
    }

    // Prepare DataFrame columns dynamically
    let mut columns: Vec<Column> = vec![];
    let schema = rows[0].columns();

    for (idx, column) in schema.iter().enumerate() {
        let col_name: PlSmallStr = column.name().into(); // Convert to `PlSmallStr`

        let data_type = column.type_();
        let current_series = match data_type.name() {
            "numeric" => {
                let col_data: Vec<Option<f64>> = numeric_column_to_f64(&rows, idx);
                Column::new(col_name.clone(), Series::new(col_name, col_data))
            }
            // if date, convert to Vec<chrono>
            "date" => {
                let col_data: Vec<Option<chrono::NaiveDate>> =
                    rows.iter().map(|row| row.get(idx)).collect();
                Column::new(col_name.clone(), Series::new(col_name, col_data))
            }
            "int2" => {
                let col_data: Vec<Option<i16>> = rows.iter().map(|row| row.get(idx)).collect();
                Column::new(col_name.clone(), Series::new(col_name, col_data))
            }
            "int4" => {
                let col_data: Vec<Option<i32>> = rows.iter().map(|row| row.get(idx)).collect();
                Column::new(col_name.clone(), Series::new(col_name, col_data))
            }
            "float8" => {
                let col_data: Vec<Option<f64>> = rows.iter().map(|row| row.get(idx)).collect();
                Column::new(col_name.clone(), Series::new(col_name, col_data))
            }
            "text" | "varchar" => {
                let col_data: Vec<Option<&str>> = rows.iter().map(|row| row.get(idx)).collect();
                Column::new(col_name.clone(), Series::new(col_name, col_data))
            }
            "bool" => {
                let col_data: Vec<Option<bool>> = rows.iter().map(|row| row.get(idx)).collect();
                Column::new(col_name.clone(), Series::new(col_name, col_data))
            }
            _ => {
                // For unsupported types, store as strings for now
                let col_data: Vec<Option<String>> = rows
                    .iter()
                    .map(|row| row.get::<_, Option<String>>(idx))
                    .collect();
                Column::new(col_name.clone(), Series::new(col_name, col_data))
            }
        };
        columns.push(current_series);
    }

    // Build DataFrame
    let mut df = DataFrame::new(columns)?;

    // Save DataFrame to desired format
    let output_file = format!(
        "{}/{}_{}.{}",
        dir_path,
        libname.to_lowercase(),
        memname.to_lowercase(),
        output_format
    );
    match output_format {
        "csv" => {
            let mut file = std::fs::File::create(&output_file)?;
            CsvWriter::new(&mut file).finish(&mut df)?;
        }
        "parquet" => {
            let mut file = std::fs::File::create(&output_file)?;
            ParquetWriter::new(&mut file).finish(&mut df)?;
        }
        _ => return Err(anyhow!("Unsupported output format: {}", output_format)),
    }
    info!("Saved table {} to {}", table_name, output_file);
    Ok(())
}

pub async fn get_crsp_data(client: &Client, dir_path: &str, output_format: &str) -> Result<()> {
    // Download required tables
    let tables = [
        ("CRSP", "MSFHDR"),    //
        ("CRSP", "MSF"),       // Main dataset
        ("CRSP", "MSEDELIST"), // delisting returns
        ("CRSP", "MSEEXCHDATES"),
        ("CRSP", "CCMXPF_LNKHIST"),
        ("CRSP", "STOCKNAMES"),
    ];

    // Specify output directory and format
    for (libname, memname) in &tables {
        get_wrds_table(&client, libname, memname, dir_path, None, output_format)
            .await
            .unwrap();
    }
    Ok(())
}

/// Converts a PostgreSQL `numeric` column into a `Vec<Option<f64>>` for compatibility with Polars.
fn numeric_column_to_f64(rows: &[Row], column_idx: usize) -> Vec<Option<f64>> {
    rows.iter()
        .map(|row| {
            // Attempt to retrieve the value as a `Decimal`
            let decimal: Option<Decimal> = row.get(column_idx);

            // Convert `Decimal` to `f64`
            decimal.and_then(|d| d.to_f64())
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_get_wrds_table() {
        let config = WrdsConfig::from_env();

        // Download required tables
        let tables = [
            // ("CRSP", "MSFHDR"), //
            ("CRSP", "MSF"), // Main dataset
                             // ("CRSP", "MSEDELIST"), // delisting returns
                             // ("CRSP", "MSEEXCHDATES"),
                             // ("CRSP", "CCMXPF_LNKHIST"),
                             // ("CRSP", "STOCKNAMES"),
        ];

        let client = establish_connection(&config).await.unwrap();
        // Specify output directory and format
        let dir_path = "data/crsp";
        let output_format = "parquet"; // or "csv"
        for (libname, memname) in &tables {
            get_wrds_table(&client, libname, memname, dir_path, None, output_format)
                .await
                .unwrap();

            // Read the parquet file
            let output_file = format!(
                "{}/{}_{}.{}",
                dir_path,
                libname.to_lowercase(),
                memname.to_lowercase(),
                output_format
            );
            let mut read_file = std::fs::File::open(output_file).unwrap();
            let read_df = ParquetReader::new(&mut read_file).finish().unwrap();
            dbg!(&read_df);
        }
    }

    #[tokio::test]
    async fn test_get_crsp_data() {
        let config = WrdsConfig::from_env();
        let client = establish_connection(&config).await.unwrap();

        // Specify output directory and format
        let dir_path = "data/crsp";
        let output_format = "parquet"; // or "csv"
        get_crsp_data(&client, dir_path, output_format)
            .await
            .unwrap();
    }
}
