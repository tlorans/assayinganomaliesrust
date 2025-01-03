use crate::wrds::connection::WrdsConfig;
use anyhow::Result;
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::Client;

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
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_establish_connection() {
        let config = WrdsConfig::from_env();
        let client = establish_connection(&config).await.unwrap();
        // Check that the client is connected
        dbg!(&client);
    }
}
