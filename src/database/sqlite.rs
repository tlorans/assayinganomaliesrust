use anyhow::Result;
use rusqlite::{params, Connection, Result as SqliteResult};

/// Represents a connection to the SQLite database.
pub struct SqliteDB {
    pub conn: Connection,
}

impl SqliteDB {
    /// Creates a new SQLite database connection.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the SQLite database file.
    ///
    /// # Returns
    ///
    /// * `Result<SqliteDB>` - Ok containing the `SqliteDB` instance or an error.
    pub fn new(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        Ok(Self { conn })
    }

    /// Executes a SQL query without returning any rows.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query to execute.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Ok if successful, Err otherwise.
    pub fn execute(&self, query: &str) -> SqliteResult<()> {
        self.conn.execute(query, params![])?;
        Ok(())
    }

    /// Executes a SQL query with parameters.
    ///
    /// # Arguments
    ///
    /// * `query` - The SQL query to execute.
    /// * `params` - The parameters for the SQL query.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - Ok if successful, Err otherwise.
    pub fn execture_with_params(
        &self,
        query: &str,
        params: &[&dyn rusqlite::ToSql],
    ) -> SqliteResult<()> {
        self.conn.execute(query, params)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_db() {
        let db = SqliteDB::new(":memory:").unwrap();
        db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO test (name) VALUES ('Alice')")
            .unwrap();
        db.execute("INSERT INTO test (name) VALUES ('Bob')")
            .unwrap();
        let mut stmt = db.conn.prepare("SELECT name FROM test").unwrap();
        let rows = stmt.query_map([], |row| row.get(0)).unwrap();
        let names: Vec<String> = rows.map(|r| r.unwrap()).collect();
        dbg!(&names);
        assert_eq!(names, vec!["Alice", "Bob"]);
    }
}
