use anyhow::Result;
use polars::prelude::*;
use std::collections::HashMap;
use std::fs;

/// Struct representing the configuration parameters
#[derive(Debug)]
pub struct Params {
    pub directory: String,
    pub sample_start: u32,
    pub sample_end: u32,
    pub dom_com_eq_flag: bool,
}

pub fn make_crsp_monthly_data(params: &Params) -> Result<DataFrame> {
    let crsp_dir_path = format!("{}/data/crsp", params.directory);

    // file paths for the required parquet files
    let crsp_msf_path = format!("{}/crsp_msf.parquet", crsp_dir_path);

    // Read Parquet files into DataFrames
    let mut crsp_msf = ParquetReader::new(std::fs::File::open(crsp_msf_path).unwrap())
        .finish()
        .unwrap();

    Ok(crsp_msf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_crsp_monthly_data() {
        let params = Params {
            directory: ".".to_string(),
            sample_start: 200001,
            sample_end: 200012,
            dom_com_eq_flag: true,
        };

        let crsp_msf = make_crsp_monthly_data(&params).unwrap();
        dbg!(crsp_msf);
    }
}
