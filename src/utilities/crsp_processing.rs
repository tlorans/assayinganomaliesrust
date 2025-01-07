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
    // Store the CRSP directory path
    let crsp_dir_path = format!("{}/data/crsp", params.directory);

    // Read the CRSP monthly stock file
    let crsp_msf_path = format!("{}/crsp_msf.parquet", crsp_dir_path);
    let mut crsp_msf = ParquetReader::new(std::fs::File::open(crsp_msf_path).unwrap())
        .finish()
        .unwrap();
    println!(
        "CRSP_MSF file loaded. It contains {} rows and {} columns.",
        crsp_msf.height(),
        crsp_msf.width()
    );

    // Read the CRSP monthly stock file with share code information
    let crsp_mseexch_path = format!("{}/crsp_mseexchdates.parquet", crsp_dir_path);
    let mut crsp_mseexchdates = ParquetReader::new(std::fs::File::open(crsp_mseexch_path).unwrap())
        .finish()
        .unwrap();
    println!(
        "CRSP_MSEEXCHDATES file loaded. It contains {} rows and {} columns.",
        crsp_mseexchdates.height(),
        crsp_mseexchdates.width()
    );

    // Inspect schemas
    println!("{:?}", crsp_msf.schema());
    println!("{:?}", crsp_mseexchdates.schema());


    // Perform the join as LazyFrame
    let result = crsp_msf.clone().lazy().join(
        crsp_mseexchdates.clone().lazy(),
        [(col("permno"))],
        [(col("permno"))],
        JoinArgs::new(JoinType::Left)
    ).collect()?;

    Ok(result)
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
