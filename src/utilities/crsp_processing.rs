use anyhow::Result;
use polars::prelude::*;
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

    // Read the CRSP monthly stock file as LazyFrame
    let crsp_msf_path = format!("{}/crsp_msf.parquet", crsp_dir_path);
    let crsp_msf_lazy = LazyFrame::scan_parquet(&crsp_msf_path, Default::default())?;
    println!("Loaded CRSP_MSF as LazyFrame.");

    // Read the CRSP monthly stock file with share code information as LazyFrame
    let crsp_mseexch_path = format!("{}/crsp_mseexchdates.parquet", crsp_dir_path);
    let crsp_mseexchdates_lazy = LazyFrame::scan_parquet(&crsp_mseexch_path, Default::default())?;
    println!("Loaded CRSP_MSEEXCHDATES as LazyFrame.");

    // // Inspect schemas
    // println!("{:?}", crsp_msf_lazy.schema()?);
    // println!("{:?}", crsp_mseexchdates_lazy.schema()?);

    // Perform the join as LazyFrame
    let result = crsp_msf_lazy
        .join(
            crsp_mseexchdates_lazy,
            [col("permno")], // Left key
            [col("permno")], // Right key
            JoinArgs::new(JoinType::Left),
        )
        // // Filter rows based on sample_start and sample_end parameters
        // .filter(
        //     col("date")
        //         .gt(lit(params.sample_start))
        //         .and(col("date").lt(lit(params.sample_end))),
        // )
        .collect()?; // Collect into a DataFrame

    println!("Join and filtering complete.");

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
