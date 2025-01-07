use anyhow::Result;
use chrono::NaiveDate; // Use chrono for date handling
use polars::prelude::*;
use std::fs;
use std::ops::BitAnd; // Required for custom logical AND
                      // ndarrays
use ndarray::Array2;
use ndarray::{ArrayBase, Ix2}; // Import dimensionality types
use std::fs::File;
use std::io::{Read, Write};
/// Struct representing the configuration parameters
#[derive(Debug)]
pub struct Params {
    pub directory: String,
    pub sample_start: NaiveDate,
    pub sample_end: NaiveDate,
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

    // Perform the join as LazyFrame
    let result = crsp_msf_lazy
        .join(
            crsp_mseexchdates_lazy,
            [col("permno")], // Left key
            [col("permno")], // Right key
            JoinArgs::new(JoinType::Left),
        )
        .filter(
            col("date")
                .gt(col("namedt"))
                .and(col("date").lt(col("nameendt"))), // The logic ensures that only rows where date is within the valid range [namedt, nameendt] are retained.
        )
        .filter(
            col("date")
                .gt_eq(lit(params.sample_start))
                .and(col("date").lt_eq(lit(params.sample_end))), // The logic ensures that only rows where date is within the sample range are retained.
        )
        .collect()?; // Collect into a DataFrame

    // print the schema
    println!("Schema of the joined DataFrame:");
    println!("{:?}", result.schema());

    println!("Join and filtering complete.");

    // Save permno and dates vectors
    let permno = result
        .clone()
        .lazy()
        .select([col("permno").unique_stable()])
        .collect()?;

    // transform permno to a Vec<u32> and save it as a parquet file
    let permno_path = format!("{}/permno.parquet", crsp_dir_path);
    let permno_u32: ArrayBase<_, Ix2> = permno
        .to_ndarray::<UInt32Type>(IndexOrder::default())
        .unwrap();

    dbg!(permno_u32);
    let dates = result
        .clone()
        .lazy()
        .select([col("date").unique_stable()])
        .collect()?;

    // print shape of dates
    println!("Shape of dates: {:?}", dates.shape());
    // print the first 5 rows of dates
    println!("First 5 rows of dates:");
    println!("{:?}", dates.head(Some(5)));

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_crsp_monthly_data() {
        let params = Params {
            directory: ".".to_string(),
            sample_start: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            sample_end: NaiveDate::from_ymd_opt(2001, 12, 31).unwrap(),
            dom_com_eq_flag: true,
        };

        let crsp_msf = make_crsp_monthly_data(&params).unwrap();
        dbg!(crsp_msf);
    }

    #[test]
    fn test_write_and_read() {
        // Create a 2D array
        let array: Array2<f64> =
            Array2::from_shape_vec((2, 3), vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]).unwrap();

        // Serialize the ndarray to JSON
        let json = serde_json::to_string(&array).unwrap();

        // Write the JSON to a file
        let mut file = File::create("array.json").unwrap();
        file.write_all(json.as_bytes()).unwrap();

        // Read the JSON from the file
        let mut file = File::open("array.json").unwrap();
        let mut json = String::new();
        file.read_to_string(&mut json).unwrap();

        // Deserialize the JSON into an ndarray
        let deserialized_array: Array2<f64> = serde_json::from_str(&json).unwrap();

        // Check that the original and deserialized arrays are equal
        dbg!(deserialized_array);
    }
}
