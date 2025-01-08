use anyhow::Result;
use chrono::NaiveDate;
use pivot::pivot;
// Use chrono for date handling
use polars::prelude::*;
use std::fs;
use std::ops::BitAnd; // Required for custom logical AND
                      // ndarrays
use ndarray::Array2;
use ndarray::{ArrayBase, Ix2}; // Import dimensionality types
use polars_ops::prelude::*;
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

pub fn make_crsp_monthly_data(params: &Params) -> Result<()> {
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
    let mut result = crsp_msf_lazy
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

    // Check to see if we should only keep share codes 10 and 11 (domestic common equity)
    if params.dom_com_eq_flag {
        // Filter the DataFrame to only keep share codes 10 and 11
        result = result
            .clone()
            .lazy()
            .filter(
                col("shrcd").eq(lit(10)).or(col("shrcd").eq(lit(11))), // The logic ensures that only rows with share codes 10 and 11 are retained.
            )
            .collect()?;

        println!("Filtered out non-domestic common equity.");
    }

    // print the schema
    println!("Schema of the joined DataFrame:");
    println!("{:?}", result.schema());

    println!("Join and filtering complete.");

    // Save permno and dates vectors
    let permno: Array2<i32> = result
        .clone()
        .lazy()
        .select([col("permno").unique_stable()])
        .collect()?
        .to_ndarray::<Int32Type>(Default::default())
        .unwrap();

    // Serialize the ndarray to JSON
    let json = serde_json::to_string(&permno).unwrap();
    // Write the JSON to a file
    let mut file = File::create(format!("{}/permno.json", crsp_dir_path)).unwrap();
    file.write_all(json.as_bytes()).unwrap();

    let dates_col = result
        .clone()
        .lazy()
        .select([col("date").dt().to_string("%Y%m%d").unique_stable()])
        .collect()?;

    let dates: Array2<i32> = dates_col
        .clone()
        .lazy()
        .select([col("date").cast(DataType::Int32)])
        .collect()?
        .to_ndarray::<Int32Type>(Default::default())
        .unwrap();

    // Serialize the ndarray to JSON
    let json = serde_json::to_string(&dates).unwrap();
    // Write the JSON to a file
    let mut file = File::create(format!("{}/dates.json", crsp_dir_path)).unwrap();
    file.write_all(json.as_bytes()).unwrap();

    // List of variables to extract
    let var_names = vec![
        "shrcd", "exchcd", "siccd", "prc", "bid", "ask", "bidlo", "askhi",
        // "vol_x_adj",
        // "ret_x_dl",
        "shrout", "cfacpr", "cfacshr", "spread", "retx",
    ];

    // Iterate through the variable names
    for (i, var_name) in var_names.iter().enumerate() {
        println!(
            "Now working on variable {} ({} out of {}).",
            var_name,
            i + 1,
            var_names.len()
        );

        // Select relevant columns (permno, dates, variable)
        let temp_df = result
            .clone()
            .lazy()
            .select([col("permno"), col("date"), col(var_name.to_string())])
            .collect()?;

        // Check the data type of the current column
        let column_type = temp_df.schema().get_field(var_name).unwrap();

        // Pivot the DataFrame to create a matrix (permno as rows, dates as columns)
        let mut matrix_df = pivot(
            &temp_df,
            ["date"],
            Some(["permno"]),
            Some([var_name.to_string()]),
            false,
            None,
            None,
        )?
        .fill_null(FillNullStrategy::Zero)?;

        // Drop permno column
        matrix_df.drop_in_place("permno")?;

        match column_type.dtype {
            DataType::Int16 => {
                let ndarray: Array2<i16> = matrix_df.to_ndarray::<Int16Type>(Default::default())?;
                save_ndarray_as_json(ndarray, &crsp_dir_path, var_name)?;
            }
            DataType::Int32 => {
                let ndarray: Array2<i32> = matrix_df.to_ndarray::<Int32Type>(Default::default())?;
                save_ndarray_as_json(ndarray, &crsp_dir_path, var_name)?;
            }
            DataType::Int64 => {
                let ndarray: Array2<i64> = matrix_df.to_ndarray::<Int64Type>(Default::default())?;
                save_ndarray_as_json(ndarray, &crsp_dir_path, var_name)?;
            }
            DataType::Float32 => {
                let ndarray: Array2<f32> =
                    matrix_df.to_ndarray::<Float32Type>(Default::default())?;
                save_ndarray_as_json(ndarray, &crsp_dir_path, var_name)?;
            }
            DataType::Float64 => {
                let ndarray: Array2<f64> =
                    matrix_df.to_ndarray::<Float64Type>(Default::default())?;
                save_ndarray_as_json(ndarray, &crsp_dir_path, var_name)?;
            }
            _ => {
                println!("Unsupported data type.");
            }
        }
    }

    Ok(())
}

fn save_ndarray_as_json<T: serde::Serialize>(
    ndarray: Array2<T>,
    crsp_dir_path: &str,
    var_name: &str,
) -> Result<()> {
    let json = serde_json::to_string(&ndarray)?;
    let file_path = format!("{}/{}.json", crsp_dir_path, var_name);
    let mut file = File::create(file_path)?;
    file.write_all(json.as_bytes())?;
    println!("Saved matrix for {}.", var_name);
    Ok(())
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

        make_crsp_monthly_data(&params).unwrap();
        // Read the JSON from the file
        let crsp_dir_path = format!("{}/data/crsp", params.directory);
        let mut file = File::open(format!("{}/shrcd.json", crsp_dir_path)).unwrap();
        let mut json = String::new();
        file.read_to_string(&mut json).unwrap();
        // Deserialize the JSON into an ndarray
        let deserialized_array: Array2<i16> = serde_json::from_str(&json).unwrap();

        dbg!(deserialized_array);
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

    #[test]
    fn test_date_to_timestamp() {
        // Create a DataFrame with a NaiveDate column
        let df = df![
            "date" => vec![
                NaiveDate::from_ymd_opt(2021, 1, 1),
                NaiveDate::from_ymd_opt(2021, 1, 2),
                NaiveDate::from_ymd_opt(2021, 1, 3),
            ]
        ]
        .unwrap();

        // Convert the NaiveDate column to a Timestamp column
        // Example NaiveDate
        let date = NaiveDate::from_ymd_opt(2020, 10, 12).unwrap();

        // Convert to numeric format YYYYMMDD
        let numeric_date = date.format("%Y%m%d").to_string().parse::<i32>().unwrap();

        println!("Numeric date: {}", numeric_date);

        // Convert the NaiveDate column to numeric format (YYYYMMDD)
        let df_string = df
            .clone()
            .lazy()
            .select([col("date").dt().to_string("%Y%m%d")])
            .collect()
            .unwrap();

        let df_numeric = df_string
            .clone()
            .lazy()
            .select([col("date").cast(DataType::Int32)])
            .collect()
            .unwrap();

        println!("{:?}", df_numeric)
    }
}
