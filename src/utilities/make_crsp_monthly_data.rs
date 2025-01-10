use anyhow::{Context, Result};
use chrono::NaiveDate;
use pivot::pivot;
// Use chrono for date handling
use polars::prelude::*;
use std::fs;
use std::ops::BitAnd; // Required for custom logical AND
                      // ndarrays
use ndarray::{Array2, Data};
use ndarray::{ArrayBase, Ix2}; // Import dimensionality types
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

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
    let crsp_dir_path = Path::new(&params.directory).join("data/crsp");

    // Read the CRSP monthly stock file as LazyFrame
    let crsp_msf_lazy = load_parquet(&crsp_dir_path.join("crsp_msf.parquet"))?;
    let crsp_mseexchdates_lazy = load_parquet(&crsp_dir_path.join("crsp_mseexchdates.parquet"))?;

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
        .collect()
        .context("Failed to join and filter the CRSP data.")?;

    // Check to see if we should only keep share codes 10 and 11 (domestic common equity)
    if params.dom_com_eq_flag {
        // Filter the DataFrame to only keep share codes 10 and 11
        result = result
            .clone()
            .lazy()
            .filter(
                col("shrcd").eq(lit(10)).or(col("shrcd").eq(lit(11))), // The logic ensures that only rows with share codes 10 and 11 are retained.
            )
            .collect()
            .context("Failed to filter out non-domestic common equity.")?;

        println!("Filtered out non-domestic common equity.");
    }

    println!("Schema of the filtered DataFrame:\n{:?}", result.schema());

    // Save permno and dates as JSON
    save_unique_column(&result, "permno", &crsp_dir_path, "permno.json")?;
    save_unique_dates(&result, "date", &crsp_dir_path, "dates.json")?;

    // Save the link file for the COMPUSTAT matrices creation
    save_link_file(&result, &crsp_dir_path)?;

    // Rename returns to indicate they are without delisting adjustment
    // Rename volume to indicate it is without adjustment for NASDAQ
    let lazy_df = result.lazy();

    // Specify the existing and new column names
    let existing_names = ["ret", "vol"];
    let new_names = ["ret_x_dl", "vol_x_adj"];
    // Rename the columns
    let result = lazy_df
        .rename(existing_names, new_names, true)
        .collect()
        .unwrap();

    // List of variables to extract
    let var_names = vec![
        "shrcd",
        "exchcd",
        "siccd",
        "prc",
        "bid",
        "ask",
        "bidlo",
        "askhi",
        "vol_x_adj",
        "ret_x_dl",
        "shrout",
        "cfacpr",
        "cfacshr",
        "spread",
        "retx",
    ];

    // Iterate through the variable names
    for (i, var_name) in var_names.iter().enumerate() {
        println!(
            "Now working on variable {} ({} out of {}).",
            var_name,
            i + 1,
            var_names.len()
        );

        process_variable(&result, var_name, Path::new(&crsp_dir_path))?;
    }

    Ok(())
}

fn save_link_file(dataframe: &DataFrame, path: &Path) -> Result<()> {
    let link = dataframe
        .clone()
        .lazy()
        .select([
            col("permno"),
            col("date").dt().to_string("%Y%m").cast(DataType::Int32),
        ])
        .collect()?;

    let link_array = link.to_ndarray::<Int32Type>(Default::default())?;
    save_ndarray_as_json(link_array, path, "crsp_link.json")
}

pub fn load_parquet(path: &Path) -> Result<LazyFrame> {
    LazyFrame::scan_parquet(path, Default::default())
        .with_context(|| format!("Failed to load parquet file: {:?}", path))
}

fn save_unique_column(df: &DataFrame, column: &str, dir: &Path, filename: &str) -> Result<()> {
    let unique_values = df
        .clone()
        .lazy()
        .select([col(column).unique_stable()])
        .collect()?
        .to_ndarray::<Int32Type>(Default::default())?;
    save_ndarray_as_json(unique_values, dir, filename)
}

fn save_unique_dates(df: &DataFrame, column: &str, dir: &Path, filename: &str) -> Result<()> {
    let dates_col = df
        .clone()
        .lazy()
        .select([col(column).dt().to_string("%Y%m").unique_stable()])
        .collect()?;
    let dates = dates_col
        .lazy()
        .select([col(column).cast(DataType::Int32)])
        .collect()?
        .to_ndarray::<Int32Type>(Default::default())?;
    save_ndarray_as_json(dates, dir, filename)
}

fn process_variable(df: &DataFrame, var_name: &str, dir: &Path) -> Result<()> {
    // to dimension nMonths x nPermno
    let temp_df = df
        .clone()
        .lazy()
        .select([col("permno"), col("date"), col(var_name)])
        .collect()?;

    let column_type = temp_df.schema().get_field(var_name).unwrap();

    let mut pivoted_df = pivot(
        &temp_df,
        ["permno"],
        Some(["date"]),
        Some([var_name]),
        false,
        None,
        None,
    )?
    .fill_null(FillNullStrategy::Zero)?;

    pivoted_df.drop_in_place("date")?;

    match column_type.dtype {
        DataType::Int16 => save_ndarray::<Int16Type>(&pivoted_df, dir, var_name),
        DataType::Int32 => save_ndarray::<Int32Type>(&pivoted_df, dir, var_name),
        DataType::Int64 => save_ndarray::<Int64Type>(&pivoted_df, dir, var_name),
        DataType::Float32 => save_ndarray::<Float32Type>(&pivoted_df, dir, var_name),
        DataType::Float64 => save_ndarray::<Float64Type>(&pivoted_df, dir, var_name),
        _ => Err(anyhow::anyhow!("Unsupported data type for {}", var_name)),
    }
}

fn save_ndarray<T: PolarsNumericType>(df: &DataFrame, dir: &Path, var_name: &str) -> Result<()>
where
    T: PolarsNumericType,
    T::Native: serde::Serialize,
{
    let ndarray = df.to_ndarray::<T>(Default::default())?;
    save_ndarray_as_json(ndarray, dir, &format!("{}.json", var_name))
}

fn save_ndarray_as_json<T: serde::Serialize>(
    ndarray: Array2<T>,
    dir: &Path,
    filename: &str,
) -> Result<()> {
    let json = serde_json::to_string(&ndarray)?;
    let file_path = dir.join(filename);
    File::create(&file_path)
        .and_then(|mut file| file.write_all(json.as_bytes()))
        .with_context(|| format!("Failed to write ndarray to file: {:?}", file_path))?;
    println!("Saved matrix for {}.", filename);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rename_column() {
        // Create a sample DataFrame
        let df = df![
            "foo" => [1, 2, 3],
            "bar" => [6, 7, 8],
            "ham" => ["a", "b", "c"]
        ]
        .unwrap();

        // Convert the DataFrame to a LazyFrame
        let lazy_df = df.lazy();

        // Specify the existing and new column names
        let existing_names = ["bar"];
        let new_names = ["banana"];

        // Rename the columns
        let renamed_df = lazy_df
            .rename(existing_names, new_names, true)
            .collect()
            .unwrap();

        // Print the renamed DataFrame
        println!("{:?}", renamed_df);
    }

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
