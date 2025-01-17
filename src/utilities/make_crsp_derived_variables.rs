use super::make_crsp_monthly_data::{load_parquet, Params};
use anyhow::Result;
use ndarray::Array2;
use polars::lazy::dsl::*;
use polars::prelude::*;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub fn make_crsp_derived_variables(params: &Params) -> Result<()> {
    let crsp_dir_path = Path::new(&params.directory).join("data/crsp");

    // Load data
    let mut ret_x_dl: Array2<f64> = load_array(&crsp_dir_path, "ret_x_dl.json")?;
    let permno: Array2<i32> = load_array(&crsp_dir_path, "permno.json")?;
    let date: Array2<i32> = load_array(&crsp_dir_path, "dates.json")?;

    // Read the CRSP delist returns file
    let mut crsp_msedelist: LazyFrame =
        load_parquet(&crsp_dir_path.join("crsp_msedelist.parquet"))?;

    // Filter delisting data
    let crsp_msedelist = filter_delisting_data(crsp_msedelist, &permno, &date)?;
    dbg!(crsp_msedelist);
    Ok(())
}

fn filter_delisting_data(
    crsp_msedelist: LazyFrame,
    permno: &Array2<i32>,
    date: &Array2<i32>,
) -> Result<DataFrame> {
    // Convert permno to a Vec for filtering
    let permno_vec: Vec<i32> = permno.iter().copied().collect();

    // Clone the Series to avoid "moved value" error
    let permno_series = Series::new("permno".into(), permno_vec);
    // Apply filtering to LazyFrame
    let filtered = crsp_msedelist
        .lazy()
        .clone()
        .filter(
            cols(["permno"])
                .is_in(lit(permno_series))
                .and(col("dlstdt").neq(col("dlstdt").max())),
        )
        .with_columns([col("dlstdt")
            .dt()
            .to_string("%Y%m")
            .cast(DataType::Int32)
            .alias("date")])
        .filter(cols(["date"]).lt(lit(date.iter().cloned().max().unwrap())))
        .collect()?;

    Ok(filtered)
}

fn load_array<T>(crsp_path: &Path, file_name: &str) -> Result<Array2<T>>
where
    T: DeserializeOwned + std::fmt::Debug,
{
    let mut file = File::open(crsp_path.join(file_name))?;
    let mut json = String::new();
    file.read_to_string(&mut json)?;
    // Deserialize JSON to Array2<T>
    let data: Array2<T> = serde_json::from_str(&json)?;
    Ok(data)
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_load_data() {
        let params = Params {
            directory: ".".to_string(),
            sample_start: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            sample_end: NaiveDate::from_ymd_opt(2001, 12, 31).unwrap(),
            dom_com_eq_flag: true,
        };
        let crsp_dir_path = Path::new(&params.directory).join("data/crsp");

        let ret_x_dl: Array2<f64> = load_array(&crsp_dir_path, "ret_x_dl.json").unwrap();

        dbg!(ret_x_dl);
    }

    #[test]
    fn test_make_crsp_derived_variables() {
        let params = Params {
            directory: ".".to_string(),
            sample_start: NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(),
            sample_end: NaiveDate::from_ymd_opt(2001, 12, 31).unwrap(),
            dom_com_eq_flag: true,
        };
        make_crsp_derived_variables(&params).unwrap();
    }
}
