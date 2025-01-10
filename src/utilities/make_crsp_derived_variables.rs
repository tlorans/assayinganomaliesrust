use super::make_crsp_monthly_data::Params;
use anyhow::Result;
use ndarray::Array2;
use serde::de::DeserializeOwned;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub fn make_crsp_derived_variables(params: &Params) -> Result<()> {
    let crsp_dir_path = Path::new(&params.directory).join("data/crsp");

    // Load data
    let ret_x_dl: Array2<f64> = load_array(&crsp_dir_path, "ret_x_dl.json")?;
    let permno: Array2<i32> = load_array(&crsp_dir_path, "permno.json")?;
    let date: Array2<i32> = load_array(&crsp_dir_path, "date.json")?;
    Ok(())
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

        let ret_x_dl: Array2<f64> = load_data(&crsp_dir_path, "ret_x_dl.json").unwrap();

        dbg!(ret_x_dl);
    }
}
