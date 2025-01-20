use clap::Parser;
use std::collections::HashMap;
use std::{error::Error, process};
use csv::StringRecord;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Input file to process
    file: String,

    /// Columns to display
    #[arg(short, long)]
    cols: Option<String>,

    /// Filtering columns by some value
    #[arg(short, long)]
    filter: Option<String>,

    /// Max rows to display
    #[arg(short, long, default_value_t = 10)]
    n: u32,

    /// Offset
    #[arg(short, long, default_value_t = 0)]
    offset: u32,

    /// Display CSV info
    #[arg(short, long)]
    info: bool,
}

#[derive(PartialEq)]
enum RowFilterOperator {
    // Equal,
    // Lesser,
    // Greater,
    EqualString
}

impl RowFilterOperator {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::EqualString, Self::EqualString) => true,
            _ => false,
        }
    }
}

struct RowFilter {
    left_column: Option<usize>,
    right_column: Option<usize>,
    left_value: Option<String>,
    right_value: Option<String>,
    operator: RowFilterOperator,
}

impl RowFilter {
    fn new(filter_str: &str, col_idx_dict: HashMap<String, usize>) -> Self {

        // TODO: depending on what the filter string says,
        //       a different filter should be built

        let operator : RowFilterOperator;

        // In the three first cases,
        // left and right are coerced as float numbers
        // if filter_str.contains("<") {
        //     operator = RowFilterOperator::Lesser;
        // } else if filter_str.contains(">") {
        //     operator = RowFilterOperator::Greater;
        // } else if filter_str.contains("==") {
        //     operator = RowFilterOperator::Equal;
        // } else
        
        let left_and_right: Vec<&str>;
        let left_column: usize;
        let right_column: usize;
        let left_value: &str;
        let right_value: &str;
        if filter_str.contains("=") {
            // In this case, left and right are treated as strings
            operator = RowFilterOperator::EqualString;
            left_and_right = filter_str.split('=').collect();

            // In this case, left should be the column
            // And right should be the value
            left_column = *col_idx_dict.get(left_and_right[0]).unwrap();
            // We get the index column
            right_value = left_and_right[1];

            return Self {
                left_column: Some(left_column),
                right_column: None,
                left_value: None,
                right_value: Some(String::from(right_value)),
                operator: operator,
            }
        } else {
            panic!("No operator for filter string {}", filter_str);
        }

        if left_and_right.len() != 2 {
            panic!("Wrong formatted filter: {}", filter_str);
        }
        
        Self {
            left_column: None,
            right_column: None,
            left_value: None,
            right_value: None,
            operator: operator,
        }
    }

    fn accepts(&self, row: csv::StringRecord) -> bool {
        match self.operator {
            RowFilterOperator::EqualString => {
                let left_value = row.get(self.left_column.unwrap()).unwrap();
                let right_value = self.right_value.as_ref().unwrap().as_str();
                return left_value == right_value;
            },
            _ => panic!("Unknown operator"), 
        }
    }
}

fn read_csv(csv: &str, cols: Option<String>,
            offset: u32,
            max_rows: u32, info: bool,
            filters: Option<String>
) -> Result<(), Box<dyn Error>> {
    // Build the CSV reader and iterate over each record.
    let mut rdr = csv::Reader::from_path(csv)?;

    if info {
        let mut n_cols : u32 = 0;
        println!("CSV columns:");
        for header in rdr.headers().unwrap().iter() {
            println!("{}", header);
            n_cols += 1;
        }
        println!("Number of columns: {}", n_cols);

        let mut n_rows = 0;
        for _ in rdr.records() {
            n_rows += 1;
        }
        println!("Number of rows: {}", n_rows);
        
        return Ok(());
    }

    let mut col_idx_hashmap : HashMap::<String, usize> = HashMap::new();
    let headers = rdr.headers().unwrap();
    let mut header_idx : usize = 0;
    for header in headers {
        col_idx_hashmap.insert(header.to_string(), header_idx);
        header_idx += 1;
    }
    
    // It creates a hashmap column name -> column index
    // So it can be used later with the filters
    let mut col_indices: Vec<usize> = Vec::new();
    if let Some(ref col_name) = cols {
        let col_names: Vec<&str> = col_name.split(',').collect();
        let headers = rdr.headers().unwrap();
        col_indices = col_names.iter()
            .map(|&name| headers.iter().position(|h| h == name).ok_or_else(|| "Column not found"))
            .collect::<Result<Vec<usize>, &str>>()?;
        let mut print_index = 0;
        for i in &col_indices {
            print!("{}", headers[*i].to_string());
            if print_index < col_indices.len() - 1 {
                print!(",");
            }
            print_index += 1;
        }
        print!("\n");
    }

    // Creates the filter list
    let mut _n_filters: Vec<RowFilter> = Vec::new();
    if let Some(filters_str) = filters {
        let _n_filter_strings: Vec<&str> = filters_str.split(',').collect();

        for filter_str in _n_filter_strings {
            _n_filters.push(RowFilter::new(filter_str, col_idx_hashmap.clone()));
        }
    }
        
    let mut rows_processed : u32 = 0;
    let mut rows_ignored : u32 = 0;
    'records_loop: for result in rdr.records() {
        if rows_ignored < offset {
            rows_ignored += 1;
            continue;
        }
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result?;

        for filter in &_n_filters {
            let cloned_record = record.clone();
            if !filter.accepts(cloned_record) {
                continue 'records_loop;
            }
        }

        match cols {
            Some(_) => {
                let mut print_index = 0;
                for i in &col_indices {
                    let col_value = record.get(*i).unwrap_or_default();
                    print!("{}", col_value);
                    if print_index < col_indices.len() - 1 {
                        print!(",");
                    }
                    print_index += 1;
                }
                print!("\n");
            },
            None => println!("{:?}", record),
        }

        rows_processed += 1;

        if rows_processed == max_rows {
            break;
        }
    }
    Ok(())
}

// Example of use:
// csvpeek <file> --info -> prints general info of the csv
// csvpeek <file> -> prints the whole file (restricted by -n)
// csvpeek <file> --cols col1,col2,col3 -> shows the data but only for certain columns
// csvpeek <file> -n N -> shows up to N rows
// csvpeek <file> --offset M -> Ignore first M rows
// Next features (not implemented yet):
// csvpeek <file> --filter "image_number<3" -> applies different filters: <, >, =, streq, etc.
// csvpeek <file> --cols col1 --agg sum -> does an aggregate of the columns.
// Agregates: sum, stdp, stds, avg, count
fn main() {
    let args = Args::parse();

    let csv = args.file;

    if let Err(err) = read_csv(&csv, args.cols, args.offset, args.n, args.info, args.filter) {
        println!("Error reading or processing CSV: {}", err);
        process::exit(1);
    }
}

#[test]
fn test_equal_row_filter_constructor() {
    let mut hash_map = HashMap::<String, usize>::new();
    hash_map.insert(String::from("IMAGE_NAME"), 2);
    let row_filter = RowFilter::new("IMAGE_NAME=file1.png", hash_map);

    // The operator should be EqualString
    assert!(row_filter.operator == RowFilterOperator::EqualString);
    assert_eq!(row_filter.left_column, Some(2));
    assert_eq!(row_filter.right_column, None);
    assert_eq!(row_filter.left_value, None);
    assert_eq!(row_filter.right_value, Some(String::from("file1.png")));
}

#[test]
fn test_equal_row_filter_accepts_method() {
    let mut hash_map = HashMap::<String, usize>::new();
    hash_map.insert(String::from("IMAGE_NAME"), 2);
    let row_filter = RowFilter::new("IMAGE_NAME=file1.png", hash_map);

    let record = StringRecord::from(vec!["someContentInFirstColumn", "someContentInSecondColumn", "file1.png"]);

    let record2 = StringRecord::from(vec!["someContentInFirstColumn", "someContentInSecondColumn", "file2.png"]);

    // First record should be accepted
    assert!(row_filter.accepts(record));
    // First record should not be accepted
    assert!(!row_filter.accepts(record2));
}
