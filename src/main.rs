use std::env;
use std::fmt::Display;
use std::io::Read;
use std::str;
use std::io::{self, Write};
use std::convert::From;
use std::vec::Vec;

use clap::arg_enum;
use chrono::prelude::*;
use chrono::Duration;
use dotenv;
use mysql;
use serde_json as json;
use structopt::StructOpt;


fn to_json_value<T>(val: &mysql::Value, tz: Option<T>) -> json::Value where T: TimeZone, T::Offset: Display {
    match val {
        &mysql::Value::NULL => json::Value::Null,
        &mysql::Value::Bytes(ref bytes) => {
            match str::from_utf8(bytes) {
                Ok(s) => json::Value::String(s.to_owned()),
                Err(_) => json::Value::String(base64::encode(bytes)),
            }
        },
        &mysql::Value::Int(num) => json::Value::Number(json::Number::from(num)),
        &mysql::Value::UInt(num) => json::Value::Number(json::Number::from(num)),
        &mysql::Value::Float(num) => json::Value::Number(json::Number::from_f64(num).unwrap()),
        &mysql::Value::Date(year, month, day, hour, min, sec, usec) => {
            json::Value::String(tz.expect("DATETIME-like column requires a timezone offset specified with --timezone")
                                  .ymd(year as i32, month as u32, day as u32)
                                  .and_hms_micro(hour as u32, min as u32, sec as u32, usec as u32).to_rfc3339())
        },
        &mysql::Value::Time(is_neg, days, hours, minutes, seconds, microseconds) => {
            // TODO
            let duration = Duration::days(days as i64)
                         + Duration::hours(hours as i64)
                         + Duration::minutes(minutes as i64)
                         + Duration::seconds(seconds as i64)
                         + Duration::microseconds(microseconds as i64);
            let duration = if is_neg { -duration } else { duration };
            json::Value::String(format!("{}", duration))
        },
    }
}

fn to_csv_value<T>(val: &mysql::Value, tz: Option<T>) -> String where T: TimeZone, T::Offset: Display {
    match val {
        &mysql::Value::NULL => String::new(),
        &mysql::Value::Bytes(ref bytes) => {
            match str::from_utf8(bytes) {
                Ok(s) => s.to_owned(),
                Err(_) => base64::encode(bytes),
            }
        },
        &mysql::Value::Int(num) => num.to_string(),
        &mysql::Value::UInt(num) => num.to_string(),
        &mysql::Value::Float(num) => num.to_string(),
        &mysql::Value::Date(year, month, day, hour, min, sec, usec) => {
            tz.expect("DATETIME-like column requires a timezone offset specified with --timezone")
              .ymd(year as i32, month as u32, day as u32)
              .and_hms_micro(hour as u32, min as u32, sec as u32, usec as u32).to_rfc3339()
        },
        &mysql::Value::Time(is_neg, days, hours, minutes, seconds, microseconds) => {
            // TODO
            let duration = Duration::days(days as i64)
                         + Duration::hours(hours as i64)
                         + Duration::minutes(minutes as i64)
                         + Duration::seconds(seconds as i64)
                         + Duration::microseconds(microseconds as i64);
            let duration = if is_neg { -duration } else { duration };
            format!("{}", duration)
        },
    }
}

arg_enum! {
    #[derive(PartialEq, Debug)]
    enum Format {
        Csv,
        Json,
    }
}

#[derive(StructOpt, Debug)]
#[structopt(name = "bottle")]
struct Opt {
    #[structopt(long = "config", name = "config_file")]
    config_file: Option<String>,

    #[structopt(long = "format", default_value = "json", raw(possible_values = "&Format::variants()", case_insensitive = "true"))]
    format: Format,

    /// Timezone in which DATETIME-like values are interpreted (in seconds)
    #[structopt(long = "time-zone", name = "offset")]
    tz_offset: Option<i32>,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "query")]
    Query {
        /// Statement to execute
        #[structopt(short = "e", name = "SQL")]
        sqls: Vec<String>,
    },
    #[structopt(name = "tail")]
    Tail {
        /// Table to read
        #[structopt(name = "TABLE")]
        table: String,

        /// Column of primary key
        #[structopt(name = "COLUMN")]
        column: String,
    },
}

fn main() {
    let opt = Opt::from_args();

    if let Some(fp) = opt.config_file {
        dotenv::from_path(fp).unwrap();
    }
    else {
        dotenv::dotenv().ok();
    }

    let mut builder = mysql::OptsBuilder::new();
    builder.ip_or_hostname(env::var("BOTTLE_HOST").ok())
           .tcp_port(env::var("BOTTLE_PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(3306))
           .user(env::var("BOTTLE_USER").ok())
           .pass(env::var("BOTTLE_PASSWORD").ok())
           .db_name(env::var("BOTTLE_DATABASE").ok())
           .prefer_socket(false);

    let mut conn = mysql::Conn::new(builder).unwrap();

    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    let tz: Option<FixedOffset> = opt.tz_offset.map(FixedOffset::east);

    match opt.cmd {
        Command::Query { sqls } => {
            let sqls = if sqls.is_empty() {
                let mut buf = String::new();
                io::stdin().read_to_string(&mut buf).unwrap();
                vec![buf]
            }
            else {
                sqls
            };
            match opt.format {
                Format::Csv => {
                    let mut wtr = csv::WriterBuilder::new()
                        .from_writer(stdout);
                    for sql in sqls {
                        let mut stmt = conn.prepare(sql).unwrap();
                        let result: mysql::QueryResult = stmt.execute(()).unwrap();
                        let column_names: Vec<String> = result.columns_ref().iter().map(|c| c.name_str().into_owned()).collect();
                        wtr.write_record(&column_names).unwrap();
                        for row in result {
                            let row: mysql::Row = row.unwrap();
                            let values: Vec<String> = column_names.iter().map(|col_name| {
                                to_csv_value(&row[col_name.as_str()], tz)
                            }).collect();
                            wtr.write_record(values).unwrap();
                        }
                        wtr.flush().unwrap();
                    }
                },
                Format::Json => {
                    for sql in sqls {
                        let mut stmt = conn.prepare(sql).unwrap();
                        let result: mysql::QueryResult = stmt.execute(()).unwrap();
                        let column_names: Vec<String> = result.columns_ref().iter().map(|c| c.name_str().into_owned()).collect();
                        for row in result {
                            let row: mysql::Row = row.unwrap();
                            let row_obj: json::Map<String, json::Value> = column_names.iter().map(|col_name| {
                                (col_name.to_owned(), to_json_value(&row[col_name.as_str()], tz))
                            }).collect();
                            json::to_writer(&mut stdout, &row_obj).unwrap();
                            stdout.write(&[b'\n']).unwrap();
                        }
                    }
                },
            }
        },
        Command::Tail { table, column } => {
            let mut last_id: u32 = {
                let sql = format!(r#"SELECT max({column}) AS max_id FROM {table};"#, table=table, column=column);
                let row: mysql::Row = conn.first_exec(sql, ()).unwrap().unwrap();
                row.get("max_id").unwrap()
            };
            let mut stmt = {
                let sql = format!(r#"SELECT * FROM {table} WHERE {column} > ? ORDER BY {column};"#, table=table, column=column);
                conn.prepare(sql).unwrap()
            };
            match opt.format {
                Format::Csv => {
                    let mut wtr = csv::WriterBuilder::new()
                        .from_writer(stdout);
                    let column_names: Vec<String> = {
                        let result: mysql::QueryResult = stmt.execute((last_id, )).unwrap();
                        let column_names: Vec<String> = result.columns_ref().iter().map(|c| c.name_str().into_owned()).collect();
                        wtr.write_record(&column_names).unwrap();
                        for row in result {
                            let row: mysql::Row = row.unwrap();
                            let values: Vec<String> = column_names.iter().map(|col_name| {
                                to_csv_value(&row[col_name.as_str()], tz)
                            }).collect();
                            wtr.write_record(values).unwrap();

                            let id: u32 = row.get(column.as_str()).unwrap();
                            if id > last_id {
                                last_id = id;
                            }
                        }
                        wtr.flush().unwrap();
                        column_names
                    };
                    loop {
                        let result: mysql::QueryResult = stmt.execute((last_id, )).unwrap();
                        for row in result {
                            let row: mysql::Row = row.unwrap();
                            let values: Vec<String> = column_names.iter().map(|col_name| {
                                to_csv_value(&row[col_name.as_str()], tz)
                            }).collect();
                            wtr.write_record(values).unwrap();

                            let id: u32 = row.get(column.as_str()).unwrap();
                            if id > last_id {
                                last_id = id;
                            }
                        }
                        wtr.flush().unwrap();
                    }
                },
                Format::Json => {
                    loop {
                        let result: mysql::QueryResult = stmt.execute((last_id, )).unwrap();
                        let column_names: Vec<String> = result.columns_ref().iter().map(|c| c.name_str().into_owned()).collect();
                        for row in result {
                            let row: mysql::Row = row.unwrap();
                            let row_obj: json::Map<String, json::Value> = column_names.iter().map(|col_name| {
                                (col_name.to_owned(), to_json_value(&row[col_name.as_str()], tz))
                            }).collect();
                            json::to_writer(&mut stdout, &row_obj).unwrap();
                            stdout.write(&[b'\n']).unwrap();

                            let id: u32 = row.get(column.as_str()).unwrap();
                            if id > last_id {
                                last_id = id;
                            }
                        }
                    }
                },
            }
        }
    }
}
