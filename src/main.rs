extern crate mysql;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate serde;
extern crate toml;
extern crate base64;
extern crate chrono;
#[macro_use]
extern crate clap;
#[macro_use]
extern crate structopt;
extern crate csv;


use std::fs::File;
use std::path::Path;
use std::io::Read;
use std::str;
use std::io::{self, Write};
use std::convert::From;
use serde_json as json;
use chrono::prelude::*;
use chrono::Duration;
use structopt::StructOpt;


#[derive(Deserialize)]
struct Config {
    host: Option<String>,
    port: Option<u16>,
    user: Option<String>,
    password: Option<String>,
}

fn read_config<P: AsRef<Path>>(path: P) -> Result<Config, String> {
    let mut file = File::open(path).map_err(|e| e.to_string())?;
    let mut buf = String::new();
    file.read_to_string(&mut buf).map_err(|e| e.to_string())?;
    toml::from_str(&buf).map_err(|e| e.to_string())
}

fn to_json_value(val: &mysql::Value) -> json::Value {
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
            json::Value::String(Utc.ymd(year as i32, month as u32, day as u32)
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

fn to_csv_value(val: &mysql::Value) -> String {
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
            Utc.ymd(year as i32, month as u32, day as u32)
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
    #[structopt(long = "format", default_value = "json", raw(possible_values = "&Format::variants()", case_insensitive = "true"))]
    format: Format,

    #[structopt(subcommand)]
    cmd: Command,
}

#[derive(StructOpt, Debug)]
enum Command {
    #[structopt(name = "query")]
    Query {
        /// Table to read
        #[structopt(short = "e", name = "SQL")]
        opt_sql: Option<String>,
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

    let config = read_config("config.toml").unwrap();

    let mut builder = mysql::OptsBuilder::new();
    builder.ip_or_hostname(config.host)
           .tcp_port(config.port.unwrap_or(3306))
           .user(config.user)
           .pass(config.password)
           .prefer_socket(false);

    let pool = mysql::Pool::new(builder).unwrap();

    let stdout = io::stdout();
    let mut stdout = stdout.lock();

    match opt.cmd {
        Command::Query { opt_sql } => {
            let sql = match opt_sql {
                Some(sql) => {
                    sql
                },
                None => {
                    let mut buf = String::new();
                    io::stdin().read_to_string(&mut buf).unwrap();
                    buf
                },
            };
            let mut stmt = pool.prepare(sql).unwrap();
            let result: mysql::QueryResult = stmt.execute(()).unwrap();
            let column_names: Vec<String> = result.columns_ref().iter().map(|c| c.name_str().into_owned()).collect();
            match opt.format {
                Format::Csv => {
                    let mut wtr = csv::WriterBuilder::new()
                        .from_writer(stdout);
                    wtr.write_record(&column_names).unwrap();
                    for row in result {
                        let row: mysql::Row = row.unwrap();
                        let values: Vec<String> = column_names.iter().map(|col_name| {
                            to_csv_value(&row[col_name.as_str()])
                        }).collect();
                        wtr.write_record(values).unwrap();
                    }
                    wtr.flush().unwrap();
                },
                Format::Json => {
                    for row in result {
                        let row: mysql::Row = row.unwrap();
                        let row_obj: json::Map<String, json::Value> = column_names.iter().map(|col_name| {
                            (col_name.to_owned(), to_json_value(&row[col_name.as_str()]))
                        }).collect();
                        json::to_writer(&mut stdout, &row_obj).unwrap();
                        stdout.write(&[b'\n']).unwrap();
                    }
                },
            }
        },
        Command::Tail { table, column } => {
            let mut last_id: u32 = {
                let sql = format!(r#"SELECT max({column}) AS max_id FROM {table};"#, table=table, column=column);
                let row = pool.first_exec(sql, ()).unwrap().unwrap();
                row.get("max_id").unwrap()
            };
            let mut stmt = {
                let sql = format!(r#"SELECT * FROM {table} WHERE {column} > ? ORDER BY {column};"#, table=table, column=column);
                pool.prepare(sql).unwrap()
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
                                to_csv_value(&row[col_name.as_str()])
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
                                to_csv_value(&row[col_name.as_str()])
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
                                (col_name.to_owned(), to_json_value(&row[col_name.as_str()]))
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
