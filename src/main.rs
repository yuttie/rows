#[macro_use]
extern crate mysql;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate toml;


use std::fs::File;
use std::path::Path;
use std::io::Read;


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

fn main() {
    let config = read_config("config.toml").unwrap();

    let mut builder = mysql::OptsBuilder::new();
    builder.ip_or_hostname(config.host)
           .tcp_port(config.port.unwrap_or(3306))
           .user(config.user)
           .pass(config.password)
           .prefer_socket(false);

    let pool = mysql::Pool::new(builder).unwrap();

    let mut last_id = {
        let row = pool.first_exec(r#"SELECT max(id) FROM test_db.table;"#, ()).unwrap();
        mysql::from_row::<u32>(row.unwrap())
    };
    let mut stmt = pool.prepare(r#"SELECT id FROM test_db.table WHERE id > ? ORDER BY id;"#).unwrap();
    loop {
        for row in stmt.execute((last_id, )).unwrap() {
            let id = mysql::from_row::<u32>(row.unwrap());
            println!("{}", id);
            if id > last_id {
                last_id = id;
            }
        }
    }
}
