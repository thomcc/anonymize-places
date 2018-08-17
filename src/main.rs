extern crate rusqlite;
extern crate dirs;

#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
extern crate rand;
extern crate clap;
extern crate env_logger;

use rand::prelude::*;
use std::{process, fs, path::{Path, PathBuf}};
use std::collections::HashMap;

use rusqlite::{Connection, OpenFlags};

type Result<T> = std::result::Result<T, failure::Error>;

#[derive(Clone, Debug)]
struct Profile {
    name: String,
    places_db: PathBuf,
    db_size: u64,
}

impl Profile {
    fn friendly_db_size(&self) -> String {
        let sizes = [
            (1024 * 1024 * 1024, "Gb"),
            (1024 * 1024, "Mb"),
            (1024, "Kb"),
        ];
        for (lim, suffix) in &sizes {
            if self.db_size >= *lim {

                return format!("~{} {}", ((self.db_size as f64 / *lim as f64) * 10.0).round() / 10.0, suffix);
            }
        }
        format!("{} bytes", self.db_size)
    }
}

// Only used if we 
fn get_profiles() -> Result<Vec<Profile>> {
    let mut path = match dirs::home_dir() {
        Some(dir) => dir,
        None => bail!("No home directory found!")
    };
    if cfg!(windows) {
        path.extend(&["AppData", "Roaming", "Mozilla", "Firefox", "Profiles"]);
    } else {
        let out = String::from_utf8(
            process::Command::new("uname").args(&["-s"]).output()?.stdout)?;
        println!("Uname says: {:?}", out);
        if out.trim() == "Darwin" {
            // ~/Library/Application Support/Firefox/Profiles
            path.extend(&["Library", "Application Support", "Firefox", "Profiles"]);
        } else {
            // I'm not actually sure if this is true for all non-macos unix likes.
            path.extend(&[".mozilla", "firefox"]);
        }
    }
    debug!("Using profile path: {:?}", path);
    let res = fs::read_dir(path)?
    .map(|entry_result| {
        let entry = entry_result?;
        trace!("Considering path {:?}", entry.path());
        if !entry.path().is_dir() {
            trace!("  Not dir: {:?}", entry.path());
            return Ok(None);
        }
        let mut path = entry.path().to_owned();
        let profile_name = path.file_name().unwrap_or_default().to_str().ok_or_else(|| {
            warn!("  Path has invalid UTF8: {:?}", path);
            format_err!("Path has invalid UTF8: {:?}", path)
        })?.into();
        path.push("places.sqlite");
        if !path.exists() {
            return Ok(None);
        }
        let metadata = fs::metadata(&path)?;
        let db_size = metadata.len();
        Ok(Some(Profile {
            name: profile_name,
            places_db: path,
            db_size,
        }))
    }).filter_map(|result: Result<Option<Profile>>| {
        match result {
            Ok(val) => val,
            Err(e) => {
                debug!("Got error finding profile directory, skipping: {}", e);
                None
            }
        }
    }).collect::<Vec<_>>();
    Ok(res)
}
#[derive(Default, Clone, Debug)]
struct StringAnonymizer {
    table: HashMap<String, String>,
}

fn rand_string_of_len(len: usize) -> String {
    let mut rng = thread_rng();
    rng.sample_iter(&rand::distributions::Alphanumeric).take(len).collect()
}

impl StringAnonymizer {

    fn anonymize(&mut self, s: &str) -> String {
        if s.len() == 0 {
            return "".into();
        }
        if let Some(a) = self.table.get(s) {
            return a.clone();
        }
        for i in 0..10 {
            let replacement = rand_string_of_len(s.len());
            // keep trying but force it at the last time
            if self.table.get(&replacement).is_some() && i != 9 {
                continue;
            }

            self.table.insert(s.into(), replacement.clone());
            return replacement;
        }
        unreachable!("Bug in anonymize retry loop");
    }

}

#[derive(Debug, Clone)]
struct TableInfo {
    name: String,
    cols: Vec<String>
}

impl TableInfo {
    fn for_table(name: String, conn: &Connection) -> Result<TableInfo> {
        let stmt = conn.prepare(&format!("SELECT * FROM {}", name))?;
        let cols = stmt.column_names().into_iter().map(|x| x.to_owned()).collect();
        Ok(TableInfo { name, cols })
    }
    fn make_update(&self, updater_fn: &str) -> String {
        let sets = self.cols.iter()
            .map(|col| format!("{} = {}({})", col, updater_fn, col))
            .collect::<Vec<_>>()
            .join(",\n    ");
        format!("UPDATE {}\nSET {}", self.name, sets)
    }
}

fn main() -> Result<()> {
    let matches = clap::App::new("anonymize-places")
        .arg(clap::Arg::with_name("OUTPUT")
            .index(1)
            .help("Path where we should output the anonymized db (defaults to places_anonymized.sqlite)"))
        .arg(clap::Arg::with_name("PLACES")
            .index(2)
            .help("Path to places.sqlite. If not provided, we'll use the largest places.sqlite in your firefox profiles"))
        .arg(clap::Arg::with_name("v")
            .short("v")
            .multiple(true)
            .help("Sets the level of verbosity"))
        .arg(clap::Arg::with_name("force")
            .short("f")
            .long("force")
            .help("Overwrite OUTPUT if it already exists"))
    .get_matches();

    env_logger::init_from_env(match matches.occurrences_of("v") {
        0 => env_logger::Env::default().filter_or("RUST_LOG", "warn"),
        1 => env_logger::Env::default().filter_or("RUST_LOG", "info"),
        2 => env_logger::Env::default().filter_or("RUST_LOG", "debug"),
        3 | _ => env_logger::Env::default().filter_or("RUST_LOG", "trace"),
    });

    let profile = if let Some(places) = matches.value_of("PLACES") {
        let meta = fs::metadata(&places)?;
        Profile {
            name: "".into(),
            places_db: fs::canonicalize(places)?,
            db_size: meta.len(),
        }
    } else {
        let mut profiles = get_profiles()?;
        if profiles.len() == 0 {
            eprintln!("No profiles found!");
            bail!("No profiles found");
        }
        profiles.sort_by(|a, b| b.db_size.cmp(&a.db_size));
        for p in &profiles {
            debug!("Found: {:?} with a {} places.sqlite", p.name, p.friendly_db_size())
        }
        println!("Using profile {:?}", profiles[0].name);
        profiles.into_iter().next().unwrap()
    };

    let output_path = Path::new(matches.value_of("OUTPUT")
        .unwrap_or_else(|| "./places_anonymized.sqlite".into()));
    if output_path.exists() {
        if matches.is_present("force") {
            fs::remove_file(&output_path)?;
        } else {
            eprintln!("Error: {} already exists but `-f` argument was not provided", output_path.to_str().unwrap());
            bail!("File already exists");
        }
    }

    fs::copy(&profile.places_db, &output_path)?;
    let anon_places = Connection::open_with_flags(&output_path,
        OpenFlags::SQLITE_OPEN_READ_WRITE)?;

    {
        let mut anonymizer = StringAnonymizer::default();
        anon_places.create_scalar_function("anonymize", 1, true, move |ctx| {
            let arg = ctx.get::<rusqlite::types::Value>(0)?;
            Ok(match arg {
                rusqlite::types::Value::Text(s) =>
                    rusqlite::types::Value::Text(anonymizer.anonymize(&s)),
                not_text => not_text
            })
        })?;
    }

    let schema = {
        let mut stmt = anon_places.prepare("
            SELECT name FROM sqlite_master
            WHERE type = 'table'
              AND name NOT IN ('sqlite_sequence', 'sqlite_stat1')
        ")?;
        let mut rows = stmt.query(&[])?;
        let mut tables = vec![];
        while let Some(row_or_error) = rows.next() {
            tables.push(TableInfo::for_table(row_or_error?.get("name"), &anon_places)?);
        }
        tables
    };

    for info in schema {
        let sql = info.make_update("anonymize");
        debug!("Executing sql:\n{}", sql);
        anon_places.execute(&sql, &[])?;
    }
    debug!("Clearing places url_hash");
    anon_places.execute("UPDATE moz_places SET url_hash = 0", &[])?;
    println!("Done!");

    Ok(())
}
