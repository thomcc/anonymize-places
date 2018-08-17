extern crate rusqlite;
extern crate dirs;

#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
extern crate rand;
extern crate tempfile;
extern crate url;
extern crate clap;
extern crate env_logger;


use rand::prelude::*;
use std::{process, fs, path::{Path, PathBuf}};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};


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
    let mut res = fs::read_dir(path)?
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

    // Copy `places.sqlite` into a temp file because if firefox is currently
    // open, we'll have issues reading from it.
    debug!("Copying places.sqlite to a temp directory for reading");

    // let tmp_dir = tempfile::tempdir()?;
    // let read_copy_path = tmp_dir.path().join("places.sqlite");
    // fs::copy(&read_copy_path, &profile.places_db)?;
    

    // let places = Connection::open_with_flags(&read_copy_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

    let anon_places = Connection::open_with_flags(&output_path,
        //OpenFlags::SQLITE_OPEN_CREATE |
        OpenFlags::SQLITE_OPEN_READ_WRITE)?;

    let anonymizer = Arc::new(Mutex::new(StringAnonymizer::default()));
    {
        let anonymizer = anonymizer.clone();
        anon_places.create_scalar_function("anonymize", 1, true, move |ctx| {
            let mut a = anonymizer.lock().unwrap();
            let arg = ctx.get::<String>(0)?;
            let res = a.anonymize(&arg);
            Ok(res)
        })?;
    }

    anon_places.execute_batch("
        BEGIN;
            -- TODO: anonymize should do the right thing for NULL (it's just annoying)
            UPDATE moz_origins
            SET prefix = anonymize(IFNULL(prefix, '')),
                host = anonymize(IFNULL(host, ''));

            UPDATE moz_inputhistory
            SET input = anonymize(IFNULL(input, ''));

            UPDATE moz_places
            SET url = anonymize(url),
                title = anonymize(IFNULL(title, '')),
                rev_host = anonymize(IFNULL(rev_host, '')),
                description = anonymize(IFNULL(description, '')),
                preview_image_url = anonymize(IFNULL(preview_image_url, '')),
                url_hash = 0;

            -- We don't have HASH and I don't feel like porting
            -- https://searchfox.org/mozilla-central/source/toolkit/components/places/Helpers.cpp#308
            -- to Rust.

            -- UPDATE moz_places
            -- SET url_hash = HASH(url)

            UPDATE moz_bookmarks
            SET title  = anonymize(IFNULL(title, '')),
                folder_type = anonymize(IFNULL(folder_type, ''));

            DELETE FROM moz_hosts;
            DELETE FROM moz_anno_attributes;
            DELETE FROM moz_annos;
            DELETE FROM moz_items_annos;
            DELETE FROM moz_bookmarks_deleted;
            DELETE FROM moz_keywords;
        COMMIT;
        VACUUM;
    ")?;





// table|moz_inputhistory|moz_inputhistory|4|CREATE TABLE moz_inputhistory (  place_id INTEGER NOT NULL, input LONGVARCHAR NOT NULL, use_count INTEGER, PRIMARY KEY (place_id, input))

// table|moz_hosts|moz_hosts|6|CREATE TABLE moz_hosts (  id INTEGER PRIMARY KEY, host TEXT NOT NULL UNIQUE, frecency INTEGER, typed INTEGER NOT NULL DEFAULT 0, prefix TEXT)
// table|moz_bookmarks|moz_bookmarks|8|CREATE TABLE moz_bookmarks (
//     id INTEGER PRIMARY KEY,
//     type INTEGER,
//     fk INTEGER DEFAULT NULL, parent INTEGER, position INTEGER,
//     title LONGVARCHAR,
//     keyword_id INTEGER,
//     folder_type TEXT, dateAdded INTEGER,
//     lastModified INTEGER,
//     guid TEXT,
//     syncStatus INTEGER NOT NULL DEFAULT 0,
//     syncChangeCounter INTEGER NOT NULL DEFAULT 1
// )
// table|moz_bookmarks_deleted|moz_bookmarks_deleted|9|CREATE TABLE moz_bookmarks_deleted (  guid TEXT PRIMARY KEY, dateRemoved INTEGER NOT NULL DEFAULT 0)
// table|moz_keywords|moz_keywords|11|CREATE TABLE moz_keywords (  id INTEGER PRIMARY KEY AUTOINCREMENT, keyword TEXT UNIQUE, place_id INTEGER, post_data TEXT)

// table|moz_anno_attributes|moz_anno_attributes|14|CREATE TABLE moz_anno_attributes (  id INTEGER PRIMARY KEY, name VARCHAR(32) UNIQUE NOT NULL)
// table|moz_annos|moz_annos|16|CREATE TABLE moz_annos (  id INTEGER PRIMARY KEY, place_id INTEGER NOT NULL, anno_attribute_id INTEGER, content LONGVARCHAR, flags INTEGER DEFAULT 0, expiration INTEGER DEFAULT 0, type INTEGER DEFAULT 0, dateAdded INTEGER DEFAULT 0, lastModified INTEGER DEFAULT 0)
// table|moz_items_annos|moz_items_annos|17|CREATE TABLE moz_items_annos (  id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL, anno_attribute_id INTEGER, content LONGVARCHAR, flags INTEGER DEFAULT 0, expiration INTEGER DEFAULT 0, type INTEGER DEFAULT 0, dateAdded INTEGER DEFAULT 0, lastModified INTEGER DEFAULT 0)

// table|moz_meta|moz_meta|19|CREATE TABLE moz_meta (key TEXT PRIMARY KEY, value NOT NULL) WITHOUT ROWID



    // {
    //     debug!("Copying schema");
    //     let mut stmt = places.prepare("SELECT sql FROM sqlite_master")?;
    //     let mut rows = stmt.query(&[])?;

    //     while let Some(row_or_error) = rows.next() {
    //         let row = row_or_error?;
    //         let s: String = row.get("sql");
    //         anon_places.execute(&s, &[])?;
    //     }
    // }
    /*
    {
        debug!("Anonymizing moz_origins");
        let tx = anon_places.transaction()?;
        let mut stmt = places.prepare("SELECT * FROM moz_origins")?;
        let mut rows = stmt.query(&[])?;

        while let Some(row_or_error) = rows.next() {
            let row = row_or_error?;
            let prefix = anonymizer.anonymize(&row.get::<_, String>("prefix"));
            let host = anonymizer.anonymize(&row.get::<_, String>("host"));


            tx.execute("
                INSERT INTO moz_origins(id, prefix, host, frecency) VALUES (?, ?, ?, ?)
            ", &[
                &row.get::<_, i64>("id"),
                &prefix,
                &host,
                &row.get::<_, i64>("frecency"),
            ])?;
        }
        tx.commit()?;
    }
    /*
    {
        debug!("Copying moz_origins");
        let tx = anon_places.transaction()?;
        let mut stmt = places.prepare("SELECT * FROM moz_origins")?;
        let mut rows = stmt.query(&[])?;

        while let Some(row_or_error) = rows.next() {
            let row = row_or_error?;
            let prefix = anonymizer.anonymize(&row.get::<_, String>("prefix"));
            let host = anonymizer.anonymize(&row.get::<_, String>("host"));

            tx.execute("
                INSERT INTO moz_origins(id, prefix, host, frecency) VALUES (?, ?, ?, ?)
            ", &[
                &row.get::<_, i64>("id"),
                &prefix,
                &host,
                &row.get::<_, i64>("frecency"),
            ])?;
        }
        tx.commit()?;
    }
    {
        debug!("Copying moz_places");
        let tx = anon_places.transaction()?;
        let mut stmt = places.prepare("SELECT * FROM moz_origins")?;
        let mut rows = stmt.query(&[])?;

        while let Some(row_or_error) = rows.next() {
            let row = row_or_error?;
            let id = row.get::<_, i64>("id");
            let url = anonymizer.anonymize(&row.get::<_, String>("url"));
            let title = anonymizer.anonymize(&row.get::<_, String>("title"));
            let rev_host = anonymizer.anonymize(&row.get::<_, String>("rev_host"));
            let visit_count = row.get::<_, i64>("visit_count");
            let hidden = row.get::<_, i64>("hidden");
            let typed = row.get::<_, i64>("typed");
            let frecency = row.get::<_, i64>("frecency");
            let last_visit_date = row.get::<_, i64>("last_visit_date");
            let guid = row.get::<_, i64>("guid");
            let foreign_count = row.get::<_, i64>("foreign_count");
            // let url_hash = row.get::<_, i64>("url_hash")
            let description = anonymizer.anonymize(&row.get::<_, String>("description"));
            let preview_image_url = anonymizer.anonymize(&row.get::<_, String>("preview_image_url"));
            let origin_id = row.get::<_, i64>("origin_id");
            tx.execute("
                INSERT INTO moz_origins(
                    id, url, title, rev_host, visit_count, hidden, typed, frecency,
                    last_visit_date, guid, foreign_count, description
                ) VALUES (?, ?, ?, ?)
            ", &[
                &id, 
            ])?;
        }
        tx.commit()?;
        anon_places.execute("UPDATE moz_places SET url_hash = HASH(url)")?;
    }*/
    */

/*


table|moz_places|moz_places|2|CREATE TABLE moz_places (
)

table|moz_historyvisits|moz_historyvisits|3|CREATE TABLE moz_historyvisits (  id INTEGER PRIMARY KEY, from_visit INTEGER, place_id INTEGER, visit_date INTEGER, visit_type INTEGER, session INTEGER)
table|moz_inputhistory|moz_inputhistory|4|CREATE TABLE moz_inputhistory (  place_id INTEGER NOT NULL, input LONGVARCHAR NOT NULL, use_count INTEGER, PRIMARY KEY (place_id, input))
table|moz_hosts|moz_hosts|6|CREATE TABLE moz_hosts (  id INTEGER PRIMARY KEY, host TEXT NOT NULL UNIQUE, frecency INTEGER, typed INTEGER NOT NULL DEFAULT 0, prefix TEXT)
table|moz_bookmarks|moz_bookmarks|8|CREATE TABLE moz_bookmarks (  id INTEGER PRIMARY KEY, type INTEGER, fk INTEGER DEFAULT NULL, parent INTEGER, position INTEGER, title LONGVARCHAR, keyword_id INTEGER, folder_type TEXT, dateAdded INTEGER, lastModified INTEGER, guid TEXT, syncStatus INTEGER NOT NULL DEFAULT 0, syncChangeCounter INTEGER NOT NULL DEFAULT 1)
table|moz_bookmarks_deleted|moz_bookmarks_deleted|9|CREATE TABLE moz_bookmarks_deleted (  guid TEXT PRIMARY KEY, dateRemoved INTEGER NOT NULL DEFAULT 0)
table|moz_keywords|moz_keywords|11|CREATE TABLE moz_keywords (  id INTEGER PRIMARY KEY AUTOINCREMENT, keyword TEXT UNIQUE, place_id INTEGER, post_data TEXT)

table|moz_anno_attributes|moz_anno_attributes|14|CREATE TABLE moz_anno_attributes (  id INTEGER PRIMARY KEY, name VARCHAR(32) UNIQUE NOT NULL)
table|moz_annos|moz_annos|16|CREATE TABLE moz_annos (  id INTEGER PRIMARY KEY, place_id INTEGER NOT NULL, anno_attribute_id INTEGER, content LONGVARCHAR, flags INTEGER DEFAULT 0, expiration INTEGER DEFAULT 0, type INTEGER DEFAULT 0, dateAdded INTEGER DEFAULT 0, lastModified INTEGER DEFAULT 0)
table|moz_items_annos|moz_items_annos|17|CREATE TABLE moz_items_annos (  id INTEGER PRIMARY KEY, item_id INTEGER NOT NULL, anno_attribute_id INTEGER, content LONGVARCHAR, flags INTEGER DEFAULT 0, expiration INTEGER DEFAULT 0, type INTEGER DEFAULT 0, dateAdded INTEGER DEFAULT 0, lastModified INTEGER DEFAULT 0)

table|moz_meta|moz_meta|19|CREATE TABLE moz_meta (key TEXT PRIMARY KEY, value NOT NULL) WITHOUT ROWID
table|moz_origins|moz_origins|20|CREATE TABLE moz_origins (
    id INTEGER PRIMARY KEY,
    prefix TEXT NOT NULL,
    host TEXT NOT NULL,
    frecency INTEGER NOT NULL,
    UNIQUE (prefix, host)
)


    
    */
    Ok(())
}
