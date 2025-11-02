use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Result, Write},
    path::{Path, PathBuf},
};

use crate::storage::sstable::TableId;

pub struct Manifest {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl Manifest {
    pub fn new(path: PathBuf) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            path,
        })
    }

    pub fn record_add_table(&mut self, table_id: TableId) -> Result<()> {
        writeln!(self.writer, "add {table_id}")?;
        self.sync()
    }

    pub fn record_remove_table(&mut self, table_id: TableId) -> Result<()> {
        writeln!(self.writer, "remove {table_id}")?;
        self.sync()
    }

    pub fn replay_manifest(&mut self) -> Result<Vec<u64>> {
        let file = std::fs::File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut active = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let parts: Vec<_> = line.split_whitespace().collect();
            match parts.as_slice() {
                ["add", id] => {
                    let id: u64 = id.parse().unwrap();
                    active.push(id);
                }
                ["remove", id] => {
                    let id: u64 = id.parse().unwrap();
                    active.retain(|&x| x != id);
                }
                _ => {}
            }
        }
        Ok(active)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}

pub fn current_path(data_dir: &Path) -> PathBuf {
    data_dir.join("CURRENT")
}

/// Fsyncs the parent directory of the given path.
pub fn fsync_dir(path: &Path) -> Result<()> {
    if let Some(dir) = path.parent() {
        let dfile = File::open(dir)?;
        dfile.sync_all()?;
    }
    Ok(())
}

/// Atomically writes CURRENT to point to the given manifest name.
pub fn write_current_atomic(data_dir: &Path, manifest_name: &str) -> Result<()> {
    let current = current_path(data_dir);
    let tmp = data_dir.join("CURRENT.tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        writeln!(f, "{manifest_name}")?;
        f.flush()?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &current)?;
    fsync_dir(&current)?;
    Ok(())
}

/// Reads CURRENT to get the active manifest name; if CURRENT doesn't exist,
/// initializes it to the provided initial manifest name and creates that file.
pub fn read_current_or_init(data_dir: &Path, initial_manifest_name: &str) -> Result<String> {
    let current = current_path(data_dir);
    if current.exists() {
        let mut s = String::new();
        let mut f = File::open(&current)?;
        use std::io::Read;
        f.read_to_string(&mut s)?;
        return Ok(s.trim().to_string());
    }

    let manifest_path = data_dir.join(initial_manifest_name);
    if !manifest_path.exists() {
        let _ = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&manifest_path)?;
    }
    write_current_atomic(data_dir, initial_manifest_name)?;
    Ok(initial_manifest_name.to_string())
}

/// Opens a Manifest for appending using a manifest file name resolved under the data dir.
pub fn open_manifest_append(data_dir: &Path, manifest_name: &str) -> Result<Manifest> {
    Manifest::new(data_dir.join(manifest_name))
}
