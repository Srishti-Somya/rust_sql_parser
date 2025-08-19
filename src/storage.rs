use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

const MEMTABLE_SIZE_LIMIT: usize = 1024 * 1024; // 1MB
const SSTABLE_SIZE_LIMIT: usize = 10 * 1024 * 1024; // 10MB

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageValue {
    Present(String),
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageEntry {
    pub key: String,
    pub value: StorageValue,
    pub timestamp: u64,
}

#[derive(Debug)]
pub struct MemTable {
    data: BTreeMap<String, StorageEntry>,
    size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            data: BTreeMap::new(),
            size: 0,
        }
    }

    pub fn insert(&mut self, key: String, value: String) {
        let entry = StorageEntry {
            key: key.clone(),
            value: StorageValue::Present(value),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        self.size += key.len() + entry.value.serialized_size();
        self.data.insert(key, entry);
    }

    pub fn delete(&mut self, key: String) {
        let entry = StorageEntry {
            key: key.clone(),
            value: StorageValue::Deleted,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        self.size += key.len() + entry.value.serialized_size();
        self.data.insert(key, entry);
    }

    pub fn get(&self, key: &str) -> Option<&StorageEntry> {
        self.data.get(key)
    }

    pub fn is_full(&self) -> bool {
        self.size >= MEMTABLE_SIZE_LIMIT
    }

    pub fn clear(&mut self) {
        self.data.clear();
        self.size = 0;
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &StorageEntry)> {
        self.data.iter()
    }
}

#[derive(Debug)]
pub struct SSTable {
    pub path: PathBuf,
    pub min_key: String,
    pub max_key: String,
    pub size: usize,
}

impl SSTable {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            min_key: String::new(),
            max_key: String::new(),
            size: 0,
        }
    }

    pub fn write_from_memtable(&mut self, memtable: &MemTable) -> io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        let mut writer = BufWriter::new(file);

        let mut entries: Vec<_> = memtable.iter().collect();
        entries.sort_by_key(|(key, _)| key.clone());

        if let Some((first_key, _)) = entries.first() {
            self.min_key = first_key.to_string();
        }
        if let Some((last_key, _)) = entries.last() {
            self.max_key = last_key.to_string();
        }

        for (_, entry) in entries {
            let line = serde_json::to_string(&entry)?;
            writeln!(writer, "{}", line)?;
            self.size += line.len() + 1; // +1 for newline
        }

        writer.flush()?;
        Ok(())
    }

    pub fn read_entries(&self) -> io::Result<Vec<StorageEntry>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                let entry: StorageEntry = serde_json::from_str(&line)?;
                entries.push(entry);
            }
        }

        Ok(entries)
    }

    pub fn get(&self, key: &str) -> io::Result<Option<StorageEntry>> {
        // For simplicity, we'll read all entries
        // In a real implementation, you'd use bloom filters and sparse indexes
        let entries = self.read_entries()?;
        Ok(entries.into_iter().find(|entry| entry.key == key))
    }
}

#[derive(Debug)]
pub struct WAL {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl WAL {
    pub fn new(data_dir: &Path) -> io::Result<Self> {
        let wal_path = data_dir.join("wal.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            path: wal_path,
            writer,
        })
    }

    pub fn log_insert(&mut self, key: &str, value: &str) -> io::Result<()> {
        let entry = StorageEntry {
            key: key.to_string(),
            value: StorageValue::Present(value.to_string()),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        let line = serde_json::to_string(&entry)?;
        writeln!(self.writer, "{}", line)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn log_delete(&mut self, key: &str) -> io::Result<()> {
        let entry = StorageEntry {
            key: key.to_string(),
            value: StorageValue::Deleted,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };
        let line = serde_json::to_string(&entry)?;
        writeln!(self.writer, "{}", line)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn clear(&mut self) -> io::Result<()> {
        let _ = self.writer.flush();
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.writer = BufWriter::new(file);
        Ok(())
    }

    pub fn replay(&self) -> io::Result<Vec<StorageEntry>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                let entry: StorageEntry = serde_json::from_str(&line)?;
                entries.push(entry);
            }
        }

        Ok(entries)
    }
}

#[derive(Debug)]
pub struct LSMStorage {
    data_dir: PathBuf,
    memtable: MemTable,
    sstables: Vec<SSTable>,
    wal: WAL,
    table_prefix: String,
}

impl LSMStorage {
    pub fn new(data_dir: &Path, table_name: &str) -> io::Result<Self> {
        fs::create_dir_all(data_dir)?;
        
        let table_dir = data_dir.join(table_name);
        fs::create_dir_all(&table_dir)?;

        let wal = WAL::new(&table_dir)?;
        let mut memtable = MemTable::new();
        let sstables = Vec::new();

        // Replay WAL to recover any data that was in MemTable
        if let Ok(entries) = wal.replay() {
            for entry in entries {
                match entry.value {
                    StorageValue::Present(value) => memtable.insert(entry.key, value),
                    StorageValue::Deleted => memtable.delete(entry.key),
                }
            }
        }

        Ok(Self {
            data_dir: table_dir,
            memtable,
            sstables,
            wal,
            table_prefix: table_name.to_string(),
        })
    }

    pub fn insert(&mut self, key: String, value: String) -> io::Result<()> {
        self.wal.log_insert(&key, &value)?;
        self.memtable.insert(key, value);

        if self.memtable.is_full() {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn delete(&mut self, key: String) -> io::Result<()> {
        self.wal.log_delete(&key)?;
        self.memtable.delete(key);

        if self.memtable.is_full() {
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> io::Result<Option<String>> {
        // First check memtable
        if let Some(entry) = self.memtable.get(key) {
            match &entry.value {
                StorageValue::Present(value) => return Ok(Some(value.clone())),
                StorageValue::Deleted => return Ok(None),
            }
        }

        // Then check SSTables (newest first)
        for sstable in self.sstables.iter().rev() {
            if let Some(entry) = sstable.get(key)? {
                match entry.value {
                    StorageValue::Present(value) => return Ok(Some(value)),
                    StorageValue::Deleted => return Ok(None),
                }
            }
        }

        Ok(None)
    }

    pub fn get_all(&mut self) -> io::Result<Vec<(String, String)>> {
        let mut result = Vec::new();
        let mut seen_keys = std::collections::HashSet::new();

        // Get from memtable first
        for (key, entry) in self.memtable.iter() {
            seen_keys.insert(key.clone());
            if let StorageValue::Present(value) = &entry.value {
                result.push((key.clone(), value.clone()));
            }
        }

        // Get from SSTables
        for sstable in &self.sstables {
            let entries = sstable.read_entries()?;
            for entry in entries {
                if !seen_keys.contains(&entry.key) {
                    seen_keys.insert(entry.key.clone());
                    if let StorageValue::Present(value) = entry.value {
                        result.push((entry.key, value));
                    }
                }
            }
        }

        result.sort_by_key(|(key, _)| key.clone());
        Ok(result)
    }

    fn flush_memtable(&mut self) -> io::Result<()> {
        if self.memtable.data.is_empty() {
            return Ok(());
        }

        let sstable_id = self.sstables.len();
        let sstable_path = self.data_dir.join(format!("sstable_{}.log", sstable_id));
        let mut sstable = SSTable::new(sstable_path);
        
        sstable.write_from_memtable(&self.memtable)?;
        self.sstables.push(sstable);
        
        self.memtable.clear();
        // Don't clear WAL - keep it for recovery

        // Simple compaction: if we have too many SSTables, merge them
        if self.sstables.len() > 3 {
            self.compact()?;
        }

        Ok(())
    }

    fn compact(&mut self) -> io::Result<()> {
        if self.sstables.len() < 2 {
            return Ok(());
        }

        // Merge all SSTables into one
        let mut all_entries = Vec::new();
        
        for sstable in &self.sstables {
            let entries = sstable.read_entries()?;
            all_entries.extend(entries);
        }

        // Sort by key and timestamp (newest wins)
        all_entries.sort_by(|a, b| {
            a.key.cmp(&b.key).then(b.timestamp.cmp(&a.timestamp))
        });

        // Remove duplicates, keeping the newest
        let mut unique_entries = Vec::new();
        let mut last_key = None;
        
        for entry in all_entries {
            if last_key.as_ref() != Some(&entry.key) {
                unique_entries.push(entry.clone());
                last_key = Some(entry.key);
            }
        }

        // Write to new SSTable
        let new_sstable_path = self.data_dir.join("sstable_compacted.log");
        let mut new_sstable = SSTable::new(new_sstable_path);
        
        // Create a temporary memtable to write the compacted data
        let mut temp_memtable = MemTable::new();
        for entry in unique_entries {
            match entry.value {
                StorageValue::Present(value) => temp_memtable.insert(entry.key, value),
                StorageValue::Deleted => temp_memtable.delete(entry.key),
            }
        }
        
        new_sstable.write_from_memtable(&temp_memtable)?;

        // Remove old SSTables and replace with compacted one
        for sstable in &self.sstables {
            let _ = fs::remove_file(&sstable.path);
        }
        
        self.sstables.clear();
        self.sstables.push(new_sstable);

        Ok(())
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.flush_memtable()?;
        Ok(())
    }
}

// Helper trait for serialization size calculation
trait SerializedSize {
    fn serialized_size(&self) -> usize;
}

impl SerializedSize for StorageValue {
    fn serialized_size(&self) -> usize {
        match self {
            StorageValue::Present(value) => value.len(),
            StorageValue::Deleted => 8, // "deleted" string length
        }
    }
} 