use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::{env, thread};

struct Job {
    source: PathBuf,
    target: PathBuf,
    width: usize,
}

fn parse_line<'a>(line: &'a [u8], width: usize, output: &mut Vec<u8>) -> bool {
    if line.last() != Some(&b'\n') || line.len() == 1 {
        return false;
    }
    output.clear();
    let body = &line[..line.len() - 1];
    let mut count = 0usize;
    for field in body.split(|&byte| byte == b',') {
        count += 1;
        if count > width {
            return false;
        }
        if count > 1 {
            output.push(b',');
        }
        let quoted_start = field.first() == Some(&b'"');
        let quoted_end = field.last() == Some(&b'"');
        let value = if quoted_start || quoted_end {
            if field.len() < 2 || !quoted_start || !quoted_end {
                return false;
            }
            let inner = &field[1..field.len() - 1];
            if inner.contains(&b'"') {
                return false;
            }
            inner
        } else {
            if field.contains(&b'"') {
                return false;
            }
            field
        };
        for &byte in value {
            output.push(if byte == b'\r' { 1 } else { byte });
        }
    }
    if count != width {
        return false;
    }
    output.push(b'\n');
    true
}

fn number(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) { return None; }
    let mut value = 0i64;
    for &byte in bytes { value = value.checked_mul(10)?.checked_add(i64::from(byte - b'0'))?; }
    Some(value)
}

fn leap(year: i64) -> bool { year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) }

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

fn timestamp(value: &[u8]) -> Option<i64> {
    if value.iter().all(u8::is_ascii_digit) { return number(value); }
    if value.len() != 20 && value.len() != 25 { return None; }
    if value[4] != b'-' || value[7] != b'-' || value[10] != b'T' || value[13] != b':'
        || value[16] != b':' { return None; }
    let year=number(&value[0..4])?; let month=number(&value[5..7])?; let day=number(&value[8..10])?;
    let hour=number(&value[11..13])?; let minute=number(&value[14..16])?; let second=number(&value[17..19])?;
    let month_days = match month { 1|3|5|7|8|10|12=>31, 4|6|9|11=>30, 2=>if leap(year){29}else{28}, _=>return None };
    if day < 1 || day > month_days || hour > 23 || minute > 59 || second > 59 { return None; }
    let offset = if value.len() == 20 {
        if value[19] != b'Z' { return None; } 0
    } else {
        if !matches!(value[19], b'+'|b'-') || value[22] != b':' { return None; }
        let oh=number(&value[20..22])?; let om=number(&value[23..25])?;
        if oh > 23 || om > 59 { return None; }
        let minutes=oh*60+om; if value[19]==b'+' { minutes } else { -minutes }
    };
    Some((days_from_civil(year,month,day)*86_400 + hour*3_600 + minute*60 + second - offset*60)*1_000)
}

fn normalized(row: &[u8], width: usize, output: &mut Vec<u8>) {
    output.clear();
    let body = &row[..row.len()-1];
    let fields: Vec<_> = body.split(|&byte| byte == b',').collect();
    for (at, field) in fields.iter().enumerate() {
        if at > 0 { output.push(b','); }
        let is_timestamp = (width == 7 && (at == 2 || at == 3)) || (width == 4 && at == 3);
        if is_timestamp {
            if let Some(value) = timestamp(field) { output.extend_from_slice(value.to_string().as_bytes()); }
        } else { output.extend_from_slice(field); }
    }
    output.push(b'\n');
}

fn clean(job: &Job) -> io::Result<()> {
    let input = File::open(&job.source)?;
    let output = File::create(&job.target)?;
    let mut reader = BufReader::with_capacity(4 << 20, input);
    let mut writer = BufWriter::with_capacity(4 << 20, output);
    let mut line = Vec::with_capacity(256);
    reader.read_until(b'\n', &mut line)?;
    writer.write_all(&line)?;
    let mut parsed = Vec::with_capacity(256);
    loop {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        if parse_line(&line, job.width, &mut parsed) {
            writer.write_all(&parsed)?;
        }
    }
    writer.flush()
}

fn csv_jobs(data: &Path, scratch: &Path) -> io::Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for (folder, width) in [("events", 7), ("clicks", 4), ("links", 2)] {
        let target_dir = scratch.join("clean").join(folder);
        fs::create_dir_all(&target_dir)?;
        let mut entries: Vec<_> = fs::read_dir(data.join(folder))?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().and_then(|s| s.to_str()) == Some("csv"))
            .collect();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            jobs.push(Job {
                source: entry.path(),
                target: target_dir.join(entry.file_name()),
                width,
            });
        }
    }
    Ok(jobs)
}

fn root(parent: &mut [usize], mut at: usize) -> usize {
    let mut top = at;
    while parent[top] != top {
        top = parent[top];
    }
    while parent[at] != at {
        let next = parent[at];
        parent[at] = top;
        at = next;
    }
    top
}

fn identities(scratch: &Path) -> io::Result<()> {
    let mut ids: Vec<String> = Vec::new();
    let mut parent: Vec<usize> = Vec::new();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut links: Vec<_> = fs::read_dir(scratch.join("clean/links"))?
        .filter_map(Result::ok).map(|entry| entry.path()).collect();
    links.sort();
    for path in links {
        let mut reader = BufReader::with_capacity(4 << 20, File::open(path)?);
        let mut line = String::new();
        reader.read_line(&mut line)?;
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 { break; }
            let body = line.strip_suffix('\n').unwrap_or(&line);
            let Some((left, right)) = body.split_once(',') else { continue };
            if left.is_empty() || right.is_empty() { continue; }
            let a = match index.get(left) {
                Some(&value) => value,
                None => {
                    let value = ids.len();
                    ids.push(left.to_owned()); parent.push(value);
                    index.insert(left.to_owned(), value); value
                }
            };
            let b = match index.get(right) {
                Some(&value) => value,
                None => {
                    let value = ids.len();
                    ids.push(right.to_owned()); parent.push(value);
                    index.insert(right.to_owned(), value); value
                }
            };
            let ra = root(&mut parent, a);
            let rb = root(&mut parent, b);
            if ra != rb {
                if ids[ra] < ids[rb] { parent[rb] = ra; } else { parent[ra] = rb; }
            }
        }
    }
    let mut writer = BufWriter::with_capacity(4 << 20, File::create(scratch.join("identities.csv"))?);
    for at in 0..ids.len() {
        let canonical = root(&mut parent, at);
        writeln!(writer, "{},{}", ids[at], ids[canonical])?;
    }
    writer.flush()
}

fn run() -> io::Result<()> {
    let args: Vec<_> = env::args_os().collect();
    if args.len() != 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput,
            "usage: shopping-cleaner DATA SCRATCH THREADS"));
    }
    let data = Path::new(&args[1]);
    let scratch = Path::new(&args[2]);
    let threads = args[3].to_string_lossy().parse::<usize>().unwrap_or(8).max(1);
    let jobs = csv_jobs(data, scratch)?;
    let next = AtomicUsize::new(0);
    let error = Mutex::new(None);
    thread::scope(|scope| {
        for _ in 0..threads.min(jobs.len().max(1)) {
            scope.spawn(|| loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(job) = jobs.get(index) else { break };
                if let Err(problem) = clean(job) {
                    error.lock().unwrap().get_or_insert(problem);
                    break;
                }
            });
        }
    });
    match error.into_inner().unwrap() {
        Some(problem) => Err(problem),
        None => identities(scratch),
    }
}

fn main() {
    if let Err(error) = run() {
        eprintln!("shopping-cleaner: {error}");
        std::process::exit(1);
    }
}
use std::collections::HashMap;
