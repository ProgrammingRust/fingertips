use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::mem;
use std::path::{Path, PathBuf};

use crate::tmp::TmpDir;
use crate::read::IndexFileReader;
use crate::write::IndexFileWriter;

pub struct FileMerge {
    output_dir: PathBuf,
    tmp_dir: TmpDir,
    stacks: Vec<Vec<PathBuf>>
}

// How many files to merge at a time, at most.
const NSTREAMS: usize = 8;

const MERGED_FILENAME: &'static str = "index.dat";

impl FileMerge {
    pub fn new(output_dir: &Path) -> FileMerge {
        FileMerge {
            output_dir: output_dir.to_owned(),
            tmp_dir: TmpDir::new(output_dir.to_owned()),
            stacks: vec![]
        }
    }

    pub fn add_file(&mut self, mut file: PathBuf) -> io::Result<()> {
        let mut level = 0;
        loop {
            if level == self.stacks.len() {
                self.stacks.push(vec![]);
            }
            self.stacks[level].push(file);
            if self.stacks[level].len() < NSTREAMS {
                break;
            }
            let (filename, out) = self.tmp_dir.create()?;
            let mut to_merge = vec![];
            mem::swap(&mut self.stacks[level], &mut to_merge);
            merge_streams(to_merge, out)?;
            file = filename;
            level += 1;
        }
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<()> {
        let mut tmp = Vec::with_capacity(NSTREAMS);
        for stack in self.stacks {
            for file in stack.into_iter().rev() {
                tmp.push(file);
                if tmp.len() == NSTREAMS {
                    merge_reversed(&mut tmp, &mut self.tmp_dir)?;
                }
            }
        }

        if tmp.len() > 1 {
            merge_reversed(&mut tmp, &mut self.tmp_dir)?;
        }
        assert!(tmp.len() <= 1);
        match tmp.pop() {
            Some(last_file) =>
                fs::rename(last_file, self.output_dir.join(MERGED_FILENAME)),
            None =>
                Err(io::Error::new(io::ErrorKind::Other,
                                   "no documents were parsed or none contained any words"))
        }
    }
}

fn merge_streams(files: Vec<PathBuf>, out: BufWriter<File>)
    -> io::Result<()>
{
    let mut streams: Vec<IndexFileReader> =
        files.into_iter()
            .map(IndexFileReader::open_and_delete)
            .collect::<io::Result<_>>()?;

    let mut output = IndexFileWriter::new(out)?;

    let mut point: u64 = 0;
    let mut count = streams.iter().filter(|s| s.peek().is_some()).count();
    while count > 0 {
        let mut term = None;
        let mut nbytes = 0;
        let mut df = 0;
        for s in &streams {
            match s.peek() {
                None => {}
                Some(entry) => {
                    if term.is_none() || entry.term < *term.as_ref().unwrap() {
                        term = Some(entry.term.clone()); // XXX LAME clone
                        nbytes = entry.nbytes;
                        df = entry.df;
                    } else if entry.term == *term.as_ref().unwrap() {
                        nbytes += entry.nbytes;
                        df += entry.df;
                    }
                }
            }
        }
        let term = term.expect("bug in algorithm!");

        for s in &mut streams {
            if s.is_at(&term) {
                s.move_entry_to(&mut output)?;
                if s.peek().is_none() {
                    count -= 1;
                }
            }
        }
        output.write_contents_entry(term, df, point, nbytes as u64);
        point += nbytes as u64;
    }

    assert!(streams.iter().all(|s| s.peek().is_none()));
    output.finish()
}

fn merge_reversed(filenames: &mut Vec<PathBuf>, tmp_dir: &mut TmpDir) -> io::Result<()> {
    filenames.reverse();
    let (merged_filename, out) = tmp_dir.create()?;
    let mut to_merge = Vec::with_capacity(NSTREAMS);
    mem::swap(filenames, &mut to_merge);
    merge_streams(to_merge, out)?;
    filenames.push(merged_filename);
    Ok(())
}
