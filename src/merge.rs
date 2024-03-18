use std::path::{Path, PathBuf};
use std::{fmt, mem};
use std::{
    fmt::Debug,
    fs::{self, File},
};
use std::{
    fmt::Formatter,
    io::{self, BufWriter},
};

use crate::write::IndexFileWriter;
use crate::{error::FingertipsErrorKind, tmp::TmpDir};
use crate::{error::FingertipsResult, read::IndexFileReader};

pub(crate) mod constants {
    // How many files to merge at a time, at most.
    pub const NSTREAMS: usize = 8;
    pub const MERGED_FILENAME: &str = "index.dat";
}

#[derive(Clone)]
pub struct FileMerge {
    output_dir: PathBuf,
    tmp_dir: TmpDir,
    stacks: Vec<Vec<PathBuf>>,
}

impl Debug for FileMerge {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileMerge")
            .field("output_dir", &self.output_dir)
            .field("stacks", &self.stacks)
            .finish()
    }
}

impl FileMerge {
    pub fn new(output_dir: &Path) -> Self {
        Self {
            output_dir: output_dir.to_owned(),
            tmp_dir: TmpDir::new(output_dir),
            stacks: vec![],
        }
    }

    pub fn add_file(&mut self, mut file: PathBuf) -> FingertipsResult<()> {
        let mut level = 0;
        loop {
            if level == self.stacks.len() {
                self.stacks.push(vec![]);
            }
            self.stacks[level].push(file);
            if self.stacks[level].len() < constants::NSTREAMS {
                break;
            }
            let (filename, out) = self.tmp_dir.create().map_err(FingertipsErrorKind::Io)?;
            let mut to_merge = vec![];
            mem::swap(&mut self.stacks[level], &mut to_merge);
            merge_streams(to_merge, out)?;
            file = filename;
            level += 1;
        }
        Ok(())
    }

    pub fn finish(mut self) -> FingertipsResult<()> {
        let mut tmp = Vec::with_capacity(constants::NSTREAMS);
        for stack in self.stacks {
            for file in stack.into_iter().rev() {
                tmp.push(file);
                if tmp.len() == constants::NSTREAMS {
                    merge_reversed(&mut tmp, &mut self.tmp_dir)?;
                }
            }
        }

        if tmp.len() > 1 {
            merge_reversed(&mut tmp, &mut self.tmp_dir)?;
        }
        assert!(tmp.len() <= 1);

        if let Some(last_file) = tmp.pop() {
            fs::rename(last_file, self.output_dir.join(constants::MERGED_FILENAME))
                .map_err(|err| FingertipsErrorKind::Io(err).into())
        } else {
            Err(FingertipsErrorKind::from(io::Error::new(
                io::ErrorKind::Other,
                "no documents were parsed or none contained any words",
            ))
            .into())
        }
    }
}

fn merge_streams(files: Vec<PathBuf>, out: BufWriter<File>) -> FingertipsResult<()> {
    let mut streams: Vec<IndexFileReader> = files
        .into_iter()
        .map(IndexFileReader::open_and_delete)
        .map(|result| result.map_err(|err| FingertipsErrorKind::Io(err).into()))
        .collect::<FingertipsResult<_>>()?;

    let mut output = IndexFileWriter::new(out).map_err(FingertipsErrorKind::Io)?;

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
                    if term.is_none()
                        || entry.term < *term.as_ref().ok_or(FingertipsErrorKind::TermEmpty)?
                    {
                        term = Some(entry.term.clone()); // XXX LAME clone
                        nbytes = entry.nbytes;
                        df = entry.df;
                    } else if entry.term == *term.as_ref().ok_or(FingertipsErrorKind::TermEmpty)? {
                        nbytes += entry.nbytes;
                        df += entry.df;
                    }
                }
            }
        }
        let term = term.ok_or(FingertipsErrorKind::AlgorithmError)?;

        for s in &mut streams {
            if s.is_at(&term) {
                s.move_entry_to(&mut output)?;
                if s.peek().is_none() {
                    count -= 1;
                }
            }
        }
        output
            .write_contents_entry(term, df, point, nbytes)
            .map_err(FingertipsErrorKind::Io)?;

        point += nbytes;
    }

    assert!(streams.iter().all(|s| s.peek().is_none()));

    Ok(output.finish().map_err(FingertipsErrorKind::Io)?)
}

fn merge_reversed(filenames: &mut Vec<PathBuf>, tmp_dir: &mut TmpDir) -> FingertipsResult<()> {
    filenames.reverse();
    let (merged_filename, out) = tmp_dir.create().map_err(FingertipsErrorKind::Io)?;
    let mut to_merge = Vec::with_capacity(constants::NSTREAMS);
    mem::swap(filenames, &mut to_merge);
    merge_streams(to_merge, out)?;
    filenames.push(merged_filename);
    Ok(())
}
