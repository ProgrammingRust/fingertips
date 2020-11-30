/// `fingertips` creates an inverted index for a set of text files.
///
/// Most of the actual work is done by the modules `index`, `read`, `write`,
/// and `merge`.  In this file, `main.rs`, we put the pieces together in two
/// different ways.
///
/// *   `run_single_threaded` simply does everything in one thread, in
///     the most straightforward possible way.
///
/// *   Then, we break the work into a five-stage pipeline so that we can run
///     it on multiple CPUs. `run_pipeline` puts the five stages together.
///
/// The `main` function at the end handles command-line arguments. It calls one
/// of the two functions above to do the work.

mod index;
mod read;
mod write;
mod merge;
mod tmp;

use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver};
use std::thread::{spawn, JoinHandle};
use argparse::{ArgumentParser, StoreTrue, Collect};

use crate::index::InMemoryIndex;
use crate::write::write_index_to_tmp_file;
use crate::merge::FileMerge;
use crate::tmp::TmpDir;

/// Create an inverted index for the given list of `documents`,
/// storing it in the specified `output_dir`.
fn run_single_threaded(documents: Vec<PathBuf>, output_dir: PathBuf)
    -> io::Result<()>
{
    // If all the documents fit comfortably in memory, we'll create the whole
    // index in memory.
    let mut accumulated_index = InMemoryIndex::new();

    // If not, then as memory fills up, we'll write largeish temporary index
    // files to disk, saving the temporary filenames in `merge` so that later we
    // can merge them all into a single huge file.
    let mut merge = FileMerge::new(&output_dir);

    // A tool for generating temporary filenames.
    let mut tmp_dir = TmpDir::new(&output_dir);

    // For each document in the set...
    for (doc_id, filename) in documents.into_iter().enumerate() {
        // ...load it into memory...
        let mut f = File::open(filename)?;
        let mut text = String::new();
        f.read_to_string(&mut text)?;

        // ...and add its contents to the in-memory `accumulated_index`.
        let index = InMemoryIndex::from_single_document(doc_id, text);
        accumulated_index.merge(index);
        if accumulated_index.is_large() {
            // To avoid running out of memory, dump `accumulated_index` to disk.
            let file = write_index_to_tmp_file(accumulated_index, &mut tmp_dir)?;
            merge.add_file(file)?;
            accumulated_index = InMemoryIndex::new();
        }
    }

    // Done reading documents! Save the last data set to disk, then merge the
    // temporary index files if there are more than one.
    if !accumulated_index.is_empty() {
        let file = write_index_to_tmp_file(accumulated_index, &mut tmp_dir)?;
        merge.add_file(file)?;
    }
    merge.finish()
}

/// Start a thread that loads documents from the filesystem into memory.
///
/// `documents` is a list of filenames to load.
///
/// This returns a pair of values: a receiver that receives the documents, as
/// Strings; and a `JoinHandle` that can be used to wait for this thread to
/// exit and to get the `io::Error` value if anything goes wrong.
fn start_file_reader_thread(documents: Vec<PathBuf>)
    -> (Receiver<String>, JoinHandle<io::Result<()>>)
{
    let (sender, receiver) = channel();

    let handle = spawn(move || {
        for filename in documents {
            let mut f = File::open(filename)?;
            let mut text = String::new();
            f.read_to_string(&mut text)?;

            if sender.send(text).is_err() {
                break;
            }
        }
        Ok(())
    });

    (receiver, handle)
}

/// Start a thread that tokenizes each text and converts it into an in-memory
/// index. (We assume that every document fits comfortably in memory.)
///
/// `texts` is the stream of documents from the file reader thread.
///
/// This assigns each document a number. It returns a pair of values: a
/// receiver, the sequence of in-memory indexes; and a `JoinHandle` that can be
/// used to wait for this thread to exit. This stage of the pipeline is
/// infallible (it performs no I/O, so there are no possible errors).
fn start_file_indexing_thread(texts: Receiver<String>)
    -> (Receiver<InMemoryIndex>, JoinHandle<()>)
{
    let (sender, receiver) = channel();

    let handle = spawn(move || {
        for (doc_id, text) in texts.into_iter().enumerate() {
            let index = InMemoryIndex::from_single_document(doc_id, text);
            if sender.send(index).is_err() {
                break;
            }
        }
    });

    (receiver, handle)
}

/// Start a thread that merges in-memory indexes.
///
/// `file_indexes` receives a stream of indexes from the file indexing thread.
/// These indexes typically vary a lot in size, since the input documents will
/// typically be all different sizes.
///
/// The thread created by this function merges those indexes into "large"
/// indexes and passes these large indexes on to a new channel.
///
/// This returns a pair: a receiver, the sequence of large indexes produced by
/// merging the input indexes; and a `JoinHandle` that can be used to wait for
/// this thread to exit. This stage of the pipeline is infallible (it performs
/// no I/O).
fn start_in_memory_merge_thread(file_indexes: Receiver<InMemoryIndex>)
    -> (Receiver<InMemoryIndex>, JoinHandle<()>)
{
    let (sender, receiver) = channel();

    let handle = spawn(move || {
        let mut accumulated_index = InMemoryIndex::new();
        for fi in file_indexes {
            accumulated_index.merge(fi);
            if accumulated_index.is_large() {
                if sender.send(accumulated_index).is_err() {
                    return;
                }
                accumulated_index = InMemoryIndex::new();
            }
        }
        if !accumulated_index.is_empty() {
            let _ = sender.send(accumulated_index);
        }
    });

    (receiver, handle)
}

/// Start a thread that saves large indexes to temporary files.
///
/// This thread generates a meaningless unique filename for each index in
/// `big_indexes`, saves the data, and passes the filename on to a new channel.
///
/// This returns a pair: a receiver that receives the filenames; and a
/// `JoinHandle` that can be used to wait for this thread to exit and receive
/// any I/O errors it encountered.
fn start_index_writer_thread(big_indexes: Receiver<InMemoryIndex>,
                             output_dir: &Path)
    -> (Receiver<PathBuf>, JoinHandle<io::Result<()>>)
{
    let (sender, receiver) = channel();

    let mut tmp_dir = TmpDir::new(output_dir);
    let handle = spawn(move || {
        for index in big_indexes {
            let file = write_index_to_tmp_file(index, &mut tmp_dir)?;
            if sender.send(file).is_err() {
                break;
            }
        }
        Ok(())
    });

    (receiver, handle)
}

/// Given a sequence of filenames of index data files, merge all the files
/// into a single index data file.
fn merge_index_files(files: Receiver<PathBuf>, output_dir: &Path)
    -> io::Result<()>
{
    let mut merge = FileMerge::new(output_dir);
    for file in files {
        merge.add_file(file)?;
    }
    merge.finish()
}

/// Create an inverted index for the given list of `documents`,
/// storing it in the specified `output_dir`.
///
/// On success this does exactly the same thing as `run_single_threaded`, but
/// faster since it uses multiple CPUs and keeps them busy while I/O is
/// happening.
fn run_pipeline(documents: Vec<PathBuf>, output_dir: PathBuf)
    -> io::Result<()>
{
    // Launch all five stages of the pipeline.
    let (texts,   h1) = start_file_reader_thread(documents);
    let (pints,   h2) = start_file_indexing_thread(texts);
    let (gallons, h3) = start_in_memory_merge_thread(pints);
    let (files,   h4) = start_index_writer_thread(gallons, &output_dir);
    let result = merge_index_files(files, &output_dir);

    // Wait for threads to finish, holding on to any errors that they encounter.
    let r1 = h1.join().unwrap();
    h2.join().unwrap();
    h3.join().unwrap();
    let r4 = h4.join().unwrap();

    // Return the first error encountered, if any.
    // (As it happens, h2 and h3 can't fail: those threads
    // are pure in-memory data processing.)
    r1?;
    r4?;
    result
}

/// Given some paths, generate the complete list of text files to index. We check
/// on disk whether the path is the name of a file or a directory; for
/// directories, all .txt files immediately under the directory are indexed.
/// Relative paths are fine.
///
/// It's an error if any of the `args` is not a valid path to an existing file
/// or directory.
fn expand_filename_arguments(args: Vec<String>) -> io::Result<Vec<PathBuf>> {
    let mut filenames = vec![];
    for arg in args {
        let path = PathBuf::from(arg);
        if path.metadata()?.is_dir() {
            for entry in path.read_dir()? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    filenames.push(entry.path());
                }
            }
        } else {
            filenames.push(path);
        }
    }
    Ok(filenames)
}

/// Generate an index for a bunch of text files.
fn run(filenames: Vec<String>, single_threaded: bool) -> io::Result<()> {
    let output_dir = PathBuf::from(".");
    let documents = expand_filename_arguments(filenames)?;

    if single_threaded {
        run_single_threaded(documents, output_dir)
    } else {
        run_pipeline(documents, output_dir)
    }
}

fn main() {
    let mut single_threaded = false;
    let mut filenames = vec![];

    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Make an inverted index for searching documents.");
        ap.refer(&mut single_threaded)
            .add_option(&["-1", "--single-threaded"], StoreTrue,
                        "Do all the work on a single thread.");
        ap.refer(&mut filenames)
            .add_argument("filenames", Collect,
                          "Names of files/directories to index. \
                           For directories, all .txt files immediately \
                           under the directory are indexed.");
        ap.parse_args_or_exit();
    }

    match run(filenames, single_threaded) {
        Ok(()) => {}
        Err(err) => println!("error: {}", err)
    }
}
