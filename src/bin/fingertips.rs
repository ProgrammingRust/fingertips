//! `fingertips` creates an inverted index for a set of text files.
//!
//! Most of the actual work is done by the modules `index`, `read`, `write`,
//! and `merge`.  In this file, `main.rs`, we put the pieces together in two
//! different ways.
//!
//! *   `run_single_threaded` simply does everything in one thread, in
//!     the most straightforward possible way.
//!
//! *   Then, we break the work into a five-stage pipeline so that we can run
//!     it on multiple CPUs. `run_pipeline` puts the five stages together.
//!
//! This is the `main` function that handles command-line arguments. It calls one
//! of the two functions above to do the work.

use argparse::{ArgumentParser, Collect, StoreTrue};
use fingertips::run;

fn main() {
    let mut single_threaded = false;
    let mut filenames = vec![];

    {
        let mut ap = ArgumentParser::new();
        ap.set_description("Make an inverted index for searching documents.");
        _ = ap.refer(&mut single_threaded).add_option(
            &["-1", "--single-threaded"],
            StoreTrue,
            "Do all the work on a single thread.",
        );
        _ = ap.refer(&mut filenames).add_argument(
            "filenames",
            Collect,
            "Names of files/directories to index. \
                For directories, all .txt files immediately \
                under the directory are indexed.",
        );
        ap.parse_args_or_exit();
    }

    match run(filenames, single_threaded) {
        Ok(()) => {}
        Err(err) => println!("error: {err}"),
    }
}
