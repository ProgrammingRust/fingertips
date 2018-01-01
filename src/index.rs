//! In-memory indexes.
//!
//! The first step in building the index is to index documents in memory.
//! `InMemoryIndex` can be used to do that, up to the size of the machine's
//! memory.

use std::collections::HashMap;
use byteorder::{LittleEndian, WriteBytesExt};

/// Break a string into words.
fn tokenize(text: &str) -> Vec<&str> {
    text.split(|ch: char| !ch.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect()
}

/// An in-memory index.
///
/// Of course, a real index for a large corpus of documents won't fit in
/// memory. But apart from memory constraints, this is everything you need to
/// answer simple search queries. And you can use the `read`, `write`, and
/// `merge` modules to save an in-memory index to disk and merge it with other
/// indices, producing a large index.
pub struct InMemoryIndex {
    /// The total number of words in the indexed documents.
    pub word_count: usize,

    /// For every term that appears in the index, the list of all search hits
    /// for that term (i.e. which documents contain that term, and where).
    ///
    /// It's possible for an index to be "sorted by document id", which means
    /// that for every `Vec<Hit>` in this map, the `Hit` elements all have
    /// distinct document ids (the first u32) and the `Hit`s are arranged by
    /// document id in increasing order. This is handy for some algorithms you
    /// might want to run on the index, so we preserve this property wherever
    /// possible.
    pub map: HashMap<String, Vec<Hit>>
}

/// A `Hit` indicates that a particular document contains some term, how many
/// times it appears, and at what offsets (that is, the word count, from the
/// beginning of the document, of each place where the term appears).
///
/// The buffer contains all the hit data in binary form, little-endian. The
/// first u32 of the data is the document id. The remaining [u32] are offsets.
pub type Hit = Vec<u8>;

impl InMemoryIndex {
    /// Create a new, empty index.
    pub fn new() -> InMemoryIndex {
        InMemoryIndex {
            word_count: 0,
            map: HashMap::new()
        }
    }

    /// Index a single document.
    ///
    /// The resulting index contains exactly one `Hit` per term.
    pub fn from_single_document(document_id: usize, text: String) -> InMemoryIndex {
        let document_id = document_id as u32;
        let mut index = InMemoryIndex::new();

        let text = text.to_lowercase();
        let tokens = tokenize(&text);
        for (i, token) in tokens.iter().enumerate() {
            let hits =
                index.map
                .entry(token.to_string())
                .or_insert_with(|| {
                    let mut hits = Vec::with_capacity(4 + 4);
                    hits.write_u32::<LittleEndian>(document_id).unwrap();
                    vec![hits]
                });
            hits[0].write_u32::<LittleEndian>(i as u32).unwrap();
            index.word_count += 1;
        }

        if document_id % 100 == 0 {
            println!("indexed document {}, {} bytes, {} words", document_id, text.len(), index.word_count);
        }

        index
    }

    /// Add all search hits from `other` to this index.
    ///
    /// If both `*self` and `other` are sorted by document id, and all document
    /// ids in `other` are greater than every document id in `*self`, then
    /// `*self` remains sorted by document id after merging.
    pub fn merge(&mut self, other: InMemoryIndex) {
        for (term, hits) in other.map {
            self.map.entry(term)
                .or_insert_with(|| vec![])
                .extend(hits)
        }
        self.word_count += other.word_count;
    }

    /// True if this index contains no data.
    pub fn is_empty(&self) -> bool {
        self.word_count == 0
    }

    /// True if this index is large enough that we should dump it to disk rather
    /// than keep adding more data to it.
    pub fn is_large(&self) -> bool {
        // This depends on how much memory your computer has, of course.
        const REASONABLE_SIZE: usize = 100_000_000;
        self.word_count > REASONABLE_SIZE
    }
}
