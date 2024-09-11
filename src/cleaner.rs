use arrow::{
    array::{
        ArrayRef, Float32Builder, Int32Builder, LargeStringBuilder, ListBuilder, RecordBatch,
        StringBuilder, StructArray,
    },
    datatypes::{DataType, Field},
};
use parquet::{
    arrow::ArrowWriter,
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};
use tokio::{
    sync::{mpsc, Semaphore},
    task::JoinSet,
};
use walkdir::{DirEntry, WalkDir};

use crate::oscar::Document;

// Converts OscarBuilder` into `StructArray`
#[derive(Debug, Default)]
struct OscarBuilder {
    warc_record_id: StringBuilder,
    warc_refers_to: StringBuilder,
    warc_target_uri: StringBuilder,
    warc_date: StringBuilder,

    content: LargeStringBuilder,

    identified_doc_lang: StringBuilder,
    identified_doc_prob: Float32Builder,

    sentences_langs: ListBuilder<StringBuilder>,
    sentences_probs: ListBuilder<Float32Builder>,

    warc_identified_content_language: ListBuilder<StringBuilder>,

    harmful_pp: Float32Builder,
    tlsh: StringBuilder,
    quality_warnings: ListBuilder<StringBuilder>,
    categories: ListBuilder<StringBuilder>,

    warc_type: StringBuilder,
    content_length: Int32Builder,
    warc_block_digest: StringBuilder,
    content_type: StringBuilder,
}

impl OscarBuilder {
    fn append(&mut self, document: &Document) {
        self.warc_record_id
            .append_option(document.warc_headers.warc_record_id.as_ref());
        self.warc_refers_to
            .append_option(document.warc_headers.warc_refers_to.as_ref());
        self.warc_target_uri
            .append_option(document.warc_headers.warc_target_uri.as_ref());
        self.warc_date
            .append_option(document.warc_headers.warc_date.as_ref());

        self.content.append_value(document.content.as_str());

        self.identified_doc_lang
            .append_value(document.metadata.identification.label.as_str());

        self.identified_doc_prob
            .append_value(document.metadata.identification.prob);

        let mut senteces_langs: Vec<Option<String>> = vec![];
        let mut sentences_probs: Vec<Option<f32>> = vec![];

        for sentence in document.metadata.sentence_identifications.iter() {
            senteces_langs.push(sentence.as_ref().map(|s| s.label.clone()));
            sentences_probs.push(sentence.as_ref().map(|s| s.prob));
        }

        self.sentences_langs.append_value(senteces_langs);
        self.sentences_probs.append_value(sentences_probs);

        let id_langs: Option<Vec<Option<String>>> = document
            .warc_headers
            .warc_identified_content_language
            .as_ref()
            .map(|s| {
                s.split(",")
                    .map(|s| Some(s.to_string()))
                    .collect::<Vec<Option<String>>>()
            });

        self.warc_identified_content_language
            .append_option(id_langs);

        self.harmful_pp.append_option(document.metadata.harmful_pp);
        self.tlsh.append_option(document.metadata.tlsh.as_ref());
        self.quality_warnings
            .append_option(document.metadata.quality_warnings.clone());
        self.categories
            .append_option(document.metadata.categories.clone());

        self.warc_type
            .append_option(document.warc_headers.warc_type.as_ref());

        let length = document
            .warc_headers
            .content_length
            .as_ref()
            .map(|s| s.parse::<i32>().unwrap_or(-1));

        self.content_length.append_option(length);
        self.warc_block_digest
            .append_option(document.warc_headers.warc_block_digest.as_ref());
        self.content_type
            .append_option(document.warc_headers.content_type.as_ref());
    }

    /// Note: returns StructArray to allow nesting within another array if desired
    fn finish(&mut self) -> StructArray {
        let warc_record_id = Arc::new(self.warc_record_id.finish()) as ArrayRef;
        let warc_record_id_field = Arc::new(Field::new("warc_record_id", DataType::Utf8, true));

        let warc_refers_to = Arc::new(self.warc_refers_to.finish()) as ArrayRef;
        let warc_refers_to_field = Arc::new(Field::new("warc_refers_to", DataType::Utf8, true));

        let warc_target_uri = Arc::new(self.warc_target_uri.finish()) as ArrayRef;
        let warc_target_uri_field = Arc::new(Field::new("warc_target_uri", DataType::Utf8, true));

        let warc_date = Arc::new(self.warc_date.finish()) as ArrayRef;
        let warc_date_field = Arc::new(Field::new("warc_date", DataType::Utf8, true));

        let content = Arc::new(self.content.finish()) as ArrayRef;
        let content_field = Arc::new(Field::new("content", DataType::LargeUtf8, false));

        let identified_doc_lang = Arc::new(self.identified_doc_lang.finish()) as ArrayRef;
        let identified_doc_lang_field =
            Arc::new(Field::new("identified_doc_lang", DataType::Utf8, false));

        let identified_doc_prob = Arc::new(self.identified_doc_prob.finish()) as ArrayRef;
        let identified_doc_prob_field =
            Arc::new(Field::new("identified_doc_prob", DataType::Float32, false));

        let sentences_langs = Arc::new(self.sentences_langs.finish()) as ArrayRef;
        let senteces_langs_value_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let sentences_langs_field = Arc::new(Field::new(
            "sentence_langs",
            DataType::List(senteces_langs_value_field),
            true,
        ));

        let sentences_probs = Arc::new(self.sentences_probs.finish()) as ArrayRef;
        let sentences_probs_value_field = Arc::new(Field::new("item", DataType::Float32, true));
        let sentences_probs_field = Arc::new(Field::new(
            "sentences_probs",
            DataType::List(sentences_probs_value_field),
            true,
        ));

        let warc_identified_content_language =
            Arc::new(self.warc_identified_content_language.finish()) as ArrayRef;
        let warc_identified_content_language_value_field =
            Arc::new(Field::new("item", DataType::Utf8, true));
        let warc_identified_content_language_field = Arc::new(Field::new(
            "warc_identified_content_language",
            DataType::List(warc_identified_content_language_value_field),
            true,
        ));

        let harmful_pp = Arc::new(self.harmful_pp.finish()) as ArrayRef;
        let harmful_pp_field = Arc::new(Field::new("harmful_pp", DataType::Float32, true));

        let tlsh = Arc::new(self.tlsh.finish()) as ArrayRef;
        let tlsh_field = Arc::new(Field::new("tlsh", DataType::Utf8, true));

        let quality_warnings = Arc::new(self.quality_warnings.finish()) as ArrayRef;
        let quality_warnings_value_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let quality_warnings_field = Arc::new(Field::new(
            "quality_warnings",
            DataType::List(quality_warnings_value_field),
            true,
        ));

        let categories = Arc::new(self.categories.finish()) as ArrayRef;
        let categories_value_field = Arc::new(Field::new("item", DataType::Utf8, true));
        let categories_field = Arc::new(Field::new(
            "categories",
            DataType::List(categories_value_field),
            true,
        ));

        let warc_type = Arc::new(self.warc_type.finish()) as ArrayRef;
        let warc_type_field = Arc::new(Field::new("warc_type", DataType::Utf8, true));

        let content_length = Arc::new(self.content_length.finish()) as ArrayRef;
        let content_length_field = Arc::new(Field::new("content_length", DataType::Int32, true));

        let warc_block_digest = Arc::new(self.warc_block_digest.finish()) as ArrayRef;
        let warc_block_digest_field =
            Arc::new(Field::new("warc_block_digest", DataType::Utf8, true));

        let content_type = Arc::new(self.content_type.finish()) as ArrayRef;
        let content_type_field = Arc::new(Field::new("content_type", DataType::Utf8, true));

        StructArray::from(vec![
            (warc_record_id_field, warc_record_id),
            (warc_refers_to_field, warc_refers_to),
            (warc_target_uri_field, warc_target_uri),
            (warc_date_field, warc_date),
            (content_field, content),
            (identified_doc_lang_field, identified_doc_lang),
            (identified_doc_prob_field, identified_doc_prob),
            (sentences_langs_field, sentences_langs),
            (sentences_probs_field, sentences_probs),
            (
                warc_identified_content_language_field,
                warc_identified_content_language,
            ),
            (harmful_pp_field, harmful_pp),
            (tlsh_field, tlsh),
            (quality_warnings_field, quality_warnings),
            (categories_field, categories),
            (warc_type_field, warc_type),
            (content_length_field, content_length),
            (warc_block_digest_field, warc_block_digest),
            (content_type_field, content_type),
        ])
    }
}

impl<'a> Extend<&'a Document> for OscarBuilder {
    fn extend<T: IntoIterator<Item = &'a Document>>(&mut self, iter: T) {
        iter.into_iter().for_each(|row| self.append(row));
    }
}

// / Converts a slice of [`Document`] to a [`RecordBatch`]
fn rows_to_batch(rows: &[Document]) -> RecordBatch {
    let mut builder = OscarBuilder::default();
    builder.extend(rows);
    RecordBatch::from(&builder.finish())
}

async fn write_to_parquet(docs: Vec<Document>, folder_path: PathBuf, lang: String, part: usize) {
    let mut path = folder_path.clone();

    path.push(lang.clone());

    if part == 0 {
        fs::create_dir_all(&path).unwrap();
    }

    let batch = rows_to_batch(&docs);
    path.push(format!("{}_part_{}.parquet", lang, part));
    let parquet = File::create(path.clone()).unwrap();

    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let mut writer = ArrowWriter::try_new(parquet, batch.schema(), Some(properties)).unwrap();
    writer.write(&batch).expect("Writing batch");
    writer.close().unwrap();
    println!("Wrote to: {}", path.display());
}

async fn language_handler(mut rx: mpsc::Receiver<Vec<Document>>, path: &PathBuf) {
    let mut docs_buffer: Vec<Document> = vec![];
    let mut part = 0;

    let mut set = JoinSet::new();

    while let Some(documents) = rx.recv().await {
        let lang_tag = documents[0].metadata.identification.label.clone();

        docs_buffer.extend(documents);

        if docs_buffer.len() >= 90_000 {
            let docs_to_write: Vec<Document> = docs_buffer.drain(0..90_000).collect();
            let write_path = path.clone();
            set.spawn(async move {
                write_to_parquet(docs_to_write, write_path, lang_tag.clone(), part.clone()).await;
            });
            part += 1;
        }
    }

    if docs_buffer.len() > 0 {
        let write_path = path.clone();
        let lang_tag = docs_buffer[0].metadata.identification.label.clone();
        set.spawn(async move {
            write_to_parquet(docs_buffer, write_path, lang_tag, part).await;
        });
    }

    while let Some(result) = set.join_next().await {
        result.unwrap();
    }
}

async fn process_file(
    file: DirEntry,
    dangerous_categories: HashSet<&str>,
    tx: mpsc::Sender<Vec<Document>>,
) {
    let reader = {
        let file = File::open(file.path()).unwrap();
        let decoder = zstd::Decoder::new(file).unwrap();
        BufReader::new(decoder)
    };

    println!("Processing file: {}", file.file_name().to_str().unwrap());

    let mut clean_docs = vec![];

    for line in reader.lines() {
        let line = line.unwrap();
        let document: Document = serde_json::from_str(&line).unwrap();
        match document.metadata.quality_warnings {
            Some(_) => {
                continue;
            }
            None => {}
        }
        match document.metadata.categories {
            Some(ref categories) => {
                let result = categories.iter().flatten().try_for_each(|category| {
                    if dangerous_categories.contains(category.as_str()) {
                        return Err(());
                    }
                    Ok(())
                });
                if result.is_err() {
                    continue;
                }
            }
            None => {}
        }
        match document.metadata.harmful_pp {
            Some(harmful_pp) => {
                if harmful_pp < 25.0 || harmful_pp > 100000.0 {
                    continue;
                }
            }
            None => {}
        }

        clean_docs.push(document);
    }

    if clean_docs.len() > 0 {
        tx.send(clean_docs).await.unwrap();
    }

    println!(
        "Finished processing file: {}",
        file.file_name().to_str().unwrap()
    );
}

pub async fn clean_snapshot(src: &PathBuf, dst: PathBuf, threads: usize) {
    let dangerous_categories = HashSet::from([
        "agressif",
        "adult",
        "cryptojacking",
        "dangerous_material",
        "phishing",
        "warez",
        "ddos",
        "hacking",
        "malware",
        "mixed_adult",
        "sect",
    ]);

    let lang_paths: Vec<DirEntry> = WalkDir::new(src)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_dir())
        .collect();

    let semaphore = Arc::new(Semaphore::new(threads));

    let mut processing_set = JoinSet::new();

    let mut writing_set = JoinSet::new();

    for lang_path in lang_paths {
        let file_paths: Vec<DirEntry> = WalkDir::new(lang_path.path())
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
            .filter(|e| e.file_name().to_str().unwrap().ends_with(".zst"))
            .collect();

        if file_paths.len() == 0 {
            continue;
        }

        let lang_tag = lang_path
            .path()
            .components()
            .last()
            .unwrap()
            .as_os_str()
            .to_str()
            .unwrap();

        println!("Processing language: {}", lang_tag);

        let (tx, rx) = mpsc::channel::<Vec<Document>>(threads);

        for file in file_paths {
            let dangerous_categories = dangerous_categories.clone();
            let semaphore = semaphore.clone();
            let tx1 = tx.clone();
            processing_set.spawn(async move {
                let _permit = semaphore.acquire().await;
                process_file(file, dangerous_categories, tx1).await;
            });
        }

        let _ = tx.downgrade();

        let dst = dst.clone();

        writing_set.spawn(async move {
            language_handler(rx, &dst).await;
        });
    }

    while let Some(result) = processing_set.join_next().await {
        result.unwrap();
    }

    while let Some(result) = writing_set.join_next().await {
        result.unwrap();
    }
}
