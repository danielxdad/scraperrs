use std::{collections::{VecDeque}, time::{Duration, Instant}, error::Error, ops::{Div, Rem}, fs, io};
use serde::Serialize;
use scraper::{Html, Selector};
use clap::Parser;
use reqwest;
use csv;

#[derive(Parser, Debug)]
#[command(author, version)]
struct Args {
    #[arg(short, long)]
    url: String,

    #[arg(short, long, default_value="stdout")]
    csv: String,

    #[arg(short, long, default_value="0", help="Maximum number of records to get, 0: unlimited.")]
    max_records: usize,

    #[arg(short, long, default_value="30", help="Maximum request timeout.")]
    timeout: usize,

    #[arg(short, long, default_value="3", help="Maximum retries on timeout.")]
    retries_on_timeout: usize,
}

#[derive(Debug, Serialize)]
struct Enterprise {
    name: String,
    address: String,
    phone: String,
    email: String,
    contact_person: String
}

impl Enterprise {
    fn new() -> Self {
        Self {
            name: String::new(),
            address: String::new(),
            phone: String::new(),
            email: String::new(),
            contact_person: String::new()
        }
    }
}

static CSV_COLUMNS_LABALS: [&str; 5] = ["Nombre", "Domicilio", "Teléfono", "Correo electrónico", "Persona de contacto"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let mut urls_to_visit:VecDeque<String> = vec![args.url.clone()].into();
    let mut url_visited: Vec<String> = vec![];
    let mut enterprises: Vec<Enterprise> = vec![];
    let mut csv_writer: csv::Writer<Box<dyn io::Write>>;
    let begin = Instant::now();

    csv_writer = csv::WriterBuilder::new()
        .has_headers(false)
        .double_quote(true)
        .quote_style(csv::QuoteStyle::Always)
        .from_writer({
            if args.csv == "stdout" {
                Box::new(io::stdout())
            } else {
                Box::new(fs::File::create(args.csv)?)
            }
        });

    while urls_to_visit.len() > 0 {
        let url = urls_to_visit.pop_front().unwrap();

        match scrap_url(&url, args.timeout, args.retries_on_timeout).await {
            Ok(body) => {
                let mut links = Vec::new();
                links.append(&mut extract_pagination_links(&body));
                links.append(&mut extract_enterprise_links(&body));

                for link in links {
                    if !url_visited.contains(&link) && !urls_to_visit.contains(&link){
                        urls_to_visit.push_back(link);
                    }
                }

                if let Some(enterprise) = extract_enterprise_data(&body) {
                    enterprises.push(enterprise);
                }
            }
            Err(err) => {
                eprintln!("ERROR on \"{}\": {:?}\n", url, err);
            }
        }

        url_visited.push(url);

        if urls_to_visit.len() > 0 {
            let q = url_visited.len() as f32 / (url_visited.len() + urls_to_visit.len()) as f32 * 100f32;
            let elapse = Instant::now() - begin;
            let minutes = elapse.as_secs().div(60);
            let seconds = elapse.as_secs().rem(60);
            eprint!(
                "Done {}/{} ({:.2}%) URLs, found {} enterprises on {}m / {}s\t\t\r",
                url_visited.len(),
                url_visited.len() + urls_to_visit.len(),
                &q,
                enterprises.len(),
                minutes,
                seconds
            );
        }

        if args.max_records > 0 && enterprises.len() >= args.max_records {
            eprintln!("\nReached maximum number of records for scrapping: {}", args.max_records);
            break;
        }
    }

    if enterprises.len() > 0 {
        csv_writer.write_record(&CSV_COLUMNS_LABALS).expect("Error writing to CSV file.");
        for ent in enterprises {
            csv_writer.serialize(ent).expect("Error writing to CSV file.");
        }
    }

    Ok(())
}

async fn scrap_url(url: &String, timeout: usize, retries_on_timeout: usize) ->  Result<String, Box<dyn Error>> {
    let client = reqwest::Client::new();

    assert!(timeout > 0);
    assert!(retries_on_timeout > 0);

    for _ in 0..retries_on_timeout {
        let future = client
            .get(url)
            .header("User-Agent", "Mozilla 5.0")
            .timeout(Duration::from_secs(timeout as u64))
            .send();
        
        match future.await {
            Ok(response) => return Ok(response.text().await.unwrap()),
            Err(err) if err.is_timeout() => continue,
            Err(err) => return Err(Box::new(err))
        }
    }

    Err(
        Box::new(
            io::Error::new(
                io::ErrorKind::TimedOut,
                "Timeout"
            )
        )
    )
}

fn extract_pagination_links (body: &String) -> Vec<String> {
    let doc = Html::parse_document(body);
    let ul_paginator_selector = Selector::parse(r#"ul[class="pager lfr-pagination-buttons"]"#).unwrap();
    let a_paginator_selector = Selector::parse(r#"a"#).unwrap();
    let mut links = vec![];

    for ul in doc.select(&ul_paginator_selector) {
        for a in ul.select(&a_paginator_selector) {
            let href = a.value().attr("href").unwrap_or("");

            if ["http://", "https://"].map(|p| href.starts_with(p))
                .into_iter()
                .reduce(|acc, e| acc || e)
                .unwrap()
            {
                links.push(href.to_string());
            }
        }
    }

    links
}

fn extract_enterprise_links (body: &String) -> Vec<String> {
    let doc = Html::parse_document(body);
    let a_selector = Selector::parse(r#"a[class="lm"]"#).unwrap();
    let mut links = vec![];

    for a in doc.select(&a_selector) {
        let href = a.value().attr("href").unwrap_or("");

        if ["http://", "https://"].map(|p| href.starts_with(p))
            .into_iter()
            .reduce(|acc, e| acc || e)
            .unwrap()
        {
            links.push(href.to_string());
        }
    }

    links
}

fn extract_enterprise_data (body: &String) -> Option<Enterprise> {
    let doc = Html::parse_document(body);
    let card_selector = Selector::parse(r#"div[class="socios-panel-lat"]"#).unwrap();
    let name_selector = Selector::parse(r#"h2[class="tit-soc"]"#).unwrap();
    let description_selector = Selector::parse(r#"div[class="socios-descripcion"]"#).unwrap();

    if let Some(card) = doc.select(&card_selector).collect::<Vec<_>>().first() {
        let mut enterprise = Enterprise::new();

        enterprise.name = String::from(card
            .select(&name_selector)
            .map(|e| e.text().collect::<Vec<_>>().join(""))
            .collect::<String>().trim());
        
        for node in card.select(&description_selector) {
            let mut text = node.text().collect::<Vec<_>>().join("");

            if let Some(index)= text.find(&"Domicilio") {
                enterprise.address = String::from(text.drain(index + "Domicilio".len()..).collect::<String>().trim());
            }

            if let Some(index)= text.find(&"Teléfono") {
                enterprise.phone = String::from(text.drain(index + "Teléfono".len()..).collect::<String>().trim());
            }

            if let Some(index)= text.find(&"Correo electrónico") {
                enterprise.email = String::from(text.drain(index + "Correo electrónico".len()..).collect::<String>().trim());
            }

            if let Some(index)= text.find(&"Persona de contacto") {
                enterprise.contact_person = String::from(text.drain(index + "Persona de contacto".len()..).collect::<String>().trim());
            }
        }

        return Some(enterprise);
    }

    None
}
