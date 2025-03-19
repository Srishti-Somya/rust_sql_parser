// mod tokenizer;
// mod parser;
use rust_sql_parser::{tokenizer::tokenize, parser::parse};

fn main() {
    let query = "SELECT name FROM users WHERE age > 30";
    let tokens = tokenize(query);
    let ast = parse(&tokens);

    match ast {
        Ok(parsed) => println!("{:#?}", parsed),
        Err(err) => eprintln!("Error: {}", err),
    }
}
