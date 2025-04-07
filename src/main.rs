mod tokenizer;
mod parser;
mod ast;
mod executor;

use tokenizer::Tokenizer;
use parser::Parser;
use executor::Database;
use std::io::{self, Write};

fn main() {
    let mut db = Database::new(); // In-memory DB instance

    loop {
        print!("sql> ");
        io::stdout().flush().unwrap();

        let mut query = String::new();
        io::stdin().read_line(&mut query).unwrap();
        let query = query.trim();

        if query.eq_ignore_ascii_case("exit") {
            println!("👋 Exiting SQL Parser...");
            break;
        }

        match execute_query(query) {
            Ok(statement) => {
                match db.execute(statement) {
                    Ok(result) => println!("{}", result),
                    Err(e) => eprintln!(" Execution error: {}", e),
                }
            },
            Err(e) => eprintln!(" Parse error: {}", e),
        }
    }
}

fn execute_query(query: &str) -> Result<ast::SQLStatement, String> {
    let mut tokenizer = Tokenizer::new(query);
    let tokens = tokenizer.tokenize()?;

    let mut parser = Parser::new(tokens);
    parser.parse()
}
