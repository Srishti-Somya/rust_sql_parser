mod tokenizer;
mod ast;
mod parser;

use tokenizer::Tokenizer;
use parser::Parser;
use std::io::{self, Write};

fn main() {
    loop {
        print!("sql> ");
        io::stdout().flush().unwrap();

        let mut query = String::new();
        io::stdin().read_line(&mut query).unwrap();
        let query = query.trim();

        if query.eq_ignore_ascii_case("exit") {
            println!("Exiting SQL Parser...");
            break;
        }

        match execute_query(query) {
            Ok(statement) => println!("Parsed AST: {:?}", statement),
            Err(e) => eprintln!("Error: {}", e),
        }
    }
}

fn execute_query(query: &str) -> Result<ast::SQLStatement, String> {
    let mut tokenizer = Tokenizer::new(query);
    let tokens = tokenizer.tokenize()?;
    
    let mut parser = Parser::new(tokens);
    parser.parse()
}
