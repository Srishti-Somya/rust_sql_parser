use crate::tokenizer::tokenize;
use crate::parser::Parser;
use crate::ast::SQLStatement;
use crate::executor::Database;

pub fn process_query(db: &mut Database, query: &str) -> Result<String, String> {
    // Step 1: Tokenization - Convert raw query into tokens
    let tokens = match tokenize(query) {
        Ok(tokens) => tokens,
        Err(e) => return Err(format!("Tokenization error: {}", e)),
    };
    
    // Step 2: Parsing - Convert tokens into an AST
    let mut parser = Parser::new(tokens);
    let ast: SQLStatement = parser.parse().map_err(|e| format!("Parsing error: {e}"))?;
    
    // Step 3: Execution - Execute the AST in the database engine
    let result = db.execute(ast).map_err(|e| format!("Execution error: {e}"))?;
    
    Ok(result)
}
