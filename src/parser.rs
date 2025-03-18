use crate::tokenizer::{Token, TokenKind};
use crate::ast::{SqlStatement, SelectStatement};

pub fn parse(tokens: &[Token]) -> Result<SqlStatement, String> {
    if tokens.is_empty() || tokens[0].kind != TokenKind::Select {
        return Err("Expected SELECT statement".to_string());
    }

    let mut iter = tokens.iter();
    iter.next(); // Skip "SELECT"

    let mut columns = Vec::new();
    while let Some(token) = iter.next() {
        match token.kind {
            TokenKind::From => break,
            TokenKind::Identifier => columns.push(token.value.clone()),
            _ => return Err("Unexpected token in SELECT clause".to_string()),
        }
    }

    if let Some(from_token) = iter.next() {
        if from_token.kind != TokenKind::From {
            return Err("Expected FROM keyword".to_string());
        }
    }

    let table = match iter.next() {
        Some(token) if token.kind == TokenKind::Identifier => token.value.clone(),
        _ => return Err("Expected table name".to_string()),
    };

    let where_clause = if let Some(where_token) = iter.next() {
        if where_token.kind == TokenKind::Where {
            Some(iter.next().unwrap().value.clone())
        } else {
            None
        }
    } else {
        None
    };

    Ok(SqlStatement::Select(SelectStatement { columns, table, where_clause }))
}
