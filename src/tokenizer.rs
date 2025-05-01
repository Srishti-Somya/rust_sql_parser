use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Select, Insert, Update, Delete, From,
    Into, Values, Set, Where,
    Identifier(String), StringLiteral(String), NumberLiteral(f64),
    Equals, Comma, Asterisk, Semicolon, LeftParen, RightParen,
    LessThan, GreaterThan,
    Unknown(String),
    Create, Table, Alter, Add, Drop,
    Modify, Order, By, Desc, Asc, Group,
    Join, Left, Right, Full, On, Dot, Cross,
}

pub struct Tokenizer {
    input: String,
    position: usize,
}

impl Tokenizer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.to_string(),
            position: 0,
        }
    }

    pub fn next_token(&mut self) -> Option<Token> {
        if self.position >= self.input.len() {
            return None;
        }
        let remaining_input = &self.input[self.position..];
        match tokenize(remaining_input) {
            Ok(tokens) => {
                if let Some(token) = tokens.get(0).cloned() {
                    self.position += token_length(&token);
                    Some(token)
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, String> {
        tokenize(&self.input)
    }
}

pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\n' => { chars.next(); }
            '*' => { tokens.push(Token::Asterisk); chars.next(); }
            ',' => { tokens.push(Token::Comma); chars.next(); }
            '=' => { tokens.push(Token::Equals); chars.next(); }
            ';' => { tokens.push(Token::Semicolon); chars.next(); }
            '(' => { tokens.push(Token::LeftParen); chars.next(); }
            ')' => { tokens.push(Token::RightParen); chars.next(); }
            '>' => { tokens.push(Token::GreaterThan); chars.next(); }
            '<' => { tokens.push(Token::LessThan); chars.next(); }
            '.' => { tokens.push(Token::Dot); chars.next(); }

            '\'' => {
                chars.next();
                let mut literal = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '\'' {
                        chars.next();
                        break;
                    }
                    literal.push(c);
                    chars.next();
                }
                if literal.is_empty() {
                    return Err("Unterminated string literal".to_string());
                }
                tokens.push(Token::StringLiteral(literal));
            }

            '0'..='9' => {
                let mut number = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_numeric() || c == '.' {
                        number.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                match number.parse::<f64>() {
                    Ok(num) => tokens.push(Token::NumberLiteral(num)),
                    Err(_) => return Err(format!("Invalid number format: {}", number)),
                }
            }

            'A'..='Z' | 'a'..='z' => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_alphanumeric() || c == '_' {
                        word.push(c);
                        chars.next();
                    } else {
                        break;
                    }
                }
                let token = match word.to_uppercase().as_str() {
                    "SELECT" => Token::Select,
                    "INSERT" => Token::Insert,
                    "UPDATE" => Token::Update,
                    "DELETE" => Token::Delete,
                    "FROM" => Token::From,
                    "INTO" => Token::Into,
                    "VALUES" => Token::Values,
                    "SET" => Token::Set,
                    "WHERE" => Token::Where,
                    "CREATE" => Token::Create,
                    "TABLE" => Token::Table,
                    "ALTER" => Token::Alter,
                    "ADD" => Token::Add,
                    "DROP" => Token::Drop,
                    "MODIFY" => Token::Modify,
                    "ORDER" => Token::Order,
                    "BY" => Token::By,
                    "GROUP" => Token::Group,
                    "DESC" => Token::Desc,
                    "ASC" => Token::Asc,
                    "JOIN" => Token::Join,
                    "LEFT" => Token::Left,
                    "RIGHT" => Token::Right,
                    "FULL" => Token::Full,
                    "CROSS" => Token::Cross,
                    "ON" => Token::On,
                    _ => Token::Identifier(word),
                };
                tokens.push(token);
            }

            _ => {
                tokens.push(Token::Unknown(ch.to_string()));
                chars.next();
            }
        }
    }
    Ok(tokens)
}

fn keyword_str(token: &Token) -> &'static str {
    match token {
        Token::Select => "SELECT",
        Token::Insert => "INSERT",
        Token::Update => "UPDATE",
        Token::Delete => "DELETE",
        Token::From => "FROM",
        Token::Into => "INTO",
        Token::Values => "VALUES",
        Token::Set => "SET",
        Token::Where => "WHERE",
        Token::Create => "CREATE",
        Token::Table => "TABLE",
        Token::Alter => "ALTER",
        Token::Add => "ADD",
        Token::Drop => "DROP",
        Token::Modify => "MODIFY",
        Token::Order => "ORDER",
        Token::By => "BY",
        Token::Group => "GROUP",
        Token::Join => "JOIN",
        Token::Left => "LEFT",
        Token::Right => "RIGHT",
        Token::Full => "FULL",
        Token::On => "ON",
        Token::Desc => "DESC",
        Token::Asc => "ASC",
        Token::Cross => "CROSS",
        _ => "",
    }
}

fn token_length(token: &Token) -> usize {
    match token {
        Token::Identifier(s) => s.len(),
        Token::StringLiteral(s) => s.len() + 2,
        Token::NumberLiteral(n) => n.to_string().len(),
        Token::Unknown(s) => s.len(),
        _ => keyword_str(token).len().max(1),
    }
}
