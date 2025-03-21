// use std::iter::Peekable;
// use std::str::Chars;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Select,
    Insert,
    Update,
    Delete,
    From,
    Into,
    Values,
    Set,
    Where,
    Identifier(String),
    StringLiteral(String),
    NumberLiteral(f64),
    Equals,
    Comma,
    Asterisk,
    Semicolon,
    LeftParen,
    RightParen,
    LessThan,
    GreaterThan,
    Unknown(String),
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
                    self.position += token_length(&token); // Move position forward
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
            ' ' | '\t' | '\n' => { chars.next(); } // Ignore whitespace
            '*' => { tokens.push(Token::Asterisk); chars.next(); }
            ',' => { tokens.push(Token::Comma); chars.next(); }
            '=' => { tokens.push(Token::Equals); chars.next(); }
            ';' => { tokens.push(Token::Semicolon); chars.next(); }
            '(' => { tokens.push(Token::LeftParen); chars.next(); }
            ')' => { tokens.push(Token::RightParen); chars.next(); }
            '>' => { tokens.push(Token::GreaterThan); chars.next(); }
            '<' => { tokens.push(Token::LessThan); chars.next(); }

            '\'' => { // Handling string literals
                chars.next(); // Skip opening quote
                let mut literal = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '\'' {
                        chars.next(); // Consume closing quote
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

            '0'..='9' => { // Handling numeric literals
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

            'A'..='Z' | 'a'..='z' => { // Handling identifiers and keywords
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
                    _ => Token::Identifier(word),
                };
                tokens.push(token);
            }

            _ => { // Handle unknown characters
                tokens.push(Token::Unknown(ch.to_string()));
                chars.next();
            }
        }
    }
    Ok(tokens)
}

fn token_length(token: &Token) -> usize {
    match token {
        Token::Identifier(s) => s.len(),
        Token::StringLiteral(s) => s.len() + 2, // Includes surrounding quotes
        Token::NumberLiteral(n) => n.to_string().len(),
        Token::Unknown(s) => s.len(),
        _ => 1, // Most single-character tokens
    }
}
