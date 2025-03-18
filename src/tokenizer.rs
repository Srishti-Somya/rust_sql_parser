use regex::Regex;

#[derive(Debug, PartialEq)]
pub enum TokenKind {
    Select, From, Where, Identifier, Number, Operator, StringLiteral, EOF
}

#[derive(Debug)]
pub struct Token {
    pub kind: TokenKind,
    pub value: String,
}

pub fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let re = Regex::new(r"(?i)\b(select|from|where)\b|\d+|'[^']*'|[\w_]+|[=<>!]+").unwrap();

    for cap in re.find_iter(input) {
        let value = cap.as_str().to_string();
        let kind = match value.to_lowercase().as_str() {
            "select" => TokenKind::Select,
            "from" => TokenKind::From,
            "where" => TokenKind::Where,
            _ if value.parse::<i64>().is_ok() => TokenKind::Number,
            _ if value.starts_with("'") => TokenKind::StringLiteral,
            _ => TokenKind::Identifier,
        };

        tokens.push(Token { kind, value });
    }

    tokens.push(Token { kind: TokenKind::EOF, value: "".to_string() });
    tokens
}
