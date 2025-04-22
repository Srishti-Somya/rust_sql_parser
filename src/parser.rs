use crate::ast::{
    SQLStatement,
    SelectStatement,
    InsertStatement,
    UpdateStatement,
    DeleteStatement,
    WhereClause,
    CreateTableStatement,
    AlterTableStatement,
    DropTableStatement,
    AlterAction,
};
use crate::tokenizer::Token;

pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, current: 0 }
    }

    pub fn parse(&mut self) -> Result<SQLStatement, String> {
        match self.peek() {
            Some(Token::Select) => { self.advance(); self.parse_select() }
            Some(Token::Insert) => { self.advance(); self.parse_insert() }
            Some(Token::Update) => { self.advance(); self.parse_update() }
            Some(Token::Delete) => { self.advance(); self.parse_delete() }
            Some(Token::Create) => { self.advance(); self.parse_create_table() }
            Some(Token::Alter) => { self.advance(); self.parse_alter_table() }
            Some(Token::Drop)   => { self.advance(); self.parse_drop_table() } 
            _ => Err("Unexpected token at start of statement".to_string()),
        }
    }

    fn parse_create_table(&mut self) -> Result<SQLStatement, String> {
        self.expect(Token::Table)?;
        let table = self.expect_identifier("Expected table name after CREATE TABLE")?;
        self.expect(Token::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            let name = self.expect_identifier("Expected column name")?;
            let datatype = self.expect_identifier("Expected data type")?;
            columns.push((name, datatype));

            match self.peek() {
                Some(Token::Comma) => { self.advance(); }
                Some(Token::RightParen) => { self.advance(); break; }
                _ => return Err("Expected ',' or ')' after column definition".to_string()),
            }
        }

        Ok(SQLStatement::CreateTable(CreateTableStatement { table, columns }))
    }

    fn parse_alter_table(&mut self) -> Result<SQLStatement, String> {
        self.expect(Token::Table)?;
        let table = self.expect_identifier("Expected table name after ALTER TABLE")?;
    
        match self.advance() {
            Some(Token::Add) => {
                let column = self.expect_identifier("Expected column name after ADD")?;
                // Optionally skip data type
                if let Some(Token::Identifier(_)) = self.peek() {
                    self.advance();
                }
                Ok(SQLStatement::AlterTable(AlterTableStatement {
                    table,
                    action: AlterAction::AddColumn(column),
                }))
            }
            Some(Token::Drop) => {
                let column = self.expect_identifier("Expected column name after DROP")?;
                Ok(SQLStatement::AlterTable(AlterTableStatement {
                    table,
                    action: AlterAction::DropColumn(column),
                }))
            }
            Some(Token::Modify) => {
                let column = self.expect_identifier("Expected column name after MODIFY")?;
                let new_type = self.expect_identifier("Expected new data type after column name")?;
                Ok(SQLStatement::AlterTable(AlterTableStatement {
                    table,
                    action: AlterAction::ModifyColumn(column, new_type),
                }))
            }
            Some(t) => Err(format!("Unexpected token in ALTER TABLE: {:?}", t)),
            None => Err("Unexpected end of input in ALTER TABLE".to_string()),
        }
    }
    

    fn parse_drop_table(&mut self) -> Result<SQLStatement, String> {
        self.expect(Token::Table)?;
        let table = self.expect_identifier("Expected table name after DROP TABLE")?;
        Ok(SQLStatement::DropTable(DropTableStatement { table }))
    }

    fn parse_select(&mut self) -> Result<SQLStatement, String> {
        let columns = self.parse_column_list_until(Token::From)?;
        self.expect(Token::From)?;
        let table = self.expect_identifier("Expected table name after FROM")?;
        let where_clause = self.parse_optional_where_clause()?;
        Ok(SQLStatement::Select(SelectStatement {
            columns: Some(columns), 
            table,
            where_clause,
        }))
    }

    fn parse_insert(&mut self) -> Result<SQLStatement, String> {
        self.expect(Token::Into)?;
        let table = self.expect_identifier("Expected table name after INSERT INTO")?;
        self.expect(Token::LeftParen)?;
        let columns = self.parse_column_list_until(Token::RightParen)?;
        self.expect(Token::RightParen)?;
        self.expect(Token::Values)?;

        let values = self.parse_values_list()?;
        Ok(SQLStatement::Insert(InsertStatement { table, columns, values }))
    }

    fn parse_update(&mut self) -> Result<SQLStatement, String> {
        let table = self.expect_identifier("Expected table name after UPDATE")?;
        self.expect(Token::Set)?;
        let assignments = self.parse_assignments()?;
        let where_clause = self.parse_optional_where_clause()?;
        Ok(SQLStatement::Update(UpdateStatement { table, assignments, where_clause }))
    }

    fn parse_delete(&mut self) -> Result<SQLStatement, String> {
        self.expect(Token::From)?;
        let table = self.expect_identifier("Expected table name after DELETE FROM")?;
        let where_clause = self.parse_optional_where_clause()?;
        Ok(SQLStatement::Delete(DeleteStatement { table, where_clause }))
    }

    fn parse_optional_where_clause(&mut self) -> Result<Option<WhereClause>, String> {
        if let Some(Token::Where) = self.peek() {
            self.advance();
            Ok(Some(self.parse_where_clause()?))
        } else {
            Ok(None)
        }
    }

    fn parse_where_clause(&mut self) -> Result<WhereClause, String> {
        let column = self.expect_identifier("Expected column name in WHERE clause")?;
        let operator = match self.advance() {
            Some(Token::Equals) => "=".to_string(),
            Some(Token::LessThan) => "<".to_string(),
            Some(Token::GreaterThan) => ">".to_string(),
            _ => return Err("Expected comparison operator in WHERE clause".to_string()),
        };
        let value = self.expect_string_literal("Expected value in WHERE clause")?;
        Ok(WhereClause { column, operator, value })
    }

    fn parse_column_list_until(&mut self, terminator: Token) -> Result<Vec<String>, String> {
        let mut columns = Vec::new();

        if let Some(Token::Asterisk) = self.peek() {
            self.advance();
            return Ok(vec!["*".to_string()]);
        }

        loop {
            match self.peek() {
                Some(t) if *t == terminator => break,
                Some(Token::Identifier(name)) => {
                    columns.push(name.clone());
                    self.advance();
                }
                Some(Token::Comma) => {
                    self.advance();
                }
                Some(t) => return Err(format!("Unexpected token in column list: {:?}", t)),
                None => return Err("Unexpected end of input in column list".to_string()),
            }
        }

        if columns.is_empty() {
            return Err("Expected at least one column".to_string());
        }

        Ok(columns)
    }

    fn parse_values_list(&mut self) -> Result<Vec<Vec<String>>, String> {
        let mut values_list = Vec::new();
        loop {
            if self.peek() != Some(&Token::LeftParen) {
                break;
            }
            let tuple = self.parse_value_tuple()?;
            values_list.push(tuple);
            if let Some(Token::Comma) = self.peek() {
                self.advance();
            } else {
                break;
            }
        }

        if values_list.is_empty() {
            return Err("Expected at least one VALUES tuple".to_string());
        }

        Ok(values_list)
    }

    fn parse_value_tuple(&mut self) -> Result<Vec<String>, String> {
        let mut values = Vec::new();
        self.expect(Token::LeftParen)?;
        loop {
            match self.peek() {
                Some(Token::StringLiteral(val)) => {
                    values.push(val.clone());
                    self.advance();
                }
                Some(Token::Comma) => {
                    self.advance();
                }
                Some(Token::RightParen) => {
                    self.advance();
                    break;
                }
                Some(t) => return Err(format!("Unexpected token in VALUES tuple: {:?}", t)),
                None => return Err("Unexpected end of input in VALUES tuple".to_string()),
            }
        }
        if values.is_empty() {
            return Err("Empty VALUES tuple is not allowed".to_string());
        }
        Ok(values)
    }

    fn parse_assignments(&mut self) -> Result<Vec<(String, String)>, String> {
        let mut assignments = Vec::new();
        loop {
            let column = self.expect_identifier("Expected column name in SET clause")?;
            self.expect(Token::Equals)?;
            let value = self.expect_string_literal("Expected value in SET clause")?;
            assignments.push((column, value));
            if let Some(Token::Comma) = self.peek() {
                self.advance();
            } else {
                break;
            }
        }

        if assignments.is_empty() {
            return Err("Expected at least one assignment in SET clause".to_string());
        }

        Ok(assignments)
    }

    fn advance(&mut self) -> Option<Token> {
        if self.current < self.tokens.len() {
            let token = self.tokens[self.current].clone();
            self.current += 1;
            Some(token)
        } else {
            None
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.current)
    }

    fn expect(&mut self, expected: Token) -> Result<(), String> {
        match self.advance() {
            Some(t) if t == expected => Ok(()),
            Some(t) => Err(format!("Expected {:?}, but found {:?}", expected, t)),
            None => Err(format!("Expected {:?}, but reached end of input", expected)),
        }
    }

    fn expect_identifier(&mut self, error_message: &str) -> Result<String, String> {
        match self.advance() {
            Some(Token::Identifier(name)) => Ok(name.clone()),
            Some(t) => Err(format!("{} but found {:?}", error_message, t)),
            None => Err(format!("{} but reached end of input", error_message)),
        }
    }

    fn expect_string_literal(&mut self, error_message: &str) -> Result<String, String> {
        match self.advance() {
            Some(Token::StringLiteral(value)) => Ok(value.clone()),
            Some(t) => Err(format!("{} but found {:?}", error_message, t)),
            None => Err(format!("{} but reached end of input", error_message)),
        }
    }
}

pub fn parse_sql(tokens: Vec<Token>) -> Result<SQLStatement, String> {
    let mut parser = Parser::new(tokens);
    parser.parse()
}
