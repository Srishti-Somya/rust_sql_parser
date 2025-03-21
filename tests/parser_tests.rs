#[cfg(test)]
mod tests {
    use rust_sql_parser::tokenizer::{tokenize, Token};
    use rust_sql_parser::parser::parse_sql;
    use rust_sql_parser::ast::{SQLStatement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement, WhereClause};

    #[test]
    fn test_parse_select() {
        let tokens = tokenize("SELECT name, age FROM users WHERE age > '30';").unwrap();
        let expected = SQLStatement::Select(SelectStatement {
            columns: vec!["name".to_string(), "age".to_string()],
            table: "users".to_string(),
            where_clause: Some(WhereClause {
                column: "age".to_string(),
                operator: ">".to_string(),
                value: "30".to_string(),
            }),
        });
        let result = parse_sql(tokens).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_insert() {
        let tokens = tokenize("INSERT INTO users (name, age) VALUES ('Alice', '25');").unwrap();
        let expected = SQLStatement::Insert(InsertStatement {
            table: "users".to_string(),
            columns: vec!["name".to_string(), "age".to_string()],
            values: vec!["Alice".to_string(), "25".to_string()],
        });
        let result = parse_sql(tokens).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_update() {
        let tokens = tokenize("UPDATE users SET age = '26' WHERE name = 'Alice';").unwrap();
        let expected = SQLStatement::Update(UpdateStatement {
            table: "users".to_string(),
            assignments: vec![("age".to_string(), "26".to_string())],
            where_clause: Some(WhereClause {
                column: "name".to_string(),
                operator: "=".to_string(),
                value: "Alice".to_string(),
            }),
        });
        let result = parse_sql(tokens).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_delete() {
        let tokens = tokenize("DELETE FROM users WHERE name = 'Bob';").unwrap();
        let expected = SQLStatement::Delete(DeleteStatement {
            table: "users".to_string(),
            where_clause: Some(WhereClause {
                column: "name".to_string(),
                operator: "=".to_string(),
                value: "Bob".to_string(),
            }),
        });
        let result = parse_sql(tokens).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_select_without_where() {
        let tokens = tokenize("SELECT id FROM products;").unwrap();
        let expected = SQLStatement::Select(SelectStatement {
            columns: vec!["id".to_string()],
            table: "products".to_string(),
            where_clause: None,
        });
        let result = parse_sql(tokens).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_update_multiple_assignments() {
        let tokens = tokenize("UPDATE users SET name = 'Charlie', age = '28' WHERE id = '3';").unwrap();
        let expected = SQLStatement::Update(UpdateStatement {
            table: "users".to_string(),
            assignments: vec![
                ("name".to_string(), "Charlie".to_string()),
                ("age".to_string(), "28".to_string()),
            ],
            where_clause: Some(WhereClause {
                column: "id".to_string(),
                operator: "=".to_string(),
                value: "3".to_string(),
            }),
        });
        let result = parse_sql(tokens).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_delete_without_where() {
        let tokens = tokenize("DELETE FROM logs;").unwrap();
        let expected = SQLStatement::Delete(DeleteStatement {
            table: "logs".to_string(),
            where_clause: None,
        });
        let result = parse_sql(tokens).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_insert_without_columns() {
        let tokens = tokenize("INSERT INTO users VALUES ('John', 'Doe', '30');").unwrap();
        let expected = SQLStatement::Insert(InsertStatement {
            table: "users".to_string(),
            columns: vec![],
            values: vec!["John".to_string(), "Doe".to_string(), "30".to_string()],
        });
        let result = parse_sql(tokens).unwrap();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_invalid_sql() {
        let tokens = tokenize("INVALID SQL QUERY;").unwrap();
        let result = parse_sql(tokens);
        assert!(result.is_err());
    }
}
