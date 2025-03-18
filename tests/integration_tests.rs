#[cfg(test)]
mod tests {
    use crate::tokenizer::tokenize;
    use crate::parser::parse;
    use crate::ast::SqlStatement;

    #[test]
    fn test_select_parsing() {
        let query = "SELECT name, age FROM users WHERE age > 21";
        let tokens = tokenize(query);
        let ast = parse(&tokens).unwrap();

        match ast {
            SqlStatement::Select(stmt) => {
                assert_eq!(stmt.columns, vec!["name", "age"]);
                assert_eq!(stmt.table, "users");
                assert!(stmt.where_clause.is_some());
            },
        }
    }
}
