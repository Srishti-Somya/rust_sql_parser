#[derive(Debug)]
pub enum SqlStatement {
    Select(SelectStatement),
}

#[derive(Debug)]
pub struct SelectStatement {
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<String>,
}

