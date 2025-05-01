#[derive(Debug, Clone, PartialEq)]
pub enum SQLStatement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    AlterTable(AlterTableStatement), 
    DropTable(DropTableStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub columns: Vec<ColumnExpr>, // NOT Option<>
    pub table: String,
    pub where_clause: Option<WhereClause>,
    pub order_by: Option<OrderByClause>,
    pub group_by: Option<Vec<String>>,
    pub having: Option<HavingClause>,
    pub join: Option<JoinClause>,
}


#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<(String, String)>,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub column: String,
    pub operator: String,
    pub value: String,
}
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub table: String,
    pub columns: Vec<(String, String)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterAction {
    AddColumn(String),
    DropColumn(String),
    ModifyColumn(String, String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    pub table: String,
    pub action: AlterAction,
}


#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub table: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByClause {
    pub column: String,
    pub descending: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnExpr {
    Column(String),
    Count(String),
    Sum(String),
    Avg(String),
    Min(String),
    Max(String),
    CountAll,
    All,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HavingClause {
    pub column_expr: ColumnExpr,
    pub operator: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}
#[derive(Debug, Clone, PartialEq)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    pub on_left: String,
    pub on_right: String,
}


