use crate::ast::{SQLStatement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement, CreateTableStatement, AlterTableStatement};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Database {
    tables: HashMap<String, Vec<HashMap<String, String>>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn execute(&mut self, statement: SQLStatement) -> Result<String, String> {
        match statement {
            SQLStatement::Select(stmt) => self.execute_select(&stmt),
            SQLStatement::Insert(stmt) => self.execute_insert(stmt),
            SQLStatement::Update(stmt) => self.execute_update(stmt),
            SQLStatement::Delete(stmt) => self.execute_delete(stmt),
            SQLStatement::CreateTable(stmt) => self.execute_create_table(stmt),
            SQLStatement::AlterTable(stmt) => self.execute_alter_table(stmt),
        }
    }

    fn execute_select(&self, stmt: &SelectStatement) -> Result<String, String> {
        let table = self.tables.get(&stmt.table)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table))?;

        let filtered_rows: Vec<&HashMap<String, String>> = table.iter()
            .filter(|row| {
                stmt.where_clause.as_ref().map_or(true, |where_clause| {
                    row.get(&where_clause.column).map_or(false, |value| value == &where_clause.value)
                })
            })
            .collect();

        if filtered_rows.is_empty() {
            return Err("No matching rows found".to_string());
        }

        // Handle SELECT *
        let selected_columns: Vec<String> = match &stmt.columns {
            Some(cols) if cols.len() == 1 && cols[0] == "*" => {
                filtered_rows[0].keys().cloned().collect()
            }
            Some(cols) => cols.clone(),
            None => return Err("No columns specified in SELECT statement".to_string()),
        };

        // Build result string
        let mut result = String::new();
        result.push_str(&selected_columns.join(" | "));
        result.push('\n');
        result.push_str(&"-".repeat(selected_columns.join(" | ").len()));
        result.push('\n');

        for row in filtered_rows {
            let values: Vec<String> = selected_columns.iter()
                .map(|col| row.get(col).cloned().unwrap_or_default())
                .collect();
            result.push_str(&values.join(" | "));
            result.push('\n');
        }

        Ok(result)
    }

    fn execute_insert(&mut self, stmt: InsertStatement) -> Result<String, String> {
        let table = self.tables.entry(stmt.table.clone()).or_insert_with(Vec::new);

        if stmt.columns.len() != stmt.values[0].len() {
            return Err("Column count does not match value count".to_string());
        }

        let row_values = stmt.values.first().expect("No values provided").clone();

        let new_row: HashMap<String, String> = stmt.columns.into_iter()
            .zip(row_values.into_iter())
            .collect();

        table.push(new_row);
        Ok("✅ Insert successful".to_string())
    }

    fn execute_update(&mut self, stmt: UpdateStatement) -> Result<String, String> {
        let table = self.tables.get_mut(&stmt.table)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table))?;

        let mut updated = 0;

        for row in table.iter_mut() {
            if stmt.where_clause.as_ref().map_or(true, |where_clause| {
                row.get(&where_clause.column).map_or(false, |value| value == &where_clause.value)
            }) {
                for (column, value) in &stmt.assignments {
                    row.insert(column.clone(), value.clone());
                }
                updated += 1;
            }
        }

        if updated > 0 {
            Ok(format!("✅ Updated {} row(s)", updated))
        } else {
            Err("No rows updated".to_string())
        }
    }

    fn execute_delete(&mut self, stmt: DeleteStatement) -> Result<String, String> {
        let table = self.tables.get_mut(&stmt.table)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table))?;

        let original_len = table.len();

        table.retain(|row| {
            stmt.where_clause.as_ref().map_or(true, |where_clause| {
                row.get(&where_clause.column).map_or(true, |value| value != &where_clause.value)
            })
        });

        let deleted = original_len - table.len();

        if deleted > 0 {
            Ok(format!("🗑️ Deleted {} row(s)", deleted))
        } else {
            Err("No matching rows found to delete".to_string())
        }
    }

    fn execute_create_table(&mut self, stmt: CreateTableStatement) -> Result<String, String> {
        if self.tables.contains_key(&stmt.table) {
            return Err(format!("Table '{}' already exists", stmt.table));
        }

        let columns = stmt.columns.iter()
            .map(|col| col.0.clone())
            .collect::<Vec<_>>();

        self.tables.insert(stmt.table.clone(), Vec::new());
        Ok(format!("✅ Table '{}' created with columns: {:?}", stmt.table, columns))
    }
    fn execute_alter_table(&mut self, stmt: AlterTableStatement) -> Result<String, String> {
        let table = self.tables.get_mut(&stmt.table)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table))?;
    
        // Add the new column with empty string to all existing rows
        for row in table.iter_mut() {
            row.insert(stmt.new_column.clone(), "".to_string());
        }
    
        Ok(format!("✅ Column '{}' added to table '{}'", stmt.new_column, stmt.table))
    }
}
