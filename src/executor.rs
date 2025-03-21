use crate::ast::{SQLStatement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement};
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
        }
    }

    fn execute_select(&self, stmt: &SelectStatement) -> Result<String, String> {
        let table = self.tables.get(&stmt.table)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table))?;
        
        let filtered_rows: Vec<String> = table.iter()
            .filter(|row| {
                stmt.where_clause.as_ref().map_or(true, |where_clause| {
                    row.get(&where_clause.column).map_or(false, |value| value == &where_clause.value)
                })
            })
            .map(|row| format!("{:?}", row))
            .collect();
        
        if filtered_rows.is_empty() {
            return Err("No matching rows found".to_string());
        }
        
        Ok(filtered_rows.join("\n"))
    }

    fn execute_insert(&mut self, stmt: InsertStatement) -> Result<String, String> {
        let table = self.tables.entry(stmt.table.clone()).or_insert_with(Vec::new);

        if stmt.columns.len() != stmt.values.len() {
            return Err("Column count does not match value count".to_string());
        }

        let new_row: HashMap<String, String> = stmt.columns.into_iter()
            .zip(stmt.values.into_iter())
            .collect();

        table.push(new_row);
        Ok("Insert successful".to_string())
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
            Ok(format!("Updated {} row(s)", updated))
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
            Ok(format!("Deleted {} row(s)", deleted))
        } else {
            Err("No matching rows found to delete".to_string())
        }
    }
}