use crate::ast::{
    SQLStatement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement,
    CreateTableStatement, AlterTableStatement, DropTableStatement, AlterAction,
    OrderByClause, WhereClause, ColumnExpr,HavingClause,
};
use std::collections::HashMap;

#[derive(Debug)]
pub struct Database {
    tables: HashMap<String, Vec<HashMap<String, String>>>,
}

impl Database {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub fn execute(&mut self, stmt: SQLStatement) -> Result<String, String> {
        match stmt {
            SQLStatement::Select(s)      => self.execute_select(&s),
            SQLStatement::Insert(s)      => self.execute_insert(s),
            SQLStatement::Update(s)      => self.execute_update(s),
            SQLStatement::Delete(s)      => self.execute_delete(s),
            SQLStatement::CreateTable(s) => self.execute_create_table(s),
            SQLStatement::AlterTable(s)  => self.execute_alter_table(s),
            SQLStatement::DropTable(s)   => self.execute_drop_table(s),
        }
    }

    fn execute_select(&self, stmt: &SelectStatement) -> Result<String, String> {
        let table = self.tables.get(&stmt.table)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table))?;
    
        // 1. Filter by WHERE clause
        let filtered_rows: Vec<_> = table.iter()
            .filter(|row| {
                stmt.where_clause
                    .as_ref()
                    .map_or(true, |wc| row.get(&wc.column).map_or(false, |v| v == &wc.value))
            })
            .collect();
    
        // 2. Handle GROUP BY
        let mut rows = if let Some(group_cols) = &stmt.group_by {
            let mut seen_keys = Vec::new();
            for r in &filtered_rows {
                let key: Vec<String> = group_cols.iter()
                    .map(|col| r.get(col).cloned().unwrap_or_default())
                    .collect();
                if !seen_keys.contains(&key) {
                    seen_keys.push(key);
                }
            }
    
            seen_keys
                .into_iter()
                .filter_map(|key| {
                    filtered_rows.iter().find(|r| {
                        group_cols.iter().enumerate().all(|(i, col)| {
                            r.get(col).cloned().unwrap_or_default() == key[i]
                        })
                    })
                })
                .copied()
                .collect::<Vec<_>>()
        } else {
            filtered_rows.clone()
        };

        if let Some(having) = &stmt.having {
            let val: f64 = having.value.parse().unwrap_or(0.0);
            // let group_by_cols = stmt.group_by.as_ref().unwrap();
            let group_by_cols = stmt.group_by.as_ref();

            rows = rows.into_iter().filter(|group_row| {
                // Compare rows by group key
                let group_matches = |r: &&HashMap<String, String>| {
                match group_by_cols {
                    Some(cols) => cols.iter().all(|col| {
                        match (r.get(col), group_row.get(col)) {
                            (Some(a), Some(b)) => a == b,
                            _ => false,
                        }
                    }),
                    None => true, // No GROUP BY: whole table is one group
                }
            };
        
                // Extract group-matching rows
                let relevant_rows: Vec<_> = filtered_rows.iter().filter(|r| group_matches(r)).collect();
        
                // Compute the aggregation value
                let agg_val = match &having.column_expr {
                    ColumnExpr::CountAll => relevant_rows.len() as f64,
                    ColumnExpr::Count(col) => relevant_rows.iter().filter(|r| r.contains_key(col)).count() as f64,
                    ColumnExpr::Sum(col) => {
                        relevant_rows.iter()
                            .filter_map(|r| r.get(col))
                            .filter_map(|v| v.parse::<f64>().ok())
                            .sum()
                    }
                    ColumnExpr::Avg(col) => {
                        let values: Vec<f64> = relevant_rows.iter()
                            .filter_map(|r| r.get(col))
                            .filter_map(|v| v.parse::<f64>().ok())
                            .collect();
                        if values.is_empty() { 0.0 } else { values.iter().sum::<f64>() / values.len() as f64 }
                    }
                    ColumnExpr::Min(col) => {
                        relevant_rows.iter()
                            .filter_map(|r| r.get(col))
                            .filter_map(|v| v.parse::<f64>().ok())
                            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                            .unwrap_or(0.0)
                    }
                    ColumnExpr::Max(col) => {
                        relevant_rows.iter()
                            .filter_map(|r| r.get(col))
                            .filter_map(|v| v.parse::<f64>().ok())
                            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                            .unwrap_or(0.0)
                    }
                    _ => 0.0,
                };
        
                // Apply the HAVING filter
                match having.operator.as_str() {
                    "=" => agg_val == val,
                    ">" => agg_val > val,
                    "<" => agg_val < val,
                    _ => false,
                }
            }).collect();
        }
        

    
        // 3. ORDER BY clause
        if let Some(order) = &stmt.order_by {
            let empty = String::new();
            rows.sort_by(|a, b| {
                let va = a.get(&order.column).unwrap_or(&empty);
                let vb = b.get(&order.column).unwrap_or(&empty);
                if order.descending { vb.cmp(va) } else { va.cmp(vb) }
            });
        }
    
        if rows.is_empty() {
            return Err("No matching rows found".to_string());
        }
    
        // 4. Determine if the query is aggregate-only
        let is_aggregate_only = stmt.columns.iter().all(|col| matches!(
            col,
            ColumnExpr::Count(_) |
            ColumnExpr::CountAll |
            ColumnExpr::Sum(_) |
            ColumnExpr::Avg(_) |
            ColumnExpr::Min(_) |
            ColumnExpr::Max(_)
        ));
    
        // 5. Build header
        let selected: Vec<String> = if stmt.columns.len() == 1 {
            match &stmt.columns[0] {
                ColumnExpr::CountAll => vec!["COUNT(*)".to_string()],
                ColumnExpr::Column(c) => vec![c.clone()],
                ColumnExpr::All => {
                    let mut cols: Vec<_> = rows[0].keys().cloned().collect();
                    cols.sort(); 
                    cols
                }
                _ => stmt.columns.iter().map(|c| match c {
                    ColumnExpr::Column(name) => name.clone(),
                    ColumnExpr::Count(col) => format!("COUNT({})", col),
                    ColumnExpr::Sum(col) => format!("SUM({})", col),
                    ColumnExpr::Avg(col) => format!("AVG({})", col),
                    ColumnExpr::Min(col) => format!("MIN({})", col),
                    ColumnExpr::Max(col) => format!("MAX({})", col),
                    ColumnExpr::CountAll => "COUNT(*)".to_string(),
                    ColumnExpr::All => "*".to_string(),
                }).collect(),
            }
        } else {
            stmt.columns.iter().map(|c| match c {
                ColumnExpr::Column(name) => name.clone(),
                ColumnExpr::Count(col) => format!("COUNT({})", col),
                ColumnExpr::Sum(col) => format!("SUM({})", col),
                ColumnExpr::Avg(col) => format!("AVG({})", col),
                ColumnExpr::Min(col) => format!("MIN({})", col),
                ColumnExpr::Max(col) => format!("MAX({})", col),
                ColumnExpr::CountAll => "COUNT(*)".to_string(),
                ColumnExpr::All => "*".to_string(),
            }).collect()
        };
    
        let mut out = String::new();
        out += &selected.join(" | ");
        out += "\n";
        out += &"-".repeat(selected.join(" | ").len());
        out += "\n";
    
        if is_aggregate_only {
            let row_vals: Vec<String> = stmt.columns.iter().map(|col| {
                match col {
                    ColumnExpr::CountAll => filtered_rows.len().to_string(),
                    ColumnExpr::Count(c) => filtered_rows.iter().filter(|rr| rr.contains_key(c)).count().to_string(),
                    ColumnExpr::Sum(c) => {
                        let sum: i32 = filtered_rows.iter()
                            .filter_map(|rr| rr.get(c)?.parse::<i32>().ok())
                            .sum();
                        sum.to_string()
                    }
                    ColumnExpr::Avg(c) => {
                        let values: Vec<i32> = filtered_rows.iter()
                            .filter_map(|rr| rr.get(c)?.parse::<i32>().ok())
                            .collect();
                        if values.is_empty() { "0".to_string() }
                        else { (values.iter().sum::<i32>() as f64 / values.len() as f64).to_string() }
                    }
                    ColumnExpr::Min(c) => {
                        filtered_rows.iter()
                            .filter_map(|rr| rr.get(c)?.parse::<i32>().ok())
                            .min().map(|v| v.to_string()).unwrap_or_default()
                    }
                    ColumnExpr::Max(c) => {
                        filtered_rows.iter()
                            .filter_map(|rr| rr.get(c)?.parse::<i32>().ok())
                            .max().map(|v| v.to_string()).unwrap_or_default()
                    }
                    _ => "".to_string(),
                }
            }).collect();
    
            out += &row_vals.join(" | ");
            out += "\n";
        } else {
            for r in rows {
                let row_output = if stmt.columns.len() == 1 && matches!(stmt.columns[0], ColumnExpr::All) {
                    let mut keys: Vec<_> = r.keys().collect();
                    keys.sort();
                    keys.iter()
                        .map(|k| r.get(*k).unwrap_or(&"".to_string()).clone())
                        .collect::<Vec<_>>()
                        .join(" | ")
                } else {
                    stmt.columns.iter().map(|col| {
                        match col {
                            ColumnExpr::Column(name) => r.get(name).cloned().unwrap_or_default(),
                            ColumnExpr::All => "".to_string(), // handled above
                            ColumnExpr::CountAll => filtered_rows.len().to_string(),
                            ColumnExpr::Count(c) => filtered_rows.iter().filter(|rr| rr.contains_key(c)).count().to_string(),
                            ColumnExpr::Sum(c) => {
                                let sum: i32 = filtered_rows.iter()
                                    .filter_map(|rr| rr.get(c)?.parse::<i32>().ok())
                                    .sum();
                                sum.to_string()
                            }
                            ColumnExpr::Avg(c) => {
                                let values: Vec<i32> = filtered_rows.iter()
                                    .filter_map(|rr| rr.get(c)?.parse::<i32>().ok())
                                    .collect();
                                if values.is_empty() { "0".to_string() }
                                else { (values.iter().sum::<i32>() as f64 / values.len() as f64).to_string() }
                            }
                            ColumnExpr::Min(c) => {
                                filtered_rows.iter()
                                    .filter_map(|rr| rr.get(c)?.parse::<i32>().ok())
                                    .min().map(|v| v.to_string()).unwrap_or_default()
                            }
                            ColumnExpr::Max(c) => {
                                filtered_rows.iter()
                                    .filter_map(|rr| rr.get(c)?.parse::<i32>().ok())
                                    .max().map(|v| v.to_string()).unwrap_or_default()
                            }
                        }
                    }).collect::<Vec<_>>().join(" | ")
                };
    
                out += &row_output;
                out += "\n";
            }
        }
    
        Ok(out)
    }
    
    fn execute_insert(&mut self, stmt: InsertStatement) -> Result<String, String> {
        let table = self.tables.entry(stmt.table.clone()).or_insert_with(Vec::new);
    
        for value_tuple in stmt.values {
            if stmt.columns.len() != value_tuple.len() {
                return Err("Column count does not match value count".to_string());
            }
    
            let new_row: HashMap<String, String> = stmt.columns
                .iter()
                .cloned()
                .zip(value_tuple.into_iter())
                .collect();
    
            table.push(new_row);
        }
    
        Ok(" Insert successful".to_string())
    }
    

    fn execute_update(&mut self, stmt: UpdateStatement) -> Result<String, String> {
        let table = self.tables.get_mut(&stmt.table)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table))?;

        let mut updated = 0;
        for row in table.iter_mut() {
            if stmt.where_clause.as_ref().map_or(true, |wc| row.get(&wc.column) == Some(&wc.value)) {
                for (col, val) in &stmt.assignments {
                    row.insert(col.clone(), val.clone());
                }
                updated += 1;
            }
        }

        if updated > 0 {
            Ok(format!(" Updated {} row(s)", updated))
        } else {
            Err("No rows updated".into())
        }
    }

    fn execute_delete(&mut self, stmt: DeleteStatement) -> Result<String, String> {
        let table = self.tables.get_mut(&stmt.table)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table))?;

        let before = table.len();
        table.retain(|row| {
            stmt.where_clause.as_ref().map_or(true, |wc| row.get(&wc.column).map_or(true, |v| v != &wc.value))
        });
        let deleted = before - table.len();

        if deleted > 0 {
            Ok(format!("🗑️ Deleted {} row(s)", deleted))
        } else {
            Err("No matching rows to delete".into())
        }
    }

    fn execute_create_table(&mut self, stmt: CreateTableStatement) -> Result<String, String> {
        if self.tables.contains_key(&stmt.table) {
            Err(format!("Table '{}' already exists", stmt.table))
        } else {
            self.tables.insert(stmt.table.clone(), Vec::new());
            Ok(format!(" Created table '{}'", stmt.table))
        }
    }

    fn execute_alter_table(&mut self, stmt: AlterTableStatement) -> Result<String, String> {
        let td = self.tables.get_mut(&stmt.table)
            .ok_or_else(|| format!("Table '{}' not found", stmt.table))?;

        match &stmt.action {
            AlterAction::AddColumn(col) => {
                for row in td.iter_mut() {
                    row.insert(col.clone(), String::new());
                }
                Ok(format!(" Added column '{}' to '{}'", col, stmt.table))
            }
            AlterAction::DropColumn(col) => {
                for row in td.iter_mut() {
                    row.remove(col);
                }
                Ok(format!(" Dropped column '{}' from '{}'", col, stmt.table))
            }
            AlterAction::ModifyColumn(col, new_type) => {
                Ok(format!(" Modified column '{}' to '{}' in '{}'", col, new_type, stmt.table))
            }
        }
    }

    fn execute_drop_table(&mut self, stmt: DropTableStatement) -> Result<String, String> {
        if self.tables.remove(&stmt.table).is_some() {
            Ok(format!("🗑️ Dropped table '{}'", stmt.table))
        } else {
            Err(format!("Table '{}' does not exist", stmt.table))
        }
    }
}
