use crate::ast::{
    SQLStatement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement,
    CreateTableStatement, AlterTableStatement, DropTableStatement, AlterAction,
    OrderByClause, WhereClause, ColumnExpr,HavingClause, JoinClause, JoinType,
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
        // 1. Evaluate JOIN if any
        let mut rows = if let Some(join) = &stmt.join {
            let left_table = self.tables.get(&stmt.table)
                .ok_or_else(|| format!("Left table '{}' not found", stmt.table))?;
            let right_table = self.tables.get(&join.table)
                .ok_or_else(|| format!("Right table '{}' not found", join.table))?;
    
            let mut result = Vec::new();
            let left_col = join.on_left.split('.').last().unwrap();
let right_col = join.on_right.split('.').last().unwrap();

match join.join_type {
    JoinType::Inner => {
        for lrow in left_table {
            for rrow in right_table {
                if lrow.get(left_col) == rrow.get(right_col) {
                    let mut combined = lrow.clone();
                    for (k, v) in rrow {
                        combined.insert(format!("{}.{}", join.table, k), v.clone());
                    }
                    result.push(combined);
                }
            }
        }
    }

    JoinType::Left => {
        for lrow in left_table {
            let mut matched = false;
            for rrow in right_table {
                if lrow.get(left_col) == rrow.get(right_col) {
                    let mut combined = lrow.clone();
                    for (k, v) in rrow {
                        combined.insert(format!("{}.{}", join.table, k), v.clone());
                    }
                    result.push(combined);
                    matched = true;
                }
            }
            if !matched {
                let mut combined = lrow.clone();
                for k in right_table[0].keys() {
                    combined.insert(format!("{}.{}", join.table, k), "NULL".to_string());
                }
                result.push(combined);
            }
        }
    }

    JoinType::Right => {
        for rrow in right_table {
            let mut matched = false;
            for lrow in left_table {
                if lrow.get(left_col) == rrow.get(right_col) {
                    let mut combined = lrow.clone();
                    for (k, v) in rrow {
                        combined.insert(format!("{}.{}", join.table, k), v.clone());
                    }
                    result.push(combined);
                    matched = true;
                }
            }
            if !matched {
                let mut combined = HashMap::new();
                for k in left_table[0].keys() {
                    combined.insert(k.clone(), "NULL".to_string());
                }
                for (k, v) in rrow {
                    combined.insert(format!("{}.{}", join.table, k), v.clone());
                }
                result.push(combined);
            }
        }
    }

    JoinType::Full => {
        let mut matched_right = vec![false; right_table.len()];
        for lrow in left_table {
            let mut matched = false;
            for (i, rrow) in right_table.iter().enumerate() {
                if lrow.get(left_col) == rrow.get(right_col) {
                    let mut combined = lrow.clone();
                    for (k, v) in rrow {
                        combined.insert(format!("{}.{}", join.table, k), v.clone());
                    }
                    result.push(combined);
                    matched = true;
                    matched_right[i] = true;
                }
            }
            if !matched {
                let mut combined = lrow.clone();
                for k in right_table[0].keys() {
                    combined.insert(format!("{}.{}", join.table, k), "NULL".to_string());
                }
                result.push(combined);
            }
        }

        for (i, rrow) in right_table.iter().enumerate() {
            if !matched_right[i] {
                let mut combined = HashMap::new();
                for k in left_table[0].keys() {
                    combined.insert(k.clone(), "NULL".to_string());
                }
                for (k, v) in rrow {
                    combined.insert(format!("{}.{}", join.table, k), v.clone());
                }
                result.push(combined);
            }
        }
    }
    JoinType::Cross => {
        for lrow in left_table {
            for rrow in right_table {
                let mut combined = lrow.clone();
                for (k, v) in rrow {
                    combined.insert(format!("{}.{}", join.table, k), v.clone());
                }
                result.push(combined);
            }
        }
    }
}

    
            result} else {
            self.tables.get(&stmt.table)
                .ok_or_else(|| format!("Table '{}' not found", stmt.table))?
                .clone()
        };
    
        // 2. Apply WHERE filter
        if let Some(where_clause) = &stmt.where_clause {
            rows = rows.into_iter()
                .filter(|row| row.get(&where_clause.column)
                    .map_or(false, |val| val == &where_clause.value))
                .collect();
        }
    
        // 3. Apply GROUP BY
        if let Some(group_cols) = &stmt.group_by {
            let mut seen = Vec::new();
            let mut grouped = Vec::new();
            for r in &rows {
                let key: Vec<String> = group_cols.iter()
                    .map(|c| r.get(c).cloned().unwrap_or_default())
                    .collect();
                if !seen.contains(&key) {
                    seen.push(key.clone());
                    grouped.push(r.clone());
                }
            }
            rows = grouped;
        }
    
        // 4. Apply HAVING
        if let Some(having) = &stmt.having {
            let val: f64 = having.value.parse().unwrap_or(0.0);
            let all_rows = rows.clone();
            rows = rows.into_iter().filter(|group_row| {
                let group: Vec<_> = self.tables.get(&stmt.table).unwrap().iter().filter(|r| {
                    stmt.group_by.as_ref().map_or(true, |cols| {
                        cols.iter().all(|c| r.get(c) == group_row.get(c))
                    })
                }).collect();
    
                let agg_val = match &having.column_expr {
                    ColumnExpr::CountAll => group.len() as f64,
                    ColumnExpr::Count(col) => group.iter().filter(|r| r.contains_key(col)).count() as f64,
                    ColumnExpr::Sum(col) => group.iter().filter_map(|r| r.get(col)?.parse::<f64>().ok()).sum(),
                    ColumnExpr::Avg(col) => {
                        let vals: Vec<f64> = group.iter().filter_map(|r| r.get(col)?.parse::<f64>().ok()).collect();
                        if vals.is_empty() { 0.0 } else { vals.iter().sum::<f64>() / vals.len() as f64 }
                    }
                    ColumnExpr::Min(col) => group.iter().filter_map(|r| r.get(col)?.parse::<f64>().ok()).min_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(0.0),
                    ColumnExpr::Max(col) => group.iter().filter_map(|r| r.get(col)?.parse::<f64>().ok()).max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(0.0),
                    _ => 0.0,
                };
    
                match having.operator.as_str() {
                    "=" => agg_val == val,
                    ">" => agg_val > val,
                    "<" => agg_val < val,
                    _ => false,
                }
            }).collect();
        }
    
        // 5. Apply ORDER BY
        if let Some(order) = &stmt.order_by {
            let empty = String::new();
            rows.sort_by(|a, b| {
                let va = a.get(&order.column).unwrap_or(&empty);
                let vb = b.get(&order.column).unwrap_or(&empty);
                if order.descending { vb.cmp(va) } else { va.cmp(vb) }
            });
        }
    
        // 6. Output formatting
        if rows.is_empty() {
            return Err("No matching rows found".to_string());
        }
    
        let mut output = String::new();
        let headers: Vec<String> = if stmt.columns.len() == 1 && matches!(stmt.columns[0], ColumnExpr::All) {
            let mut keys: Vec<_> = rows[0].keys().cloned().collect();
            keys.sort();
            keys
        } else {
            stmt.columns.iter().map(|col| match col {
                ColumnExpr::Column(c) => c.clone(),
                ColumnExpr::All => "*".to_string(),
                ColumnExpr::Count(c) => format!("COUNT({})", c),
                ColumnExpr::CountAll => "COUNT(*)".to_string(),
                ColumnExpr::Sum(c) => format!("SUM({})", c),
                ColumnExpr::Avg(c) => format!("AVG({})", c),
                ColumnExpr::Min(c) => format!("MIN({})", c),
                ColumnExpr::Max(c) => format!("MAX({})", c),
            }).collect()
        };
        output += &headers.join(" | ");
        output += "\n";
        output += &"-".repeat(headers.join(" | ").len());
        output += "\n";
    
        for row in rows {
            let line = if stmt.columns.len() == 1 && matches!(stmt.columns[0], ColumnExpr::All) {
                let mut keys: Vec<_> = row.keys().collect();
                keys.sort();
                keys.iter()
                    .map(|k| row.get(*k).unwrap_or(&"".to_string()).clone())
                    .collect::<Vec<_>>()
                    .join(" | ")
            } else {
                stmt.columns.iter().map(|col| {
                    match col {
                        ColumnExpr::Column(c) => {
                            // Try fully qualified first
                            row.get(c).cloned()
                             // If not found, try unqualified match
                             .or_else(|| {
                                 let parts: Vec<&str> = c.split('.').collect();
                                 if parts.len() == 2 {
                                     row.get(parts[1]).cloned()
                                 } else {
                                     None
                                 }
                             })
                             .unwrap_or_default()
                        },                        

        ColumnExpr::CountAll => {
            let group_rows: Vec<_> = self.tables.get(&stmt.table).unwrap().iter()
                .filter(|r| stmt.group_by.as_ref().map_or(true, |cols| {
                    cols.iter().all(|col| r.get(col) == row.get(col))
                }))
                .collect();
            group_rows.len().to_string()
        }

        ColumnExpr::Count(c) => {
            let group_rows: Vec<_> = self.tables.get(&stmt.table).unwrap().iter()
                .filter(|r| stmt.group_by.as_ref().map_or(true, |cols| {
                    cols.iter().all(|col| r.get(col) == row.get(col))
                }))
                .collect();
            group_rows.iter().filter(|r| r.contains_key(c)).count().to_string()
        }

        ColumnExpr::Sum(c) => {
            let group_rows: Vec<_> = self.tables.get(&stmt.table).unwrap().iter()
                .filter(|r| stmt.group_by.as_ref().map_or(true, |cols| {
                    cols.iter().all(|col| r.get(col) == row.get(col))
                }))
                .collect();
            let sum: f64 = group_rows.iter()
                .filter_map(|r| r.get(c)?.parse::<f64>().ok())
                .sum();
            sum.to_string()
        }

        ColumnExpr::Avg(c) => {
            let group_rows: Vec<_> = self.tables.get(&stmt.table).unwrap().iter()
                .filter(|r| stmt.group_by.as_ref().map_or(true, |cols| {
                    cols.iter().all(|col| r.get(col) == row.get(col))
                }))
                .collect();
            let vals: Vec<f64> = group_rows.iter()
                .filter_map(|r| r.get(c)?.parse::<f64>().ok())
                .collect();
            if vals.is_empty() { "0".to_string() }
            else { (vals.iter().sum::<f64>() / vals.len() as f64).to_string() }
        }

        ColumnExpr::Min(c) => {
            let group_rows: Vec<_> = self.tables.get(&stmt.table).unwrap().iter()
                .filter(|r| stmt.group_by.as_ref().map_or(true, |cols| {
                    cols.iter().all(|col| r.get(col) == row.get(col))
                }))
                .collect();
            group_rows.iter()
                .filter_map(|r| r.get(c)?.parse::<f64>().ok())
                .min_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap_or(0.0)
                .to_string()
        }

        ColumnExpr::Max(c) => {
            let group_rows: Vec<_> = self.tables.get(&stmt.table).unwrap().iter()
                .filter(|r| stmt.group_by.as_ref().map_or(true, |cols| {
                    cols.iter().all(|col| r.get(col) == row.get(col))
                }))
                .collect();
            group_rows.iter()
                .filter_map(|r| r.get(c)?.parse::<f64>().ok())
                .max_by(|a, b| a.partial_cmp(b).unwrap())
                .unwrap_or(0.0)
                .to_string()
        }

        _ => "".to_string() 
                    }
                }).collect::<Vec<_>>().join(" | ")
            };
            output += &line;
            output += "\n";
        }
    
        Ok(output)
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
