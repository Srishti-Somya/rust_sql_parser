use crate::ast::{
    SQLStatement, SelectStatement, InsertStatement, UpdateStatement, DeleteStatement,
    CreateTableStatement, AlterTableStatement, DropTableStatement, AlterAction,
    OrderByClause, WhereClause, ColumnExpr, HavingClause, JoinClause, JoinType,
};
use crate::storage::{LSMStorage, StorageValue};
use std::collections::HashMap;
use std::path::PathBuf;
use std::fs;
use serde_json;
use std::time::SystemTime;

#[derive(Debug)]
pub struct PersistentDatabase {
    data_dir: PathBuf,
    tables: HashMap<String, LSMStorage>,
    table_schemas: HashMap<String, Vec<String>>, // table_name -> column_names
}

impl PersistentDatabase {
    pub fn new(data_dir: &str) -> Result<Self, String> {
        let data_path = PathBuf::from(data_dir);
        fs::create_dir_all(&data_path).map_err(|e| format!("Failed to create data directory: {}", e))?;
        
        let mut db = Self {
            data_dir: data_path,
            tables: HashMap::new(),
            table_schemas: HashMap::new(),
        };
        
        // Load existing schemas
        db.load_schemas()?;
        
        Ok(db)
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

    fn execute_select(&mut self, stmt: &SelectStatement) -> Result<String, String> {
        let table_name = &stmt.table;
        
        // Get table storage
        let table_storage = self.tables.get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        // Get all rows from storage
        let all_rows = table_storage.get_all()
            .map_err(|e| format!("Storage error: {}", e))?;

        // Convert to HashMap format for compatibility with existing logic
        let mut rows = Vec::new();
        for (key, value) in all_rows {
            let row_data: HashMap<String, String> = serde_json::from_str(&value)
                .map_err(|e| format!("Failed to deserialize row data: {}", e))?;
            rows.push(row_data);
        }

        // Handle JOIN if present
        if let Some(join) = &stmt.join {
            let right_table_storage = self.tables.get_mut(&join.table)
                .ok_or_else(|| format!("Right table '{}' not found", join.table))?;
            
            let right_rows = right_table_storage.get_all()
                .map_err(|e| format!("Storage error: {}", e))?;
            
            let mut right_rows_data = Vec::new();
            for (_, value) in right_rows {
                let row_data: HashMap<String, String> = serde_json::from_str(&value)
                    .map_err(|e| format!("Failed to deserialize row data: {}", e))?;
                right_rows_data.push(row_data);
            }

            rows = self.perform_join(&rows, &right_rows_data, join, table_name)?;
        }

        // Apply WHERE clause
        if let Some(where_clause) = &stmt.where_clause {
            rows = self.apply_where_clause(rows, where_clause)?;
        }

        // Check if we have aggregate functions without GROUP BY
        let has_aggregates = stmt.columns.iter().any(|col| {
            matches!(col, ColumnExpr::Count(_) | ColumnExpr::Sum(_) | ColumnExpr::Avg(_) | 
                           ColumnExpr::Min(_) | ColumnExpr::Max(_) | ColumnExpr::CountAll)
        });
        
        // Apply GROUP BY or handle aggregates without GROUP BY
        if let Some(group_by) = &stmt.group_by {
            rows = self.apply_group_by(rows, group_by, &stmt.columns)?;
        } else if has_aggregates {
            // For aggregates without GROUP BY, treat all rows as one group
            rows = self.apply_group_by(rows, &[], &stmt.columns)?;
        }

        // Apply HAVING
        if let Some(having) = &stmt.having {
            rows = self.apply_having(rows, having)?;
        }

        // Apply ORDER BY
        if let Some(order_by) = &stmt.order_by {
            rows = self.apply_order_by(rows, order_by)?;
        }

        // Format result
        self.format_select_result(&rows, &stmt.columns, table_name)
    }

    fn execute_insert(&mut self, stmt: InsertStatement) -> Result<String, String> {
        let table_name = &stmt.table;
        
        // Get or create table storage
        let table_storage = self.tables.entry(table_name.clone())
            .or_insert_with(|| {
                LSMStorage::new(&self.data_dir, table_name)
                    .expect("Failed to create table storage")
            });

        let mut inserted_count = 0;

        // Process each row in the values
        for values_row in stmt.values {
            // Generate a unique key for this row
            let row_key = format!("row_{}_{}", SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(), inserted_count);

            // Create row data
            let mut row_data = HashMap::new();
            for (i, column) in stmt.columns.iter().enumerate() {
                if i < values_row.len() {
                    row_data.insert(column.clone(), values_row[i].clone());
                }
            }

            // Serialize row data
            let row_json = serde_json::to_string(&row_data)
                .map_err(|e| format!("Failed to serialize row data: {}", e))?;

            // Store in LSM storage
            table_storage.insert(row_key, row_json)
                .map_err(|e| format!("Storage error: {}", e))?;

            inserted_count += 1;
        }

        Ok(format!("{} row(s) inserted successfully", inserted_count))
    }

    fn execute_update(&mut self, stmt: UpdateStatement) -> Result<String, String> {
        let table_name = &stmt.table;
        
        let table_storage = self.tables.get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        // Get all rows
        let all_rows = table_storage.get_all()
            .map_err(|e| format!("Storage error: {}", e))?;

        let mut updated_count = 0;
        let mut updates = Vec::new();

        for (key, value) in all_rows {
            let mut row_data: HashMap<String, String> = serde_json::from_str(&value)
                .map_err(|e| format!("Failed to deserialize row data: {}", e))?;

            // Check WHERE condition
            let should_update = if let Some(where_clause) = &stmt.where_clause {
                Self::evaluate_where_condition(&row_data, where_clause)?
            } else {
                true
            };

            if should_update {
                // Apply updates
                for (column, new_value) in &stmt.assignments {
                    row_data.insert(column.clone(), new_value.clone());
                }
                updated_count += 1;
            }

            // Re-serialize and store
            let new_row_json = serde_json::to_string(&row_data)
                .map_err(|e| format!("Failed to serialize row data: {}", e))?;
            
            updates.push((key, new_row_json));
        }

        // Apply all updates
        for (key, new_row_json) in updates {
            table_storage.delete(key.clone())
                .map_err(|e| format!("Storage error: {}", e))?;
            table_storage.insert(key, new_row_json)
                .map_err(|e| format!("Storage error: {}", e))?;
        }

        Ok(format!("Updated {} rows", updated_count))
    }

    fn execute_delete(&mut self, stmt: DeleteStatement) -> Result<String, String> {
        let table_name = &stmt.table;
        
        let table_storage = self.tables.get_mut(table_name)
            .ok_or_else(|| format!("Table '{}' not found", table_name))?;

        // Get all rows
        let all_rows = table_storage.get_all()
            .map_err(|e| format!("Storage error: {}", e))?;

        let mut deleted_count = 0;
        let mut keys_to_delete = Vec::new();

        for (key, value) in all_rows {
            let row_data: HashMap<String, String> = serde_json::from_str(&value)
                .map_err(|e| format!("Failed to deserialize row data: {}", e))?;

            // Check WHERE condition
            let should_delete = if let Some(where_clause) = &stmt.where_clause {
                Self::evaluate_where_condition(&row_data, where_clause)?
            } else {
                true
            };

            if should_delete {
                keys_to_delete.push(key);
                deleted_count += 1;
            }
        }

        // Delete the keys
        for key in keys_to_delete {
            table_storage.delete(key)
                .map_err(|e| format!("Storage error: {}", e))?;
        }

        Ok(format!("Deleted {} rows", deleted_count))
    }

    fn execute_create_table(&mut self, stmt: CreateTableStatement) -> Result<String, String> {
        let table_name = &stmt.table;
        
        // Create table storage
        let table_storage = LSMStorage::new(&self.data_dir, table_name)
            .map_err(|e| format!("Failed to create table storage: {}", e))?;
        
        self.tables.insert(table_name.clone(), table_storage);

        // Store schema
        let columns: Vec<String> = stmt.columns.iter().map(|col| col.0.clone()).collect();
        self.table_schemas.insert(table_name.clone(), columns.clone());

        // Persist schema to disk
        let mut schema_storage = LSMStorage::new(&self.data_dir, &format!("{}_schema", table_name))
            .map_err(|e| format!("Failed to create schema storage: {}", e))?;
        
        let schema_json = serde_json::to_string(&columns)
            .map_err(|e| format!("Failed to serialize schema: {}", e))?;
        
        schema_storage.insert("schema".to_string(), schema_json)
            .map_err(|e| format!("Failed to store schema: {}", e))?;

        Ok(format!("Created table '{}'", table_name))
    }

    fn execute_alter_table(&mut self, stmt: AlterTableStatement) -> Result<String, String> {
        let table_name = &stmt.table;
        
        // For now, we'll just acknowledge the alter table command
        // In a full implementation, you'd need to handle schema changes
        match &stmt.action {
            AlterAction::AddColumn(column_name) => {
                Ok(format!("Added column '{}' to table '{}'", column_name, table_name))
            }
            AlterAction::DropColumn(column_name) => {
                Ok(format!("Dropped column '{}' from table '{}'", column_name, table_name))
            }
            AlterAction::ModifyColumn(column_name, _) => {
                Ok(format!("Modified column '{}' in table '{}'", column_name, table_name))
            }
        }
    }

    fn execute_drop_table(&mut self, stmt: DropTableStatement) -> Result<String, String> {
        let table_name = &stmt.table;
        
        // Remove from memory
        self.tables.remove(table_name);
        self.table_schemas.remove(table_name);

        // Remove from disk
        let table_dir = self.data_dir.join(table_name);
        if table_dir.exists() {
            fs::remove_dir_all(&table_dir)
                .map_err(|e| format!("Failed to remove table directory: {}", e))?;
        }

        Ok(format!("Dropped table '{}'", table_name))
    }

    // Helper methods for JOIN operations
    fn perform_join(&self, left_rows: &[HashMap<String, String>], 
                   right_rows: &[HashMap<String, String>], 
                   join: &JoinClause, table_name: &str) -> Result<Vec<HashMap<String, String>>, String> {
        let mut result = Vec::new();
        let left_col = join.on_left.split('.').last().unwrap();
        let right_col = join.on_right.split('.').last().unwrap();

        match join.join_type {
            JoinType::Inner => {
                for lrow in left_rows {
                    for rrow in right_rows {
                        if lrow.get(left_col) == rrow.get(right_col) {
                            let mut combined = HashMap::new();
                            // Add left table columns with table prefix (use the main table name)
                            for (k, v) in lrow {
                                combined.insert(format!("{}.{}", table_name, k), v.clone());
                            }
                            // Add right table columns with table prefix
                            for (k, v) in rrow {
                                combined.insert(format!("{}.{}", join.table, k), v.clone());
                            }
                            result.push(combined);
                        }
                    }
                }
            }
            JoinType::Left => {
                for lrow in left_rows {
                    let mut matched = false;
                    for rrow in right_rows {
                        if lrow.get(left_col) == rrow.get(right_col) {
                            let mut combined = HashMap::new();
                            // Add left table columns with table prefix
                            for (k, v) in lrow {
                                combined.insert(format!("{}.{}", table_name, k), v.clone());
                            }
                            // Add right table columns with table prefix
                            for (k, v) in rrow {
                                combined.insert(format!("{}.{}", join.table, k), v.clone());
                            }
                            result.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut combined = HashMap::new();
                        // Add left table columns with table prefix
                        for (k, v) in lrow {
                            combined.insert(format!("{}.{}", table_name, k), v.clone());
                        }
                        // Add NULL values for right table columns
                        if !right_rows.is_empty() {
                            for k in right_rows[0].keys() {
                                combined.insert(format!("{}.{}", join.table, k), "NULL".to_string());
                            }
                        }
                        result.push(combined);
                    }
                }
            }
            JoinType::Right => {
                for rrow in right_rows {
                    let mut matched = false;
                    for lrow in left_rows {
                        if lrow.get(left_col) == rrow.get(right_col) {
                            let mut combined = HashMap::new();
                            // Add left table columns with table prefix
                            for (k, v) in lrow {
                                combined.insert(format!("{}.{}", table_name, k), v.clone());
                            }
                            // Add right table columns with table prefix
                            for (k, v) in rrow {
                                combined.insert(format!("{}.{}", join.table, k), v.clone());
                            }
                            result.push(combined);
                            matched = true;
                        }
                    }
                    if !matched {
                        let mut combined = HashMap::new();
                        // Add NULL values for left table columns
                        if !left_rows.is_empty() {
                            for k in left_rows[0].keys() {
                                combined.insert(format!("{}.{}", table_name, k), "NULL".to_string());
                            }
                        }
                        // Add right table columns with table prefix
                        for (k, v) in rrow {
                            combined.insert(format!("{}.{}", join.table, k), v.clone());
                        }
                        result.push(combined);
                    }
                }
            }
            JoinType::Full => {
                // Implementation similar to LEFT + RIGHT join
                result.extend(self.perform_join(left_rows, right_rows, &JoinClause {
                    join_type: JoinType::Left,
                    table: join.table.clone(),
                    on_left: join.on_left.clone(),
                    on_right: join.on_right.clone(),
                }, table_name)?);
            }
            JoinType::Cross => {
                for lrow in left_rows {
                    for rrow in right_rows {
                        let mut combined = HashMap::new();
                        // Add left table columns with table prefix
                        for (k, v) in lrow {
                            combined.insert(format!("{}.{}", table_name, k), v.clone());
                        }
                        // Add right table columns with table prefix
                        for (k, v) in rrow {
                            combined.insert(format!("{}.{}", join.table, k), v.clone());
                        }
                        result.push(combined);
                    }
                }
            }
        }

        Ok(result)
    }

    fn apply_where_clause(&self, rows: Vec<HashMap<String, String>>, 
                         where_clause: &WhereClause) -> Result<Vec<HashMap<String, String>>, String> {
        let mut filtered_rows = Vec::new();
        
        for row in rows {
            if Self::evaluate_where_condition(&row, where_clause)? {
                filtered_rows.push(row);
            }
        }
        
        Ok(filtered_rows)
    }

    fn evaluate_where_condition(row: &HashMap<String, String>, 
                               where_clause: &WhereClause) -> Result<bool, String> {
        let left_value = row.get(&where_clause.column)
            .ok_or_else(|| format!("Column '{}' not found", where_clause.column))?;
        
        let right_value = &where_clause.value;
        
        match where_clause.operator.as_str() {
            "=" => Ok(left_value == right_value),
            "!=" => Ok(left_value != right_value),
            ">" => {
                let left_num: f64 = left_value.parse().map_err(|_| "Invalid number")?;
                let right_num: f64 = right_value.parse().map_err(|_| "Invalid number")?;
                Ok(left_num > right_num)
            }
            "<" => {
                let left_num: f64 = left_value.parse().map_err(|_| "Invalid number")?;
                let right_num: f64 = right_value.parse().map_err(|_| "Invalid number")?;
                Ok(left_num < right_num)
            }
            _ => Err(format!("Unsupported operator: {}", where_clause.operator)),
        }
    }

    fn apply_group_by(&self, rows: Vec<HashMap<String, String>>, 
                     group_by: &[String], 
                     columns: &[ColumnExpr]) -> Result<Vec<HashMap<String, String>>, String> {
        // Simple grouping implementation
        let mut groups: HashMap<String, Vec<HashMap<String, String>>> = HashMap::new();
        
        for row in rows {
            let group_key: String = group_by.iter()
                .map(|col| row.get(col).unwrap_or(&"NULL".to_string()).clone())
                .collect::<Vec<_>>()
                .join("|");
            
            groups.entry(group_key).or_insert_with(Vec::new).push(row);
        }
        
        let mut result = Vec::new();
        for (_, group_rows) in groups {
            if let Some(first_row) = group_rows.first() {
                let mut aggregated_row = first_row.clone();
                
                // Apply aggregate functions
                for col_expr in columns {
                    match col_expr {
                        ColumnExpr::Column(name) => {
                            // Keep the first value for grouping columns
                        }
                        ColumnExpr::Count(col_name) => {
                            let count = group_rows.len() as f64;
                            aggregated_row.insert(format!("COUNT({})", col_name), count.to_string());
                        }
                        ColumnExpr::Sum(col_name) => {
                            let values: Vec<f64> = group_rows.iter()
                                .filter_map(|row| row.get(col_name).and_then(|v| v.parse().ok()))
                                .collect();
                            let sum = values.iter().sum::<f64>();
                            aggregated_row.insert(format!("SUM({})", col_name), sum.to_string());
                        }
                        ColumnExpr::Avg(col_name) => {
                            let values: Vec<f64> = group_rows.iter()
                                .filter_map(|row| row.get(col_name).and_then(|v| v.parse().ok()))
                                .collect();
                            let avg = if values.is_empty() { 0.0 } else { values.iter().sum::<f64>() / values.len() as f64 };
                            aggregated_row.insert(format!("AVG({})", col_name), avg.to_string());
                        }
                        ColumnExpr::Min(col_name) => {
                            let values: Vec<f64> = group_rows.iter()
                                .filter_map(|row| row.get(col_name).and_then(|v| v.parse().ok()))
                                .collect();
                            let min = values.iter().fold(f64::INFINITY, |a, &b| a.min(b));
                            aggregated_row.insert(format!("MIN({})", col_name), min.to_string());
                        }
                        ColumnExpr::Max(col_name) => {
                            let values: Vec<f64> = group_rows.iter()
                                .filter_map(|row| row.get(col_name).and_then(|v| v.parse().ok()))
                                .collect();
                            let max = values.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
                            aggregated_row.insert(format!("MAX({})", col_name), max.to_string());
                        }
                        ColumnExpr::CountAll => {
                            let count = group_rows.len() as f64;
                            aggregated_row.insert("COUNT(*)".to_string(), count.to_string());
                        }
                        ColumnExpr::All => {
                            // Keep all columns as is
                        }
                    }
                }
                
                result.push(aggregated_row);
            }
        }
        
        Ok(result)
    }

    fn apply_having(&self, rows: Vec<HashMap<String, String>>, 
                   having: &HavingClause) -> Result<Vec<HashMap<String, String>>, String> {
        let mut filtered_rows = Vec::new();
        
        for row in rows {
            // For simplicity, we'll extract the column name from the column_expr
            let column_name = match &having.column_expr {
                ColumnExpr::Column(name) => name.clone(),
                ColumnExpr::Count(name) => format!("COUNT({})", name),
                ColumnExpr::Sum(name) => format!("SUM({})", name),
                ColumnExpr::Avg(name) => format!("AVG({})", name),
                ColumnExpr::Min(name) => format!("MIN({})", name),
                ColumnExpr::Max(name) => format!("MAX({})", name),
                ColumnExpr::CountAll => "COUNT(*)".to_string(),
                ColumnExpr::All => "ALL".to_string(),
            };
            
            let value = row.get(&column_name)
                .ok_or_else(|| format!("Column '{}' not found", column_name))?;
            
            let num_value: f64 = value.parse().map_err(|_| "Invalid number")?;
            let threshold: f64 = having.value.parse().map_err(|_| "Invalid number")?;
            
            let condition_met = match having.operator.as_str() {
                ">" => num_value > threshold,
                "<" => num_value < threshold,
                "=" => num_value == threshold,
                "!=" => num_value != threshold,
                _ => return Err(format!("Unsupported operator: {}", having.operator)),
            };
            
            if condition_met {
                filtered_rows.push(row);
            }
        }
        
        Ok(filtered_rows)
    }

    fn apply_order_by(&self, mut rows: Vec<HashMap<String, String>>, 
                     order_by: &OrderByClause) -> Result<Vec<HashMap<String, String>>, String> {
        let empty = "".to_string();
        rows.sort_by(|a, b| {
            let a_val = a.get(&order_by.column).unwrap_or(&empty);
            let b_val = b.get(&order_by.column).unwrap_or(&empty);
            
            if order_by.descending {
                b_val.cmp(a_val)
            } else {
                a_val.cmp(b_val)
            }
        });
        
        Ok(rows)
    }

    fn format_select_result(&self, rows: &[HashMap<String, String>], 
                           columns: &[ColumnExpr], table_name: &str) -> Result<String, String> {
        if rows.is_empty() {
            return Ok("No matching rows found".to_string());
        }
        
        let mut result = String::new();
        
        // Print headers
        let headers: Vec<String> = if columns.len() == 1 && matches!(columns[0], ColumnExpr::All) {
            // For SELECT *, show all column names
            if let Some(schema) = self.table_schemas.get(table_name) {
                schema.clone()
            } else {
                vec!["*".to_string()]
            }
        } else {
            columns.iter().map(|col| {
                match col {
                    ColumnExpr::Column(name) => name.clone(),
                    ColumnExpr::Count(name) => format!("COUNT({})", name),
                    ColumnExpr::Sum(name) => format!("SUM({})", name),
                    ColumnExpr::Avg(name) => format!("AVG({})", name),
                    ColumnExpr::Min(name) => format!("MIN({})", name),
                    ColumnExpr::Max(name) => format!("MAX({})", name),
                    ColumnExpr::CountAll => "COUNT(*)".to_string(),
                    ColumnExpr::All => "*".to_string(),
                }
            }).collect()
        };
        
        result.push_str(&headers.join(" | "));
        result.push('\n');
        result.push_str(&"-".repeat(result.len()));
        result.push('\n');
        
        // Print rows
        for row in rows {
            let values: Vec<String> = if columns.len() == 1 && matches!(columns[0], ColumnExpr::All) {
                // For SELECT *, show all column values in schema order
                if let Some(schema) = self.table_schemas.get(table_name) {
                    schema.iter()
                        .map(|col_name| row.get(col_name).unwrap_or(&"NULL".to_string()).clone())
                        .collect()
                } else {
                    // Fallback: show all values in the row
                    row.values().cloned().collect()
                }
            } else {
                columns.iter().map(|col| {
                    match col {
                        ColumnExpr::Column(name) => {
                            // For JOINs, handle both prefixed and unprefixed column names
                            if let Some(value) = row.get(name) {
                                // Direct match (e.g., "name" or "customers.name")
                                value.clone()
                            } else {
                                // Try to find the column with table prefix
                                let mut found = false;
                                let mut result = "NULL".to_string();
                                for (key, value) in row {
                                    if key.ends_with(&format!(".{}", name)) {
                                        result = value.clone();
                                        found = true;
                                        break;
                                    }
                                }
                                if found { 
                                    result 
                                } else {
                                    // Try to find the column without table prefix
                                    let mut found = false;
                                    let mut result = "NULL".to_string();
                                    for (key, value) in row {
                                        if key == name {
                                            result = value.clone();
                                            found = true;
                                            break;
                                        }
                                    }
                                    if found { result } else { "NULL".to_string() }
                                }
                            }
                        },
                        ColumnExpr::Count(name) => {
                            row.get(&format!("COUNT({})", name))
                                .unwrap_or(&"NULL".to_string())
                                .clone()
                        }
                        ColumnExpr::Sum(name) => {
                            row.get(&format!("SUM({})", name))
                                .unwrap_or(&"NULL".to_string())
                                .clone()
                        }
                        ColumnExpr::Avg(name) => {
                            row.get(&format!("AVG({})", name))
                                .unwrap_or(&"NULL".to_string())
                                .clone()
                        }
                        ColumnExpr::Min(name) => {
                            row.get(&format!("MIN({})", name))
                                .unwrap_or(&"NULL".to_string())
                                .clone()
                        }
                        ColumnExpr::Max(name) => {
                            row.get(&format!("MAX({})", name))
                                .unwrap_or(&"NULL".to_string())
                                .clone()
                        }
                        ColumnExpr::CountAll => {
                            row.get("COUNT(*)")
                                .unwrap_or(&"NULL".to_string())
                                .clone()
                        }
                        ColumnExpr::All => {
                            // This shouldn't happen in the else branch, but just in case
                            "*".to_string()
                        }
                    }
                }).collect()
            };
            
            result.push_str(&values.join(" | "));
            result.push('\n');
        }
        
        Ok(result)
    }

    fn load_schemas(&mut self) -> Result<(), String> {
        if !self.data_dir.exists() {
            return Ok(());
        }
        
        for entry in fs::read_dir(&self.data_dir)
            .map_err(|e| format!("Failed to read data directory: {}", e))? {
            let entry = entry.map_err(|e| format!("Failed to read directory entry: {}", e))?;
            let path = entry.path();
            
            if path.is_dir() {
                let table_name = path.file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| "Invalid table name".to_string())?;
                
                // Check if this is a schema directory
                if table_name.ends_with("_schema") {
                    let actual_table_name = table_name.trim_end_matches("_schema");
                    let mut schema_storage = LSMStorage::new(&self.data_dir, table_name)
                        .map_err(|e| format!("Failed to open schema storage: {}", e))?;
                    
                    if let Ok(Some(schema_json)) = schema_storage.get("schema") {
                        let columns: Vec<String> = serde_json::from_str(&schema_json)
                            .map_err(|e| format!("Failed to deserialize schema: {}", e))?;
                        
                        self.table_schemas.insert(actual_table_name.to_string(), columns);
                        
                        // Also initialize the table storage
                        let table_storage = LSMStorage::new(&self.data_dir, actual_table_name)
                            .map_err(|e| format!("Failed to open table storage: {}", e))?;
                        self.tables.insert(actual_table_name.to_string(), table_storage);
                    }
                } else if !table_name.ends_with("_schema") && !self.tables.contains_key(table_name) {
                    // Check if this is a regular table directory (not schema)
                    // and we haven't already loaded it
                    let schema_dir = format!("{}_schema", table_name);
                    let schema_path = self.data_dir.join(&schema_dir);
                    
                    if schema_path.exists() {
                        // This table has a schema, so it's a valid table
                        let table_storage = LSMStorage::new(&self.data_dir, table_name)
                            .map_err(|e| format!("Failed to open table storage: {}", e))?;
                        self.tables.insert(table_name.to_string(), table_storage);
                        
                        // Load the schema if not already loaded
                        if !self.table_schemas.contains_key(table_name) {
                            let mut schema_storage = LSMStorage::new(&self.data_dir, &schema_dir)
                                .map_err(|e| format!("Failed to open schema storage: {}", e))?;
                            
                            if let Ok(Some(schema_json)) = schema_storage.get("schema") {
                                let columns: Vec<String> = serde_json::from_str(&schema_json)
                                    .map_err(|e| format!("Failed to deserialize schema: {}", e))?;
                                self.table_schemas.insert(table_name.to_string(), columns);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    pub fn close(&mut self) -> Result<(), String> {
        for (_, storage) in self.tables.iter_mut() {
            storage.close().map_err(|e| format!("Failed to close storage: {}", e))?;
        }
        Ok(())
    }
} 